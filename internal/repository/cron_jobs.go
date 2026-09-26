package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CronJobRepository struct {
	pool *pgxpool.Pool
}

type CreateCronJobInput struct {
	CronExpression string
	JobTemplate    json.RawMessage
	NextRunAt      time.Time
	Enabled        bool
}
type CronInstance struct {
	ID              uuid.UUID
	Type            string
	Payload         json.RawMessage
	RunAt           time.Time
	MaxAttempts     int
	IdempotencyKey  string
	CallbackURL     *string
	QueueGeneration int64
}

type CronJob struct {
	ID             int64
	CronExpression string
	JobTemplate    json.RawMessage
	NextRunAt      time.Time
	Enabled        bool
	CreatedAt      time.Time
}

func NewCronJobRepository(pool *pgxpool.Pool) *CronJobRepository {
	return &CronJobRepository{pool: pool}
}

func (r *CronJobRepository) Create(ctx context.Context, input CreateCronJobInput) (CronJob, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var cronJob CronJob

	query := `
		INSERT INTO cron_jobs (
			cron_expression,
			job_template,
			next_run_at,
			enabled,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING
			id,
			cron_expression,
			job_template,
			next_run_at,
			enabled,
			created_at
	`

	createdAt := time.Now().UTC()

	err := r.pool.QueryRow(
		ctx,
		query,
		input.CronExpression,
		input.JobTemplate,
		input.NextRunAt,
		input.Enabled,
		createdAt,
	).Scan(
		&cronJob.ID,
		&cronJob.CronExpression,
		&cronJob.JobTemplate,
		&cronJob.NextRunAt,
		&cronJob.Enabled,
		&cronJob.CreatedAt,
	)
	if err != nil {
		return CronJob{}, fmt.Errorf("failed to create cron job: %w", err)
	}

	return cronJob, nil
}

func (r *CronJobRepository) UpdateEnabled(
	ctx context.Context,
	id int64,
	enabled bool,
) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `
		UPDATE cron_jobs
		SET enabled = $2
		WHERE id = $1
	`

	result, err := r.pool.Exec(ctx, query, id, enabled)
	if err != nil {
		return fmt.Errorf("failed to update cron job: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

func (r *CronJobRepository) ListDue(
	ctx context.Context,
	now time.Time,
) ([]CronJob, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `
		SELECT
			id,
			cron_expression,
			job_template,
			next_run_at,
			enabled,
			created_at
		FROM cron_jobs
		WHERE enabled = true
		  AND next_run_at <= $1
		ORDER BY next_run_at ASC, id ASC
	`

	rows, err := r.pool.Query(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("failed to list due cron jobs: %w", err)
	}
	defer rows.Close()

	cronJobs := make([]CronJob, 0)

	for rows.Next() {
		var cronJob CronJob

		if err := rows.Scan(
			&cronJob.ID,
			&cronJob.CronExpression,
			&cronJob.JobTemplate,
			&cronJob.NextRunAt,
			&cronJob.Enabled,
			&cronJob.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan due cron job: %w", err)
		}

		cronJobs = append(cronJobs, cronJob)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while reading due cron jobs: %w", err)
	}

	return cronJobs, nil
}

func (r *CronJobRepository) CreateDueInstance(
	ctx context.Context,
	cronJobID int64,
	now time.Time,
	nextRun func(string, time.Time) (time.Time, error),
) (CronInstance, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return CronInstance{}, false, fmt.Errorf("failed to begin cron tick transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var cronJob CronJob

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			cron_expression,
			job_template,
			next_run_at,
			enabled,
			created_at
		FROM cron_jobs
		WHERE id = $1
		  AND enabled = true
		  AND next_run_at <= $2
		FOR UPDATE
		`,
		cronJobID,
		now,
	).Scan(
		&cronJob.ID,
		&cronJob.CronExpression,
		&cronJob.JobTemplate,
		&cronJob.NextRunAt,
		&cronJob.Enabled,
		&cronJob.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CronInstance{}, false, nil
		}

		return CronInstance{}, false, fmt.Errorf(
			"failed to lock due cron job: %w",
			err,
		)
	}

	var template struct {
		Type        string          `json:"type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts int             `json:"max_attempts"`
		CallbackURL *string         `json:"callback_url"`
	}

	if err := json.Unmarshal(cronJob.JobTemplate, &template); err != nil {
		return CronInstance{}, false, fmt.Errorf(
			"invalid cron job template: %w",
			err,
		)
	}

	if template.Type == "" {
		return CronInstance{}, false, fmt.Errorf(
			"cron job template type is required",
		)
	}

	if len(template.Payload) == 0 || string(template.Payload) == "null" {
		return CronInstance{}, false, fmt.Errorf(
			"cron job template payload is required",
		)
	}

	if template.MaxAttempts <= 0 {
		return CronInstance{}, false, fmt.Errorf(
			"cron job template max_attempts must be greater than zero",
		)
	}

	occurrence := cronJob.NextRunAt.UTC()

	idempotencyKey := fmt.Sprintf(
		"cron:%d:%s",
		cronJob.ID,
		occurrence.Format(time.RFC3339Nano),
	)

	jobID := uuid.New()
	createdAt := now.UTC()

	var instance CronInstance

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO jobs (
			id,
			type,
			payload,
			status,
			run_at,
			max_attempts,
			idempotency_key,
			callback_url,
			created_at,
			updated_at
		)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$9
		)
		RETURNING
			id,
			type,
			payload,
			run_at,
			max_attempts,
			idempotency_key,
			callback_url,
			queue_generation
		`,
		jobID,
		template.Type,
		template.Payload,
		StatusScheduled,
		occurrence,
		template.MaxAttempts,
		idempotencyKey,
		template.CallbackURL,
		createdAt,
	).Scan(
		&instance.ID,
		&instance.Type,
		&instance.Payload,
		&instance.RunAt,
		&instance.MaxAttempts,
		&instance.IdempotencyKey,
		&instance.CallbackURL,
		&instance.QueueGeneration,
	)
	if err != nil {
		return CronInstance{}, false, fmt.Errorf(
			"failed to create cron job instance: %w",
			err,
		)
	}

	nextRunAt, err := nextRun(
		cronJob.CronExpression,
		occurrence,
	)
	if err != nil {
		return CronInstance{}, false, fmt.Errorf(
			"failed to calculate next cron run: %w",
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE cron_jobs
		SET next_run_at = $2
		WHERE id = $1
		`,
		cronJob.ID,
		nextRunAt,
	)
	if err != nil {
		return CronInstance{}, false, fmt.Errorf(
			"failed to advance cron job: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return CronInstance{}, false, fmt.Errorf(
			"failed to commit cron tick transaction: %w",
			err,
		)
	}

	return instance, true, nil
}
