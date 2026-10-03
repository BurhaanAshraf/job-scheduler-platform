package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = fmt.Errorf("resource does not exist")

const StatusPending = "pending"
const StatusScheduled = "scheduled"
const StatusRunning = "running"
const StatusDone = "done"
const StatusDead = "dead"
const StatusCancelled = "cancelled" //nolint:misspell // 'cancelled' is the contractual spelling (DB CHECK constraint)

var ErrNotCancellable = errors.New("job cannot be cancelled in its current state") //nolint:misspell // 'cancelled' is the contractual spelling (DB CHECK constraint)

var validStatuses = map[string]struct{}{
	StatusPending:   {},
	StatusScheduled: {},
	StatusRunning:   {},
	StatusDone:      {},
	StatusDead:      {},
	StatusCancelled: {},
}

type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string {
	return e.Message
}

func (e *ConflictError) Is(target error) bool {
	_, ok := target.(*ConflictError)
	return ok
}

var ErrConflict = &ConflictError{
	Message: "idempotency key already exists",
}

type JobRepository struct {
	pool *pgxpool.Pool
}
type CreateJobInput struct {
	Type           string
	Payload        json.RawMessage
	RunAt          time.Time
	MaxAttempts    int
	IdempotencyKey string
	CallbackURL    *string
}
type Job struct {
	ID              uuid.UUID       `json:"id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Status          string          `json:"status"`
	RunAt           time.Time       `json:"run_at"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	IdempotencyKey  string          `json:"idempotency_key"`
	CallbackURL     *string         `json:"callback_url,omitempty"`
	LastError       *string         `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	QueueGeneration int64           `json:"queue_generation"`
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (r *JobRepository) Create(ctx context.Context, input CreateJobInput) (uuid.UUID, error) {
	if err := validateCreateJobInput(input); err != nil {
		return uuid.Nil, err
	}
	var jobID uuid.UUID
	// generate UUID
	id := uuid.New()
	// execute INSERT ... RETURNING
	now := time.Now().UTC()

	status := StatusPending

	createdAt := now
	updatedAt := now
	query := `INSERT INTO jobs(id, type, payload, status, run_at, max_attempts, idempotency_key, callback_url, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	RETURNING id`
	// scan returned columns
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// attempts is not an INSERT argument because PostgreSQL supplies its DEFAULT 0.
	err := r.pool.QueryRow(ctx, query, id, input.Type, input.Payload, status, input.RunAt, input.MaxAttempts, input.IdempotencyKey, input.CallbackURL, createdAt, updatedAt).Scan(&jobID)

	// translate duplicate idempotency key
	if err != nil {

		var pgErr *pgconn.PgError

		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "jobs_idempotency_key_key" {
			return uuid.Nil, ErrConflict
		}
		return uuid.Nil, fmt.Errorf("failed to create job: %w", err)
	}
	return jobID, nil

}

// validateCreateJobInput enforces the repository-boundary contract for 2.6:
// non-empty type and, when a callback URL is provided, a well-formed
// absolute http(s) URL. Nil callback is allowed at this layer (cron
// templates and legacy tests); the API layer additionally requires it.
func validateCreateJobInput(input CreateJobInput) error {
	if strings.TrimSpace(input.Type) == "" {
		return fmt.Errorf("type is required")
	}
	if input.CallbackURL == nil {
		return nil
	}
	if strings.TrimSpace(*input.CallbackURL) == "" {
		return fmt.Errorf("callback_url is required")
	}
	u, err := url.Parse(strings.TrimSpace(*input.CallbackURL))
	if err != nil || !u.IsAbs() || u.Host == "" {
		return fmt.Errorf("callback_url must be a valid absolute URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("callback_url must use http or https")
	}
	return nil
}

func (r *JobRepository) GetByID(ctx context.Context, id uuid.UUID) (Job, error) {

	var job Job

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `
	SELECT
    id,
    type,
    payload,
    status,
    run_at,
    attempts,
    max_attempts,
    idempotency_key,
    callback_url,
    last_error,
    created_at,
    updated_at,
    queue_generation
	FROM jobs
	WHERE id = $1`

	err := r.pool.QueryRow(ctx, query, id).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.RunAt,
		&job.Attempts,
		&job.MaxAttempts,
		&job.IdempotencyKey,
		&job.CallbackURL,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.QueueGeneration,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, fmt.Errorf("failed to get job: %w", err)
	}

	return job, nil
}

func (r *JobRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status string, lastError *string) error {

	if _, ok := validStatuses[status]; !ok {
		return fmt.Errorf("invalid job status: %q", status)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `UPDATE jobs SET status = $2 , last_error = $3 , updated_at = $4 WHERE id = $1`
	now := time.Now().UTC()
	result, err := r.pool.Exec(ctx, query, id, status, lastError, now)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *JobRepository) ListByStatus(ctx context.Context, status string, limit, offset int) ([]Job, error) {

	if _, ok := validStatuses[status]; !ok {
		return nil, fmt.Errorf("invalid job status: %q", status)
	}

	return r.list(ctx, &status, limit, offset)
}

// List returns jobs across all statuses, newest ordering preserved for
// pagination stability. Used by GET /v1/jobs without a status filter.
func (r *JobRepository) List(ctx context.Context, limit, offset int) ([]Job, error) {
	return r.list(ctx, nil, limit, offset)
}

func (r *JobRepository) list(ctx context.Context, status *string, limit, offset int) ([]Job, error) {

	if limit <= 0 {
		return nil, errors.New("limit must be greater than zero")
	}

	if offset < 0 {
		return nil, errors.New("offset cannot be negative")
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var query string
	var args []any
	if status != nil {
		query = `SELECT id , type , payload , status , run_at , attempts , max_attempts , idempotency_key , callback_url , last_error , created_at , updated_at , queue_generation
	FROM jobs
	WHERE status = $1
	ORDER BY created_at ASC, id ASC
	LIMIT $2
	OFFSET $3`
		args = []any{*status, limit, offset}
	} else {
		query = `SELECT id , type , payload , status , run_at , attempts , max_attempts , idempotency_key , callback_url , last_error , created_at , updated_at , queue_generation
	FROM jobs
	ORDER BY created_at ASC, id ASC
	LIMIT $1
	OFFSET $2`
		args = []any{limit, offset}
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return []Job{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		var job Job

		err := rows.Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.RunAt, &job.Attempts, &job.MaxAttempts, &job.IdempotencyKey, &job.CallbackURL, &job.LastError, &job.CreatedAt, &job.UpdatedAt, &job.QueueGeneration)
		if err != nil {
			return []Job{}, fmt.Errorf("failed to scan job: %w", err)
		}

		jobs = append(jobs, job)

	}
	// Always checks rows.Err() after the loop finishes
	if err := rows.Err(); err != nil {
		return []Job{}, fmt.Errorf("failed while reading jobs: %w", err)
	}

	return jobs, nil
}

func (r *JobRepository) GetByIdempotencyKey(ctx context.Context, key string) (Job, error) {
	var job Job

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `
	SELECT id , type , payload , status , run_at , attempts , max_attempts, idempotency_key , callback_url, last_error , created_at , updated_at , queue_generation FROM jobs WHERE idempotency_key = $1`

	err := r.pool.QueryRow(ctx, query, key).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.RunAt,
		&job.Attempts,
		&job.MaxAttempts,
		&job.IdempotencyKey,
		&job.CallbackURL,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.QueueGeneration,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrNotFound
		}

		return Job{}, fmt.Errorf("failed to get job by idempotency key: %w", err)
	}

	return job, nil
}

func (r *JobRepository) Cancel(ctx context.Context, id uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// begin is used for transactions
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin cancellation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string

	err = tx.QueryRow(
		ctx,
		"SELECT status FROM jobs WHERE id = $1 FOR UPDATE",
		id,
	).Scan(&status)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}

		return fmt.Errorf("failed to get job status: %w", err)
	}

	if status != StatusPending && status != StatusScheduled {
		return ErrNotCancellable
	}

	_, err = tx.Exec(
		ctx,
		`UPDATE jobs
			 SET status = $2, updated_at = $3
			 WHERE id = $1`,
		id,
		StatusCancelled,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit job cancellation: %w", err)
	}

	return nil
}

func (r *JobRepository) IncrementAttempts(ctx context.Context, id uuid.UUID) (int, error) {
	var attempts int

	err := r.pool.QueryRow(
		ctx,
		`UPDATE jobs
		 SET attempts = attempts + 1,
		     updated_at = NOW()
		 WHERE id = $1
		 RETURNING attempts`,
		id,
	).Scan(&attempts)
	if err != nil {
		return 0, fmt.Errorf("increment attempts for job %s: %w", id, err)
	}

	return attempts, nil
}

func (r *JobRepository) StartExecution(ctx context.Context, id uuid.UUID, queueGeneration int64) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var attempts int

	err := r.pool.QueryRow(
		ctx,
		`UPDATE jobs
		 SET status = $2,
		     attempts = attempts + 1,
		     updated_at = NOW()
		 WHERE id = $1
		   AND queue_generation = $3
		   AND status IN ($4, $5)
		 RETURNING attempts`,
		id,
		StatusRunning,
		queueGeneration,
		StatusPending,
		StatusScheduled,
	).Scan(&attempts)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}

		return 0, fmt.Errorf("start job execution: %w", err)
	}

	return attempts, nil
}

func (r *JobRepository) Retry(ctx context.Context, id uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	result, err := r.pool.Exec(
		ctx,
		`UPDATE jobs
		 SET attempts = 0,
		     status = $2,
		     run_at = $3,
		     last_error = NULL,
		     queue_generation = queue_generation + 1,
		     updated_at = $3
		 WHERE id = $1
		   AND status = $4`,
		id,
		StatusScheduled,
		time.Now().UTC(),
		StatusDead,
	)
	if err != nil {
		return fmt.Errorf("failed to retry job: %w", err)
	}

	if result.RowsAffected() == 0 {
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer checkCancel()

		var status string

		err := r.pool.QueryRow(
			checkCtx,
			`SELECT status FROM jobs WHERE id = $1`,
			id,
		).Scan(&status)

		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}

		if err != nil {
			return fmt.Errorf("failed to check job status: %w", err)
		}

		return fmt.Errorf("%w: job status is %q, only dead jobs can be retried", ErrNotCancellable, status)
	}

	return nil
}

func (r *JobRepository) ScheduleRetry(
	ctx context.Context,
	id uuid.UUID,
	nextRunAt time.Time,
	lastError *string,
) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var queueGeneration int64

	err := r.pool.QueryRow(
		ctx,
		`UPDATE jobs
		 SET status = $2,
		     queue_generation = queue_generation + 1,
		     run_at = $3,
		     last_error = $4,
		     updated_at = NOW()
		 WHERE id = $1
		   AND status = $5
		 RETURNING queue_generation`,
		id,
		StatusScheduled,
		nextRunAt,
		lastError,
		StatusRunning,
	).Scan(&queueGeneration)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotFound
		}

		return 0, fmt.Errorf("schedule retry: %w", err)
	}

	return queueGeneration, nil
}
