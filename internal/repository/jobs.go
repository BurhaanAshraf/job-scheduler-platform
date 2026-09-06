package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = fmt.Errorf("resource does not exists")

const StatusPending = "pending"
const StatusScheduled = "scheduled"
const StatusRunning = "running"
const StatusDone = "done"
const StatusDead = "dead"
const StatusCancelled = "cancelled"

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
	ID             uuid.UUID
	Type           string
	Payload        json.RawMessage
	Status         string
	RunAt          time.Time
	Attempts       int
	MaxAttempts    int
	IdempotencyKey string
	CallbackURL    *string
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (r *JobRepository) Create(ctx context.Context, input CreateJobInput) (uuid.UUID, error) {
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
    updated_at
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

	if limit <= 0 {
		return nil, errors.New("limit must be greater than zero")
	}

	if offset < 0 {
		return nil, errors.New("offset cannot be negative")
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `SELECT id , type , payload , status , run_at , attempts , max_attempts , idempotency_key , callback_url , last_error , created_at , updated_at 
	FROM jobs 
	WHERE status = $1 
	ORDER BY created_at ASC 
	LIMIT $2 
	OFFSET $3`

	rows, err := r.pool.Query(ctx, query, status, limit, offset)
	if err != nil {
		return []Job{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		var job Job

		err := rows.Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.RunAt, &job.Attempts, &job.MaxAttempts, &job.IdempotencyKey, &job.CallbackURL, &job.LastError, &job.CreatedAt, &job.UpdatedAt)
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
