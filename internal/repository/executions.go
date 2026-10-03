package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobExecutionRepository struct {
	pool *pgxpool.Pool
}

func NewJobExecutionRepository(pool *pgxpool.Pool) *JobExecutionRepository {
	return &JobExecutionRepository{pool: pool}
}

type CreateJobExecutionInput struct {
	JobID           uuid.UUID
	AttemptNumber   int
	StartedAt       time.Time
	Status          string
	QueueGeneration int64
}

func (r *JobExecutionRepository) Create(ctx context.Context, input CreateJobExecutionInput) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	generation := input.QueueGeneration
	if generation <= 0 {
		generation = 1
	}

	query := `INSERT INTO job_executions (job_id , attempt_number , started_at , status , queue_generation) VALUES($1 , $2 , $3 , $4 , $5) RETURNING id`

	var executionID int64

	err := r.pool.QueryRow(ctx, query, input.JobID, input.AttemptNumber, input.StartedAt, input.Status, generation).Scan(&executionID)
	if err != nil {
		return 0, fmt.Errorf("failed to create job execution: %w", err)
	}

	return executionID, nil
}

// HasCompletedExecution reports whether the job has a finished ("done")
// execution row for the given queue generation. The worker's stale-delivery
// path uses this as completion evidence: only a redelivery WITH a done row
// may finalize the job as done (covers XACK-lost-after-success). A redelivery
// with no done row means the callback never completed and the job must be
// re-driven, never phantom-marked done.
func (r *JobExecutionRepository) HasCompletedExecution(ctx context.Context, jobID uuid.UUID, queueGeneration int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var exists bool
	err := r.pool.QueryRow(
		ctx,
		`SELECT EXISTS(
			SELECT 1 FROM job_executions
			WHERE job_id = $1 AND queue_generation = $2 AND status = 'done'
		)`,
		jobID,
		queueGeneration,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check completed executions: %w", err)
	}

	return exists, nil
}

func (r *JobExecutionRepository) Complete(
	ctx context.Context,
	id int64,
	finishedAt time.Time,
	status string,
	executionErr *string,
	responseCode *int,
) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `
		UPDATE job_executions
		SET finished_at = $2,
		    status = $3,
		    error = $4,
		    response_code = $5
		WHERE id = $1
	`

	result, err := r.pool.Exec(
		ctx,
		query,
		id,
		finishedAt,
		status,
		executionErr,
		responseCode,
	)
	if err != nil {
		return fmt.Errorf("failed to complete job execution: %w", err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: job execution %d", ErrNotFound, id)
	}

	return nil
}
