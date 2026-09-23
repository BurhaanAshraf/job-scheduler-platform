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
	JobID         uuid.UUID
	AttemptNumber int
	StartedAt     time.Time
	Status        string
}

func (r *JobExecutionRepository) Create(ctx context.Context, input CreateJobExecutionInput) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := `INSERT INTO job_executions (job_id , attempt_number , started_at , status) VALUES($1 , $2 , $3 , $4) RETURNING id`

	var executionID int64

	err := r.pool.QueryRow(ctx, query, input.JobID, input.AttemptNumber, input.StartedAt, input.Status).Scan(&executionID)
	if err != nil {
		return 0, fmt.Errorf("failed to create job execution: %w", err)
	}

	return executionID, nil
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
		return fmt.Errorf("job execution %d not found", id)
	}

	return nil
}
