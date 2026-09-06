package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestJobExecutionRepository_Create(t *testing.T) {

	ctx := context.Background()

	pool := testDBPool(t)

	now := time.Now().UTC().Truncate(time.Microsecond)

	jobRepo := NewJobRepository(pool)

	jobInput := CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{"to":"test@example.com","subject":"test email"}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
		CallbackURL:    nil,
	}

	jobID, err := jobRepo.Create(ctx, jobInput)
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		if err != nil {
			t.Fatalf("unable to cleanup test cases: %v", err)
		}

		_, err = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
		if err != nil {
			t.Fatalf("unable to cleanup job: %v", err)
		}
		pool.Close()
	})

	execRepo := NewJobExecutionRepository(pool)

	execInput := CreateJobExecutionInput{
		JobID:         jobID,
		AttemptNumber: 1,
		StartedAt:     now,
		Status:        "scheduled",
	}
	execID1, err := execRepo.Create(ctx, execInput)
	if err != nil {
		t.Fatalf("failed to create job execution: %v", err)
	}

	execInput.AttemptNumber = 2

	execID2, err := execRepo.Create(ctx, execInput)
	if err != nil {
		t.Fatalf("failed to create job execution: %v", err)
	}

	if execID1 == execID2 {
		t.Fatalf("expected different execution IDs, got same ID: %d", execID1)
	}

	storedJobID1, attemptNumber1, err := getExecution(t, ctx, pool, execID1)
	if err != nil {
		t.Fatalf("failed to retrieve execution %d: %v", execID1, err)
	}

	if storedJobID1 != jobID {
		t.Fatalf("job ID did not match for execution ID: %v", execID1)
	}

	if attemptNumber1 != 1 {
		t.Fatalf("attempt number did not match for execution ID: %v", execID1)
	}

	storedJobID2, attemptNumber2, err := getExecution(t, ctx, pool, execID2)
	if err != nil {
		t.Fatalf("failed to retrieve execution %d: %v", execID2, err)
	}

	if storedJobID2 != jobID {
		t.Fatalf("job ID did not match for execution ID: %v", execID2)
	}

	if attemptNumber2 != 2 {
		t.Fatalf("attempt number did not match for execution ID: %v", execID2)
	}

	if attemptNumber1 == attemptNumber2 {
		t.Fatalf("expected different attempt numbers, got %d", attemptNumber1)
	}

}

func getExecution(t *testing.T, ctx context.Context, pool *pgxpool.Pool, executionID int64) (uuid.UUID, int, error) {

	t.Helper()
	var storedJobID uuid.UUID
	var attemptNumber int
	query := `SELECT job_id , attempt_number 
	FROM job_executions
	WHERE id = $1`

	err := pool.QueryRow(ctx, query, executionID).Scan(&storedJobID, &attemptNumber)
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("failed to get execution: %w", err)
	}

	return storedJobID, attemptNumber, nil
}
