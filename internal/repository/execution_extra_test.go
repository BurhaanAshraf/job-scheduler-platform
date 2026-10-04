package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJobExecutionRepository_Complete(t *testing.T) {
	ctx := context.Background()
	pool := testDBPool(t)

	jobRepo := NewJobRepository(pool)
	jobID, err := jobRepo.Create(ctx, CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})

	execRepo := NewJobExecutionRepository(pool)
	execID, err := execRepo.Create(ctx, CreateJobExecutionInput{
		JobID:           jobID,
		AttemptNumber:   1,
		StartedAt:       time.Now().UTC(),
		Status:          "running",
		QueueGeneration: 1,
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}

	// Successful completion.
	code := 200
	if err := execRepo.Complete(ctx, execID, time.Now().UTC(), "done", nil, &code); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	var status string
	var respCode *int
	if err := pool.QueryRow(ctx, `SELECT status, response_code FROM job_executions WHERE id = $1`, execID).Scan(&status, &respCode); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "done" || respCode == nil || *respCode != 200 {
		t.Fatalf("unexpected row: %q %v", status, respCode)
	}

	// Failed completion records the error.
	execID2, err := execRepo.Create(ctx, CreateJobExecutionInput{
		JobID:           jobID,
		AttemptNumber:   2,
		StartedAt:       time.Now().UTC(),
		Status:          "running",
		QueueGeneration: 1,
	})
	if err != nil {
		t.Fatalf("create execution 2: %v", err)
	}
	errMsg := "connection refused"
	if err := execRepo.Complete(ctx, execID2, time.Now().UTC(), "failed", &errMsg, nil); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
}
