package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestJobRepository_Create(t *testing.T) {
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	repo := NewJobRepository(pool)

	payload := json.RawMessage(`{"message":"hello"}`)
	idempotencyKey := "test-" + uuid.NewString()
	runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	input := CreateJobInput{
		Type:           "email",
		Payload:        payload,
		RunAt:          runAt,
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    nil,
	}

	jobID, err := repo.Create(ctx, input)

	if err != nil {
		t.Fatalf("Create() returned unexpected error: %v", err)
	}

	if jobID == uuid.Nil {
		t.Fatal("Create() returned uuid.Nil")
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1", jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	var (
		gotType           string
		gotPayload        string
		gotStatus         string
		gotRunAt          time.Time
		gotAttempts       int
		gotMaxAttempts    int
		gotIdempotencyKey string
		gotCallbackURL    *string
		gotLastError      *string
	)
	err = pool.QueryRow(ctx, `SELECT type, payload, status, run_at , attempts, max_attempts, idempotency_key, callback_url, last_error FROM jobs WHERE id = $1`, jobID).Scan(&gotType, &gotPayload, &gotStatus, &gotRunAt, &gotAttempts, &gotMaxAttempts, &gotIdempotencyKey, &gotCallbackURL, &gotLastError)

	if err != nil {
		t.Fatalf("failed to retrieve created job: %v", err)
	}

	if gotType != input.Type {
		t.Errorf("type = %q, want %q", gotType, input.Type)
	}

	assertJSONEqual(t, []byte(gotPayload), input.Payload)

	if !gotRunAt.Equal(input.RunAt) {
		t.Errorf("run_at = %v, want %v", gotRunAt, input.RunAt)
	}
	if gotStatus != StatusPending {
		t.Errorf("status = %q, want %q", gotStatus, StatusPending)
	}
	if gotAttempts != 0 {
		t.Errorf("attempts = %d, want 0", gotAttempts)
	}
	if gotMaxAttempts != input.MaxAttempts {
		t.Errorf("max_attempts = %d, want %d", gotMaxAttempts, input.MaxAttempts)
	}
	if gotIdempotencyKey != input.IdempotencyKey {
		t.Errorf("idempotency_key = %q, want %q", gotIdempotencyKey, input.IdempotencyKey)
	}
	if gotCallbackURL != nil {
		t.Errorf("callback_url = %v, want nil", gotCallbackURL)
	}
	if gotLastError != nil {
		t.Errorf("last_error = %v, want nil", gotLastError)
	}

}

func TestJobRepository_Create_DuplicateIdempotencyKey(t *testing.T) {

	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required...")
	}
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	repo := NewJobRepository(pool)

	idempotencyKey := "duplicate-" + uuid.NewString()

	payload := json.RawMessage(`{"message":"duplicate test"}`)

	input := CreateJobInput{
		Type:           "email",
		Payload:        payload,
		RunAt:          time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    nil,
	}

	firstJobID, err := repo.Create(ctx, input)
	if err != nil {
		t.Fatalf("first Create() returned unexpected error: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), `DELETE FROM jobs WHERE id = $1`, firstJobID)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	_, err = repo.Create(ctx, input)
	if err == nil {
		t.Fatal("second Create() expected an error, got nil")
	}

	var conflictErr *ConflictError

	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected ConflictError, got %T: %v", err, err)
	}

}
func TestJobRepository_Create_DatabaseError(t *testing.T) {
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}

	repo := NewJobRepository(pool)

	pool.Close()

	input := CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{"message":"database error test"}`),
		RunAt:          time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
		MaxAttempts:    3,
		IdempotencyKey: "database-error-" + uuid.NewString(),
		CallbackURL:    nil,
	}
	jobID, err := repo.Create(ctx, input)
	if err == nil {
		t.Fatal("Create() unexpected error,, got nil")
	}

	if jobID != uuid.Nil {
		t.Errorf("jobID = %v, want uuid.Nil", jobID)
	}

}

func TestJobRepository_GetByID(t *testing.T) {
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database connection pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("failed to ping database: %v", err)
	}

	repo := NewJobRepository(pool)

	payload := json.RawMessage(`{"message":"get by id test"}`)
	idempotencyKey := "get-by-id-" + uuid.NewString()
	runAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	input := CreateJobInput{
		Type:           "email",
		Payload:        payload,
		RunAt:          runAt,
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    nil,
	}

	jobID, err := repo.Create(ctx, input)
	if err != nil {
		pool.Close()
		t.Fatalf("Create() returned unexpected error: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
		pool.Close()
	})

	gotJob, err := repo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("GetByID() returned unexpected error: %v", err)
	}

	// Assertions

	if gotJob.ID != jobID {
		t.Errorf("ID = %v, want %v", gotJob.ID, jobID)
	}
	if gotJob.Type != input.Type {
		t.Errorf("Type = %q, want %q", gotJob.Type, input.Type)
	}

	if !gotJob.RunAt.Equal(input.RunAt) {
		t.Errorf("RunAt = %v, want %v", gotJob.RunAt, input.RunAt)
	}

	if gotJob.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", gotJob.Attempts)
	}

	if gotJob.MaxAttempts != input.MaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", gotJob.MaxAttempts, input.MaxAttempts)
	}

	if gotJob.IdempotencyKey != input.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q, want %q", gotJob.IdempotencyKey, input.IdempotencyKey)
	}

	if gotJob.Status != StatusPending {
		t.Errorf("Status = %q, want %q", gotJob.Status, StatusPending)
	}

	if gotJob.CallbackURL != nil {
		t.Errorf("CallbackURL = %v, want nil", gotJob.CallbackURL)
	}

	if gotJob.LastError != nil {
		t.Errorf("LastError = %v, want nil", gotJob.LastError)
	}

	assertJSONEqual(t, gotJob.Payload, input.Payload)
}

func TestJobRepository_GetByID_NotFound(t *testing.T) {

	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("failed to ping database: %v", err)
	}

	repo := NewJobRepository(pool)

	id := uuid.New()

	job, err := repo.GetByID(ctx, id)
	if err == nil {
		t.Fatal("GetByID() expected an error, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %T: %v", err, err)
	}

	if job.ID != uuid.Nil {
		t.Errorf("job.ID = %v, want uuid.Nil", job.ID)
	}
	pool.Close()

}

func TestJobRepository_GetByID_Database_Error(t *testing.T) {
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	repo := NewJobRepository(pool)

	pool.Close()

	job, err := repo.GetByID(ctx, uuid.New())
	if err == nil {
		t.Fatalf("GetByID() expected an error, got nil")
	}

	if job.ID != uuid.Nil {
		t.Errorf("job.ID = %v, want uuid.Nil", job.ID)
	}
}

func TestJobRepository_UpdateStatus(t *testing.T) {

	pool := testDBPool(t)

	repo := NewJobRepository(pool)

	input := CreateJobInput{
		Type:           "test-job",
		Payload:        json.RawMessage(`{"key":"value"}`),
		RunAt:          time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
	}

	jobID, err := repo.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("failed to create test job: %v", err)
	}

	t.Cleanup(func() {

		_, err := pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	lastError := "test execution failed"

	err = repo.UpdateStatus(context.Background(), jobID, "dead", &lastError)
	if err != nil {
		t.Fatalf("failed to update job status: %v", err)
	}

	job, err := repo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to fetch updated job: %v", err)
	}
	if job.Status != "dead" {
		t.Errorf("status = %q , want %q", job.Status, "dead")
	}

	if job.LastError == nil {
		t.Fatal("last error = nil, wanted value")
	}

	if *job.LastError != lastError {
		t.Errorf("last error = %q, want %q", *job.LastError, lastError)
	}

}

func TestJobRepository_UpdateStatus_NotFound(t *testing.T) {
	pool := testDBPool(t)
	repo := NewJobRepository(pool)

	jobID := uuid.New()
	lastError := "test error"

	err := repo.UpdateStatus(
		context.Background(), jobID, "dead", &lastError,
	)

	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}

}

func TestJobRepository_UpdateStatus_DatabaseError(t *testing.T) {
	pool := testDBPool(t)
	repo := NewJobRepository(pool)

	pool.Close()

	jobID := uuid.New()
	lastError := "test error"
	err := repo.UpdateStatus(context.Background(), jobID, "dead", &lastError)
	if err == nil {
		t.Fatal("expected database error, got nil")
	}
}

func TestJobRepository_ListByStatus(t *testing.T) {
	pool := testDBPool(t)
	repo := NewJobRepository(pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `DELETE FROM job_executions`)
	if err != nil {
		t.Fatalf("failed to clean job executions table: %v", err)
	}

	_, err = pool.Exec(ctx, `DELETE FROM jobs`)
	if err != nil {
		t.Fatalf("failed to clean jobs table: %v", err)
	}

	jobIDs := make([]uuid.UUID, 0, 15)

	t.Cleanup(func() {
		for _, jobID := range jobIDs {
			_, err := pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
			if err != nil {
				t.Fatalf("failed to clean up job %s: %v", jobID, err)
			}
		}
		pool.Close()
	})

	for i := 0; i < 15; i++ {
		input := CreateJobInput{
			Type:           "test-job",
			Payload:        json.RawMessage(`{"key":"value"}`),
			RunAt:          time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
			MaxAttempts:    3,
			CallbackURL:    nil,
			IdempotencyKey: uuid.NewString(),
		}

		jobID, err := repo.Create(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to create test job %d: %v", i, err)
		}
		jobIDs = append(jobIDs, jobID)
	}

	allJobs, err := repo.ListByStatus(
		context.Background(),
		StatusPending,
		100,
		0,
	)
	if err != nil {
		t.Fatalf("failed to list jobs: %v", err)
	}

	positions := make(map[uuid.UUID]int)

	for i, job := range allJobs {
		positions[job.ID] = i
	}

	for _, jobID := range jobIDs {
		if _, ok := positions[jobID]; !ok {
			t.Fatalf("test job %s was not returned", jobID)
		}
	}

	firstPosition := positions[jobIDs[0]]

	firstPage, err := repo.ListByStatus(
		context.Background(),
		StatusPending,
		10,
		firstPosition,
	)
	if err != nil {
		t.Fatalf("failed to list first page: %v", err)
	}

	if len(firstPage) != 10 {
		t.Errorf("first page contains %d jobs, want 10", len(firstPage))
	}

	assertJobsHaveStatus(t, firstPage, StatusPending)

	for i, job := range firstPage {
		if job.ID != allJobs[firstPosition+i].ID {
			t.Errorf(
				"first page job %d is %s, want %s",
				i,
				job.ID,
				allJobs[firstPosition+i].ID,
			)
		}
	}

	secondPage, err := repo.ListByStatus(
		context.Background(),
		StatusPending,
		10,
		firstPosition+10,
	)
	if err != nil {
		t.Fatalf("failed to list second page: %v", err)
	}

	if len(secondPage) != 5 {
		t.Fatalf("second page contains %d jobs, want 5", len(secondPage))
	}

	assertJobsHaveStatus(t, secondPage, StatusPending)

	for i, job := range secondPage {
		if job.ID != allJobs[firstPosition+10+i].ID {
			t.Errorf(
				"second page job %d is %s, want %s",
				i,
				job.ID,
				allJobs[firstPosition+10+i].ID,
			)
		}
	}

}

func testDBPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create database connection pool : %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("failed to ping the database: %v", err)
	}
	return pool
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()

	var gotValue any
	var wantValue any

	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("failed to decode got JSON: %v", err)
	}

	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("failed to decode want JSON: %v", err)
	}

	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Errorf("JSON mismatch: got %s, want %s", got, want)
	}
}

func assertJobsHaveStatus(t *testing.T, jobs []Job, status string) {
	t.Helper()

	for _, job := range jobs {
		if job.Status != status {
			t.Errorf("job %s has status %q, want %q", job.ID, job.Status, status)
		}
	}
}

func TestJobRepository_IncrementAttempts(t *testing.T) {
	ctx := context.Background()
	pool := testDBPool(t)
	repo := NewJobRepository(pool)

	jobID, err := repo.Create(ctx, CreateJobInput{
		Type:        "test",
		Payload:     json.RawMessage(`{"message":"attempt-test"}`),
		RunAt:       time.Now().UTC(),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	attempt, err := repo.IncrementAttempts(ctx, jobID)
	if err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	if attempt != 1 {
		t.Fatalf("expected attempts=1, got %d", attempt)
	}

	attempt, err = repo.IncrementAttempts(ctx, jobID)
	if err != nil {
		t.Fatalf("increment attempts second time: %v", err)
	}

	if attempt != 2 {
		t.Fatalf("expected attempts=2, got %d", attempt)
	}
}

func TestJobRepository_StartExecution(t *testing.T) {
	ctx := context.Background()
	pool := testDBPool(t)
	repo := NewJobRepository(pool)

	jobID, err := repo.Create(ctx, CreateJobInput{
		Type:           "test",
		Payload:        json.RawMessage(`{"message":"start-execution-test"}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: "start-execution-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	attempt, err := repo.StartExecution(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("start execution: %v", err)
	}

	if attempt != 1 {
		t.Fatalf("expected first attempt=1, got %d", attempt)
	}

	job, err := repo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get job after start execution: %v", err)
	}

	if job.Status != StatusRunning {
		t.Fatalf(
			"expected status=%q, got %q",
			StatusRunning,
			job.Status,
		)
	}

	if job.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", job.Attempts)
	}

	// A second worker must not be able to claim the same running job.
	attempt, err = repo.StartExecution(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("start execution second time: %v", err)
	}

	if attempt != 0 {
		t.Fatalf(
			"expected second claim to return 0, got %d",
			attempt,
		)
	}
}
