package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

func TestCreateJob(t *testing.T) {
	pool := testDBPool(t)

	var jobID uuid.UUID

	t.Cleanup(func() {
		if jobID != uuid.Nil {
			_, err := pool.Exec(
				context.Background(),
				"DELETE FROM job_executions WHERE job_id = $1",
				jobID,
			)
			if err != nil {
				t.Errorf("failed to cleanup job executions: %v", err)
			}

			_, err = pool.Exec(
				context.Background(),
				"DELETE FROM jobs WHERE id = $1",
				jobID,
			)
			if err != nil {
				t.Errorf("failed to cleanup job: %v", err)
			}
		}

		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)

	idempotencyKey := uuid.New().String()

	body := fmt.Sprintf(`{
		"type": "email",
		"payload": {"to": "test@example.com"},
		"run_at": "2026-09-07T12:00:00Z",
		"max_attempts": 3,
		"idempotency_key": "%s",
		"callback_url": "https://example.com/callback"

	}`, idempotencyKey)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))

	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf(
			"expected 201, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var response struct {
		ID uuid.UUID `json:"id"`
	}

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response %v", err)
	}

	if response.ID == uuid.Nil {
		t.Fatal("expected a non-zero job ID")
	}

	jobID = response.ID

	job, err := jobRepo.GetByID(context.Background(), response.ID)
	if err != nil {
		t.Fatalf("failed to get created job: %v", err)
	}

	if job.ID != response.ID {
		t.Fatalf("expected job ID %s, got %s", response.ID, job.ID)
	}
}

func TestCreateJob_InvalidJSON(t *testing.T) {
	handler := NewHandler(nil)

	body := `{"type": "email",`

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))

	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 , got %d", recorder.Code)
	}
}

func TestCreateJob_MissingType(t *testing.T) {
	handler := NewHandler(nil)
	body := `{
		"payload": {"to": "test@example.com"},
		"run_at": "2026-09-07T12:00:00Z",
		"max_attempts": 3,
		"idempotency_key": "test-key"
	}`

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))

	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}
func TestCreateJob_InvalidPayload(t *testing.T) {
	handler := NewHandler(nil)

	body := `{
		"type": "email",
		"payload": {"to": },
		"run_at": "2026-09-07T12:00:00Z",
		"max_attempts": 3,
		"idempotency_key": "test-key"
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		strings.NewReader(body),
	)

	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestCreateJob_InvalidMaxAttempts(t *testing.T) {
	handler := NewHandler(nil)

	body := `{
		"type": "email",
		"payload": {"to": "test@example.com"},
		"run_at": "2026-09-07T12:00:00Z",
		"max_attempts": 0,
		"idempotency_key": "test-key"
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		strings.NewReader(body),
	)

	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestCreateJob_MissingIdempotencyKey(t *testing.T) {
	handler := NewHandler(nil)

	body := `{
		"type": "email",
		"payload": {"to": "test@example.com"},
		"run_at": "2026-09-07T12:00:00Z",
		"max_attempts": 3
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		strings.NewReader(body),
	)

	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}
func TestCreateJob_MissingCallbackURL(t *testing.T) {
	handler := NewHandler(nil)

	body := `{
		"type": "email",
		"payload": {"message": "hello"},
		"max_attempts": 3,
		"idempotency_key": "test-key"
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		strings.NewReader(body),
	)

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}

	if !strings.Contains(recorder.Body.String(), "callback_url") {
		t.Fatalf("expected error to mention callback_url, got %s", recorder.Body.String())
	}
}

func TestCreateJob_InvalidCallbackURL(t *testing.T) {
	handler := NewHandler(nil)

	body := `{
		"type": "email",
		"payload": {"message": "hello"},
		"max_attempts": 3,
		"idempotency_key": "test-key",
		"callback_url": "/callback"
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		strings.NewReader(body),
	)

	recorder := httptest.NewRecorder()

	handler.CreateJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}
