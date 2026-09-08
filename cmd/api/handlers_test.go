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

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)

	idempotencyKey := uuid.New().String()

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

	body := fmt.Sprintf(`{
		"type": "email",
		"payload": {"to": "test@example.com"},
		"run_at": "2026-09-07T12:00:00Z",
		"max_attempts": 3,
		"idempotency_key": "%s",
		"callback_url": "https://example.com/callback"

	}`, idempotencyKey)

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))

	firstReq.Header.Set("Content-Type", "application/json")

	firstRecorder := httptest.NewRecorder()

	handler.CreateJob(firstRecorder, firstReq)

	if firstRecorder.Code != http.StatusCreated {
		t.Fatalf("first request: expected 201, got %d, body: %s", firstRecorder.Code, firstRecorder.Body.String())
	}

	var firstResponse struct {
		ID uuid.UUID `json:"id"`
	}

	if err := json.NewDecoder(firstRecorder.Body).Decode(&firstResponse); err != nil {
		t.Fatalf("failed to decode first response: %v", err)
	}

	if firstResponse.ID == uuid.Nil {
		t.Fatal("first response returned a zero job ID")
	}

	// Second submission

	secondReq := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))

	secondReq.Header.Set("Content-Type", "application/json")

	secondRecorder := httptest.NewRecorder()

	handler.CreateJob(secondRecorder, secondReq)

	if secondRecorder.Code != http.StatusOK {
		t.Fatalf(
			"expected 201, got %d, body: %s",
			secondRecorder.Code,
			secondRecorder.Body.String(),
		)
	}

	var Secondresponse struct {
		ID uuid.UUID `json:"id"`
	}

	if err := json.NewDecoder(secondRecorder.Body).Decode(&Secondresponse); err != nil {
		t.Fatalf("failed to decode response %v", err)
	}

	if Secondresponse.ID == uuid.Nil {
		t.Fatal("expected a non-zero job ID")
	}

	var count int

	err := pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1", idempotencyKey).Scan(&count)

	if err != nil {
		t.Fatalf("failed to count jobs: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected exactly 1 job, got %d", count)
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
