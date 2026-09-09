package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

func TestGetJob(t *testing.T) {
	pool := testDBPool(t)

	var jobID uuid.UUID

	t.Cleanup(func() {
		if jobID != uuid.Nil {
			_, err := pool.Exec(
				context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
			if err != nil {
				t.Errorf("failed to cleanup job executions: %v", err)
			}

			_, err = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)

			if err != nil {
				t.Errorf("failed to cleanup job: %v", err)
			}
		}
		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	Handler := NewHandler(jobRepo)
	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, Handler)

	idempotencyKey := "get-job-" + uuid.NewString()
	callbackURL := "https://example.com/callback"

	input := repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{"to":"test@example.com"}`),
		RunAt:          time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    &callbackURL,
	}

	createdID, err := jobRepo.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("failed to create test job: %v", err)
	}
	jobID = createdID

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+createdID.String(), nil)

	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body: %s", recorder.Code, recorder.Body.String())
	}

	var response repository.Job

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode job response: %v", err)
	}

	if response.ID != createdID {
		t.Fatalf("expected job ID %s, got %s", createdID, response.ID)
	}

	if response.Type != input.Type {
		t.Fatalf("expected type %q, got %q", input.Type, response.Type)
	}

	if string(response.Payload) != string(input.Payload) {
		t.Fatalf(
			"expected payload %s, got %s",
			input.Payload,
			response.Payload,
		)
	}

	if response.Status != repository.StatusPending {
		t.Fatalf(
			"expected status %q, got %q",
			repository.StatusPending,
			response.Status,
		)
	}

	if response.MaxAttempts != input.MaxAttempts {
		t.Fatalf(
			"expected max_attempts %d, got %d",
			input.MaxAttempts,
			response.MaxAttempts,
		)
	}

	if response.IdempotencyKey != input.IdempotencyKey {
		t.Fatalf(
			"expected idempotency_key %q, got %q",
			input.IdempotencyKey,
			response.IdempotencyKey,
		)
	}

	if response.CallbackURL == nil || *response.CallbackURL != callbackURL {
		t.Fatalf(
			"expected callback_url %q, got %v",
			callbackURL,
			response.CallbackURL,
		)
	}

}

func TestGetJob_NotFound(t *testing.T) {
	pool := testDBPool(t)

	t.Cleanup(func() {
		pool.Close()
	})
	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)
	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, handler)

	id := uuid.New()

	req := httptest.NewRequest(
		http.MethodGet, "/v1/jobs/"+id.String(), nil)

	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d, body: %s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if response.Error.Code == "" {
		t.Fatal("expected error code")
	}

	if response.Error.Message == "" {
		t.Fatal("expected error message")
	}
}

func TESTAPI_ErrorResponseShape(t *testing.T) {
	pool := testDBPool(t)

	t.Cleanup(func() {
		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, handler)

	postReq := httptest.NewRequest(
		http.MethodPost, "/v1/jobs", strings.NewReader(`{}`),
	)
	postReq.Header.Set("Content-Type", "application/json")
	postRecorder := httptest.NewRecorder()

	router.ServeHTTP(postRecorder, postReq)

	if postRecorder.Code != http.StatusBadRequest {
		t.Fatalf("POST expected 400, got %d, body: %s", postRecorder.Code, postRecorder.Body.String())
	}

	var postResponse map[string]any

	if err := json.NewDecoder(postRecorder.Body).Decode(&postResponse); err != nil {
		t.Fatalf("failed to decode POST error response: %v", err)
	}

	getReq := httptest.NewRequest(
		http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil,
	)

	getRecorder := httptest.NewRecorder()

	router.ServeHTTP(getRecorder, getReq)

	if getRecorder.Code != http.StatusNotFound {
		t.Fatalf("GET expected 404, got %d, body: %s", getRecorder.Code, getRecorder.Body.String())
	}

	var getResponse map[string]any

	if err := json.NewDecoder(getRecorder.Body).Decode(&getResponse); err != nil {
		t.Fatalf("failed to decode GET error response: %v", err)
	}

	if _, ok := postResponse["error"]; !ok {
		t.Fatal("POST error response missing top-level error field")
	}

	if _, ok := getResponse["error"]; !ok {
		t.Fatal("GET error response missing top-level error field")
	}

	postError, ok := postResponse["error"].(map[string]any)
	if !ok {
		t.Fatal("POST error field has unexpected shape")
	}

	getError, ok := getResponse["error"].(map[string]any)
	if !ok {
		t.Fatal("GET error field has unexpected shape")
	}

	if _, ok := postError["code"]; !ok {
		t.Fatal("POST error missing code")
	}

	if _, ok := postError["message"]; !ok {
		t.Fatal("POST error missing message")
	}

	if _, ok := getError["code"]; !ok {
		t.Fatal("GET error missing code")
	}

	if _, ok := getError["message"]; !ok {
		t.Fatal("GET error missing message")
	}
}
