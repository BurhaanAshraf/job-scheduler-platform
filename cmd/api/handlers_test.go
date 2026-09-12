package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
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
	apiKey := createTestAPIKey(t, pool)

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
	req.Header.Set("Authorization", "Bearer "+apiKey)

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
	apiKey := createTestAPIKey(t, pool)

	id := uuid.New()

	req := httptest.NewRequest(
		http.MethodGet, "/v1/jobs/"+id.String(), nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)

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

func TestAPI_ErrorResponseShape(t *testing.T) {
	pool := testDBPool(t)

	t.Cleanup(func() {
		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, handler)
	apiKey := createTestAPIKey(t, pool)

	postReq := httptest.NewRequest(
		http.MethodPost, "/v1/jobs", strings.NewReader(`{}`),
	)
	postReq.Header.Set("Authorization", "Bearer "+apiKey)
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
	getReq.Header.Set("Authorization", "Bearer "+apiKey)

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

func TestListJobs_OversizedLimit(t *testing.T) {
	pool := testDBPool(t)

	var jobIDs []uuid.UUID

	t.Cleanup(func() {
		ctx := context.Background()

		for _, jobID := range jobIDs {
			if _, err := pool.Exec(
				ctx,
				"DELETE FROM job_executions WHERE job_id = $1",
				jobID,
			); err != nil {
				t.Errorf("failed to cleanup job executions: %v", err)
			}

			if _, err := pool.Exec(
				ctx,
				"DELETE FROM jobs WHERE id = $1",
				jobID,
			); err != nil {
				t.Errorf("failed to cleanup job: %v", err)
			}
		}

		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)
	apiKey := createTestAPIKey(t, pool)

	callbackURL := "https://example.com/callback"

	// Create more jobs than the API's maximum list limit.
	for i := 0; i < 110; i++ {
		input := repository.CreateJobInput{
			Type:           "pagination-test",
			Payload:        json.RawMessage(`{"test":true}`),
			RunAt:          time.Now().UTC(),
			MaxAttempts:    3,
			IdempotencyKey: fmt.Sprintf("list-limit-%s-%d", uuid.NewString(), i),
			CallbackURL:    &callbackURL,
		}

		jobID, err := jobRepo.Create(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to create test job %d: %v", i, err)
		}

		jobIDs = append(jobIDs, jobID)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, handler)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs?status=pending&limit=1000&offset=0",
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"expected 200, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var jobs []repository.Job

	if err := json.NewDecoder(recorder.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode jobs response: %v", err)
	}

	if len(jobs) != maxJobListLimit {
		t.Fatalf(
			"expected %d jobs after limit capping, got %d",
			maxJobListLimit,
			len(jobs),
		)
	}
}

func TestListJobs_StatusFilter(t *testing.T) {
	pool := testDBPool(t)

	var jobIDs []uuid.UUID

	t.Cleanup(func() {
		ctx := context.Background()

		for _, jobID := range jobIDs {
			if _, err := pool.Exec(
				ctx,
				"DELETE FROM job_executions WHERE job_id = $1",
				jobID,
			); err != nil {
				t.Errorf("failed to cleanup job executions: %v", err)
			}

			if _, err := pool.Exec(
				ctx,
				"DELETE FROM jobs WHERE id = $1",
				jobID,
			); err != nil {
				t.Errorf("failed to cleanup job: %v", err)
			}
		}

		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)
	apiKey := createTestAPIKey(t, pool)

	callbackURL := "https://example.com/callback"

	createJob := func(key string) uuid.UUID {
		t.Helper()

		input := repository.CreateJobInput{
			Type:           "status-filter-test",
			Payload:        json.RawMessage(`{"test":true}`),
			RunAt:          time.Now().UTC(),
			MaxAttempts:    3,
			IdempotencyKey: key,
			CallbackURL:    &callbackURL,
		}

		jobID, err := jobRepo.Create(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to create test job: %v", err)
		}

		jobIDs = append(jobIDs, jobID)
		return jobID
	}

	pendingJobID := createJob("status-pending-" + uuid.NewString())
	doneJobID := createJob("status-done-" + uuid.NewString())

	if err := jobRepo.UpdateStatus(
		context.Background(),
		doneJobID,
		repository.StatusDone,
		nil,
	); err != nil {
		t.Fatalf("failed to update test job status: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, handler)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs?status=pending&limit=100&offset=0",
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"expected 200, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var jobs []repository.Job

	if err := json.NewDecoder(recorder.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode jobs response: %v", err)
	}

	foundPending := false

	for _, job := range jobs {
		if job.ID == doneJobID {
			t.Fatalf("status filter returned a done job")
		}

		if job.ID == pendingJobID {
			foundPending = true
		}

		if job.Status != repository.StatusPending {
			t.Fatalf(
				"expected only pending jobs, got status %q",
				job.Status,
			)
		}
	}

	if !foundPending {
		t.Fatal("status filter did not return the pending test job")
	}
}

func TestListJobs_Pagination(t *testing.T) {
	pool := testDBPool(t)

	var jobIDs []uuid.UUID

	t.Cleanup(func() {
		ctx := context.Background()

		for _, jobID := range jobIDs {
			if _, err := pool.Exec(
				ctx,
				"DELETE FROM job_executions WHERE job_id = $1",
				jobID,
			); err != nil {
				t.Errorf("failed to cleanup job executions: %v", err)
			}

			if _, err := pool.Exec(
				ctx,
				"DELETE FROM jobs WHERE id = $1",
				jobID,
			); err != nil {
				t.Errorf("failed to cleanup job: %v", err)
			}
		}

		pool.Close()
	})

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)
	apiKey := createTestAPIKey(t, pool)

	callbackURL := "https://example.com/callback"

	for i := 0; i < 3; i++ {
		input := repository.CreateJobInput{
			Type:           "pagination-test",
			Payload:        json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
			RunAt:          time.Now().UTC(),
			MaxAttempts:    3,
			IdempotencyKey: fmt.Sprintf("pagination-%s-%d", uuid.NewString(), i),
			CallbackURL:    &callbackURL,
		}

		jobID, err := jobRepo.Create(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to create test job %d: %v", i, err)
		}

		jobIDs = append(jobIDs, jobID)

		time.Sleep(2 * time.Millisecond)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	router := Server(logger, handler)

	// Fetch the ordered result set.
	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs?status=pending&limit=100&offset=0",
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"expected 200, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var allJobs []repository.Job

	if err := json.NewDecoder(recorder.Body).Decode(&allJobs); err != nil {
		t.Fatalf("failed to decode jobs response: %v", err)
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

	// Request the page beginning at our first test job.
	req = httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf(
			"/v1/jobs?status=pending&limit=2&offset=%d",
			firstPosition,
		),
		nil,
	)
	req.Header.Set("Authorization", "Bearer "+apiKey)

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"expected 200, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var page []repository.Job

	if err := json.NewDecoder(recorder.Body).Decode(&page); err != nil {
		t.Fatalf("failed to decode paginated response: %v", err)
	}

	if len(page) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(page))
	}

	if page[0].ID != jobIDs[0] {
		t.Fatalf(
			"expected first paginated job %s, got %s",
			jobIDs[0],
			page[0].ID,
		)
	}

	if page[1].ID != jobIDs[1] {
		t.Fatalf(
			"expected second paginated job %s, got %s",
			jobIDs[1],
			page[1].ID,
		)
	}
}

func TestDeleteJob(t *testing.T) {
	pool := testDBPool(t)

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)
	router := Server(slog.Default(), handler)
	apiKey := createTestAPIKey(t, pool)

	var pendingJobID uuid.UUID
	var runningJobID uuid.UUID

	t.Cleanup(func() {
		for _, id := range []uuid.UUID{pendingJobID, runningJobID} {
			if id == uuid.Nil {
				continue
			}

			_, err := pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", id)

			if err != nil {
				t.Errorf("failed to cleanup job executions: %v", err)
			}

			_, err = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id)

			if err != nil {
				t.Errorf("failed to cleanup job: %v", err)
			}
		}
		pool.Close()
	})

	createJob := func(t *testing.T, key string) uuid.UUID {
		t.Helper()
		callbackURL := "https://example.com/callback"
		id, err := jobRepo.Create(
			context.Background(), repository.CreateJobInput{
				Type:           "email",
				Payload:        json.RawMessage(`{"to":"test@example.com"}`),
				RunAt:          time.Now().UTC().Add(time.Hour),
				MaxAttempts:    3,
				IdempotencyKey: key,
				CallbackURL:    &callbackURL,
			},
		)

		if err != nil {
			t.Fatalf("failed to create test job: %v", err)
		}
		return id
	}
	pendingJobID = createJob(t, "delete-pending-"+uuid.NewString())
	runningJobID = createJob(t, "delete-running-"+uuid.NewString())

	err := jobRepo.UpdateStatus(context.Background(), runningJobID, repository.StatusRunning, nil)

	if err != nil {
		t.Fatalf("failed to set running job status: %v", err)
	}

	t.Run("cancel pending job", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodDelete, "/v1/jobs/"+pendingJobID.String(), nil,
		)
		req.Header.Set("Authorization", "Bearer "+apiKey)

		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, req)

		if recorder.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d, body: %s", recorder.Code, recorder.Body.String())
		}

		job, err := jobRepo.GetByID(context.Background(), pendingJobID)

		if err != nil {
			t.Fatalf("failed to get cancelled job: %v", err)
		}
		if job.Status != repository.StatusCancelled {
			t.Fatalf("expected status %q, got %q", repository.StatusCancelled, job.Status)
		}
	})

	t.Run("cannot cancel running job", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodDelete, "/v1/jobs/"+runningJobID.String(), nil,
		)
		req.Header.Set("Authorization", "Bearer "+apiKey)

		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, req)

		if recorder.Code != http.StatusConflict {
			t.Fatalf("expected 409, got %d, body: %s", recorder.Code, recorder.Body.String())
		}

		if !strings.Contains(
			recorder.Body.String(), "cannot be cancelled",
		) {
			t.Fatalf("expected cancellation explanation, got %s", recorder.Body.String())
		}
	})
}

func TestGetJob_InvalidID(t *testing.T) {
	handler := NewHandler(nil)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs/not-a-uuid",
		nil,
	)

	recorder := httptest.NewRecorder()

	handler.GetJob(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected 400, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var response api.ErrorResponse

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if response.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected code INVALID_REQUEST, got %q", response.Error.Code)
	}

	if response.Error.Message != "invalid job id" {
		t.Fatalf("expected message %q, got %q",
			"invalid job id",
			response.Error.Message,
		)
	}
}
func TestListJobs_InvalidLimit(t *testing.T) {
	handler := NewHandler(nil)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs?status=pending&limit=invalid",
		nil,
	)

	recorder := httptest.NewRecorder()

	handler.ListJobs(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected 400, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var response api.ErrorResponse

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if response.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected code INVALID_REQUEST, got %q", response.Error.Code)
	}

	if response.Error.Message != "limit must be a positive integer" {
		t.Fatalf(
			"unexpected error message: %q",
			response.Error.Message,
		)
	}
}
func TestListJobs_InvalidOffset(t *testing.T) {
	handler := NewHandler(nil)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs?status=pending&offset=-1",
		nil,
	)

	recorder := httptest.NewRecorder()

	handler.ListJobs(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected 400, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var response api.ErrorResponse

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if response.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected code INVALID_REQUEST, got %q", response.Error.Code)
	}

	if response.Error.Message != "offset must be a non-negative integer" {
		t.Fatalf(
			"unexpected error message: %q",
			response.Error.Message,
		)
	}
}
func TestListJobs_InvalidStatus(t *testing.T) {
	pool := testDBPool(t)

	t.Cleanup(func() {
		pool.Close()
	})

	handler := NewHandler(repository.NewJobRepository(pool))

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/jobs?status=invalid-status",
		nil,
	)

	recorder := httptest.NewRecorder()

	handler.ListJobs(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected 400, got %d, body: %s",
			recorder.Code,
			recorder.Body.String(),
		)
	}

	var response api.ErrorResponse

	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if response.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("expected code INVALID_REQUEST, got %q", response.Error.Code)
	}

	if !strings.Contains(response.Error.Message, "invalid job status") {
		t.Fatalf(
			"expected invalid job status message, got %q",
			response.Error.Message,
		)
	}
}
func TestDeleteJob_InvalidID(t *testing.T) {
	handler := NewHandler(nil)

	req := httptest.NewRequest(
		http.MethodDelete,
		"/v1/jobs/not-a-uuid",
		nil,
	)
	rec := httptest.NewRecorder()

	handler.DeleteJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	var response api.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response.Error.Code != "INVALID_REQUEST" {
		t.Errorf(
			"expected code INVALID_REQUEST, got %q",
			response.Error.Code,
		)
	}

	if response.Error.Message != "invalid job id" {
		t.Errorf(
			"expected message %q, got %q",
			"invalid job id",
			response.Error.Message,
		)
	}
}
func TestDeleteJob_NotFound(t *testing.T) {
	ctx := context.Background()

	cfg := config.Config{
		DBDSN:      os.Getenv("JOB_SCHEDULER_DB_DSN"),
		DBMaxConns: 20,
	}

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	defer pool.Close()

	repo := repository.NewJobRepository(pool)
	handler := NewHandler(repo)

	id := uuid.New()

	req := httptest.NewRequest(
		http.MethodDelete,
		"/v1/jobs/"+id.String(),
		nil,
	)
	req.SetPathValue("id", id.String())
	rec := httptest.NewRecorder()

	handler.DeleteJob(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}

	var response api.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response.Error.Code != "NOT_FOUND" {
		t.Errorf(
			"expected code NOT_FOUND, got %q",
			response.Error.Code,
		)
	}

	if response.Error.Message != "job not found" {
		t.Errorf(
			"expected message %q, got %q",
			"job not found",
			response.Error.Message,
		)
	}
}
func createTestAPIKey(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()

	rawKey := uuid.NewString()

	sum := sha256.Sum256([]byte(rawKey))
	hashedKey := hex.EncodeToString(sum[:])

	_, err := pool.Exec(
		context.Background(),
		`INSERT INTO api_keys (id, client_name, hashed_key, created_at)
		 VALUES ($1, $2, $3, $4)`,
		uuid.New(),
		"test-client",
		hashedKey,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("failed to create test API key: %v", err)
	}

	return rawKey
}
