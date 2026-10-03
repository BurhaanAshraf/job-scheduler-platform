package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/google/uuid"
)

// Regression: Postgres jsonb normalizes payload whitespace on write
// ({"a":1} becomes {"a": 1}), so an idempotent replay with byte-identical
// JSON must still return 200 + the same id, not 409.
func TestCreateJob_IdempotentReplayCompactPayload(t *testing.T) {
	pool := testDBPool(t)
	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)
	redisClient := testRateLimitRedis(t)
	handler := NewHandler(jobRepo, cronRepo, redisClient, slog.Default())

	idempotencyKey := uuid.New().String()
	var jobID uuid.UUID
	t.Cleanup(func() {
		if jobID != uuid.Nil {
			_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
			_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
		}
		pool.Close()
	})

	// Compact payload: no spaces, so stored bytes WILL differ from sent bytes.
	body := `{"type":"email","payload":{"to":"compact@example.com"},"run_at":"2026-09-07T12:00:00Z","max_attempts":3,"idempotency_key":"` + idempotencyKey + `","callback_url":"https://example.com/callback"}`
	doPost := func() *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.CreateJob(rec, req)
		return rec
	}

	first := doPost()
	if first.Code != http.StatusCreated {
		t.Fatalf("first: got %d body=%s", first.Code, first.Body.String())
	}
	var firstResp struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(first.Body).Decode(&firstResp); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	jobID = firstResp.ID

	second := doPost()
	if second.Code != http.StatusOK {
		t.Fatalf("replay: got %d body=%s, want 200 (jsonb normalization must not 409)", second.Code, second.Body.String())
	}
	var secondResp struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(second.Body).Decode(&secondResp); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if secondResp.ID != firstResp.ID {
		t.Fatalf("replay id %v != first id %v", secondResp.ID, firstResp.ID)
	}
}

func TestJsonPayloadEqual(t *testing.T) {
	if !jsonPayloadEqual([]byte(`{"a":1}`), []byte(`{"a": 1}`)) {
		t.Fatal("whitespace-normalized payloads should be equal")
	}
	if !jsonPayloadEqual([]byte(`{"a":1,"b":[1,2]}`), []byte(`{"b": [1, 2], "a": 1}`)) {
		t.Fatal("key-order/normalized payloads should be equal")
	}
	if jsonPayloadEqual([]byte(`{"a":1}`), []byte(`{"a":2}`)) {
		t.Fatal("different payloads should not be equal")
	}
}
