package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
)

func TestCreateCronJob_InvalidExpression400(t *testing.T) {
	pool := testDBPool(t)
	redisClient := testRateLimitRedis(t)
	t.Cleanup(pool.Close)

	handler := NewHandler(
		repository.NewJobRepository(pool),
		repository.NewCronJobRepository(pool),
		redisClient,
		slog.Default(),
	)

	body := `{"cron_expression": "not-a-cron", "job_template": {"type":"email","payload":{"to":"a@b.c"},"max_attempts":3,"callback_url":"https://example.com/callback"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/cron-jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.CreateCronJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid cron: got %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(strings.ToLower(rec.Body.String()), "cron") {
		t.Fatalf("400 body should identify the cron parse failure, got: %s", rec.Body.String())
	}
}

func TestCreateCronJob_ValidThenDisableStopsInstances(t *testing.T) {
	pool := testDBPool(t)
	redisClient := testRateLimitRedis(t)
	t.Cleanup(pool.Close)
	ctx := context.Background()

	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)
	handler := NewHandler(jobRepo, cronRepo, redisClient, slog.Default())

	body := `{"cron_expression": "* * * * *", "job_template": {"type":"email","payload":{"to":"a@b.c"},"max_attempts":3,"callback_url":"https://example.com/callback"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/cron-jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.CreateCronJob(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cron: got %d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode cron id: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM jobs WHERE idempotency_key LIKE $1", "cron:%")
		pool.Exec(context.Background(), "DELETE FROM cron_jobs WHERE id = $1", created.ID)
	})

	// Force due now so a tick would materialize an instance.
	_, err := pool.Exec(ctx, "UPDATE cron_jobs SET next_run_at = $1 WHERE id = $2", time.Now().UTC().Add(-time.Minute), created.ID)
	if err != nil {
		t.Fatalf("force due: %v", err)
	}

	// Disable via endpoint.
	disableReq := httptest.NewRequest(
		http.MethodPatch,
		"/v1/cron-jobs/1",
		strings.NewReader(`{"enabled": false}`),
	)
	disableReq.SetPathValue("id", jsonNumber(created.ID))
	disableReq.Header.Set("Content-Type", "application/json")
	disableRec := httptest.NewRecorder()
	handler.UpdateCronJob(disableRec, disableReq)
	if disableRec.Code != http.StatusNoContent {
		t.Fatalf("disable: got %d body=%s", disableRec.Code, disableRec.Body.String())
	}

	// Subsequent ticks must create no new instances.
	due, err := cronRepo.ListDue(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	for _, c := range due {
		if c.ID == created.ID {
			t.Fatalf("disabled cron %d still listed as due", created.ID)
		}
	}
}

func jsonNumber(id int64) string {
	var sb strings.Builder
	json.NewEncoder(&sb).Encode(id)
	return strings.TrimSpace(sb.String())
}
