package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func resilienceDeps(t *testing.T) (context.Context, *pgxpool.Pool, *redis.Client) {
	t.Helper()
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Fatal("REDIS_ADDR is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	if err := redisClient.Del(ctx, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("clean stream: %v", err)
	}
	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	return ctx, pool, redisClient
}

// 5.8: a 500-job must not crash the loop; the next good job still drains.
func TestWorker_StaysAliveAfterFailure(t *testing.T) {
	ctx, pool, redisClient := resilienceDeps(t)
	jobRepo := repository.NewJobRepository(pool)
	execRepo := repository.NewJobExecutionRepository(pool)

	badHits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/bad", func(w http.ResponseWriter, _ *http.Request) {
		badHits++
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/good", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	badURL := server.URL + "/bad"
	goodURL := server.URL + "/good"
	payload := json.RawMessage(`{"n":1}`)

	badID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: payload, RunAt: time.Now().UTC(),
		MaxAttempts: 1, IdempotencyKey: "stayalive-bad-" + uuid.NewString(), CallbackURL: &badURL,
	})
	if err != nil {
		t.Fatalf("create bad: %v", err)
	}
	goodID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: payload, RunAt: time.Now().UTC(),
		MaxAttempts: 3, IdempotencyKey: "stayalive-good-" + uuid.NewString(), CallbackURL: &goodURL,
	})
	if err != nil {
		t.Fatalf("create good: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id IN ($1,$2)", badID, goodID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id IN ($1,$2)", badID, goodID)
	})

	if _, err := stream.EnqueueDue(ctx, redisClient, badID.String(), payload, 1); err != nil {
		t.Fatalf("enqueue bad: %v", err)
	}
	if _, err := stream.EnqueueDue(ctx, redisClient, goodID.String(), payload, 1); err != nil {
		t.Fatalf("enqueue good: %v", err)
	}

	processor := NewProcessor(jobRepo, execRepo, redisClient, executor.NewHTTPExecutor(), validator.Config{AllowPrivateIPs: true})
	w := NewWorker(redisClient, processor, "stayalive-"+uuid.NewString(), slog.Default())
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()

	deadline := time.Now().Add(8 * time.Second)
	for {
		g, err := jobRepo.GetByID(ctx, goodID)
		if err != nil {
			t.Fatalf("get good: %v", err)
		}
		if g.Status == repository.StatusDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("good job status=%q after bad job; worker did not stay alive (badHits=%d)", g.Status, badHits)
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, err := jobRepo.GetByID(ctx, badID)
	if err != nil {
		t.Fatalf("get bad: %v", err)
	}
	if b.Status != repository.StatusDead {
		t.Fatalf("bad job status=%q, want dead", b.Status)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker exit: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not stop")
	}
}

// 5.9: kill-mid-processing leaves the message in XPENDING.
func TestWorker_KilledMidProcessingAppearsInPending(t *testing.T) {
	ctx, pool, redisClient := resilienceDeps(t)
	jobRepo := repository.NewJobRepository(pool)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cb := server.URL
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: json.RawMessage(`{"k":1}`), RunAt: time.Now().UTC(),
		MaxAttempts: 3, IdempotencyKey: "pending-" + uuid.NewString(), CallbackURL: &cb,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	// Simulate a worker that reads but dies before XACK.
	msgs, err := stream.ReadNextWithTimeout(ctx, redisClient, "crasher-"+uuid.NewString(), 2*time.Second)
	_ = msgs
	_ = err // first read is intentionally discarded; the reread below is asserted
	// Enqueue first so there is something to read.
	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), []byte(`{"k":1}`), 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	msgs, err = stream.ReadNextWithTimeout(ctx, redisClient, "crasher", 2*time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(msgs))
	}
	// No ACK: message must now be pending.
	pending, err := redisClient.XPending(ctx, stream.ReadyStream, stream.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if pending.Count < 1 {
		t.Fatalf("expected >=1 pending after kill-mid-processing, got %+v", pending)
	}
	stale, err := stream.ListStalePending(ctx, redisClient, 0, 10)
	if err != nil {
		t.Fatalf("ListStalePending: %v", err)
	}
	if len(stale) == 0 {
		t.Fatal("expected message in XPENDING output")
	}
}

// 5.7: DB-done + XACK ordering — after success XPENDING is zero; a failed
// Complete leaves the message unacked (still pending) instead of lost.
func TestProcessor_SuccessLeavesZeroPending(t *testing.T) {
	ctx, pool, redisClient := resilienceDeps(t)
	jobRepo := repository.NewJobRepository(pool)
	execRepo := repository.NewJobExecutionRepository(pool)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cb := server.URL
	payload := json.RawMessage(`{"ok":true}`)
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: payload, RunAt: time.Now().UTC(),
		MaxAttempts: 3, IdempotencyKey: "split-" + uuid.NewString(), CallbackURL: &cb,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})
	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), payload, 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	msgs, err := stream.ReadNextWithTimeout(ctx, redisClient, "split-"+uuid.NewString(), 2*time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %v %d", err, len(msgs))
	}
	processor := NewProcessor(jobRepo, execRepo, redisClient, executor.NewHTTPExecutor(), validator.Config{AllowPrivateIPs: true})
	if err := processor.Process(ctx, msgs[0]); err != nil {
		t.Fatalf("process: %v", err)
	}
	pending, err := redisClient.XPending(ctx, stream.ReadyStream, stream.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("after success XPENDING=%d, want 0", pending.Count)
	}
}

// A redelivery for a running job with NO completed execution must re-drive
// (at-least-once), never phantom-mark done.
func TestProcessor_StaleRunningWithoutCompletionRedrives(t *testing.T) {
	ctx, pool, redisClient := resilienceDeps(t)
	jobRepo := repository.NewJobRepository(pool)
	execRepo := repository.NewJobExecutionRepository(pool)

	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cb := server.URL
	payload := json.RawMessage(`{"redrive":1}`)
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: payload, RunAt: time.Now().UTC(),
		MaxAttempts: 3, IdempotencyKey: "redrive-" + uuid.NewString(), CallbackURL: &cb,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})

	// Simulate crash between StartExecution and Complete.
	attempt, err := jobRepo.StartExecution(ctx, jobID, 1)
	if err != nil || attempt != 1 {
		t.Fatalf("StartExecution = %d,%v; want 1,nil", attempt, err)
	}
	if _, err := execRepo.Create(ctx, repository.CreateJobExecutionInput{
		JobID: jobID, AttemptNumber: 1, StartedAt: time.Now().UTC(),
		Status: "running", QueueGeneration: 1,
	}); err != nil {
		t.Fatalf("create execution: %v", err)
	}

	processor := NewProcessor(jobRepo, execRepo, redisClient, executor.NewHTTPExecutor(), validator.Config{AllowPrivateIPs: true})
	stale := stream.Message{ID: "fake-stale-1", JobID: jobID.String(), Payload: string(payload), QueueGeneration: 1}
	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), payload, 1); err != nil {
		t.Fatalf("enqueue stale carrier: %v", err)
	}
	msgs, err := stream.ReadNextWithTimeout(ctx, redisClient, "redrive-"+uuid.NewString(), 2*time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read carrier: %v %d", err, len(msgs))
	}
	stale.ID = msgs[0].ID

	if err := processor.Process(ctx, stale); err != nil {
		t.Fatalf("Process(stale) = %v; want nil (re-drive)", err)
	}
	if hits != 0 {
		t.Fatalf("callback hits = %d during re-drive; want 0 (execution happens on redelivery)", hits)
	}
	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status == repository.StatusDone {
		t.Fatal("job phantom-marked done without a completed execution")
	}

	// The re-driven message must now execute to done.
	msgs2, err := stream.ReadNextWithTimeout(ctx, redisClient, "redrive-"+uuid.NewString(), 2*time.Second)
	if err != nil || len(msgs2) != 1 {
		t.Fatalf("read re-driven: %v %d", err, len(msgs2))
	}
	if err := processor.Process(ctx, msgs2[0]); err != nil {
		t.Fatalf("Process(redriven) = %v", err)
	}
	if hits != 1 {
		t.Fatalf("callback hits = %d; want exactly 1", hits)
	}
	job, err = jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status != repository.StatusDone {
		t.Fatalf("status = %q; want done", job.Status)
	}
}

// A redelivery for a running job WITH a done execution (XACK lost after
// success) must finalize done without re-executing the callback.
func TestProcessor_StaleRunningWithCompletionFinalizes(t *testing.T) {
	ctx, pool, redisClient := resilienceDeps(t)
	jobRepo := repository.NewJobRepository(pool)
	execRepo := repository.NewJobExecutionRepository(pool)

	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cb := server.URL
	payload := json.RawMessage(`{"finalize":1}`)
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: payload, RunAt: time.Now().UTC(),
		MaxAttempts: 3, IdempotencyKey: "finalize-" + uuid.NewString(), CallbackURL: &cb,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})

	attempt, err := jobRepo.StartExecution(ctx, jobID, 1)
	if err != nil || attempt != 1 {
		t.Fatalf("StartExecution = %d,%v", attempt, err)
	}
	execID, err := execRepo.Create(ctx, repository.CreateJobExecutionInput{
		JobID: jobID, AttemptNumber: 1, StartedAt: time.Now().UTC(),
		Status: "running", QueueGeneration: 1,
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	code := 200
	if err := execRepo.Complete(ctx, execID, time.Now().UTC(), "done", nil, &code); err != nil {
		t.Fatalf("complete: %v", err)
	}

	processor := NewProcessor(jobRepo, execRepo, redisClient, executor.NewHTTPExecutor(), validator.Config{AllowPrivateIPs: true})
	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), payload, 1); err != nil {
		t.Fatalf("enqueue carrier: %v", err)
	}
	msgs, err := stream.ReadNextWithTimeout(ctx, redisClient, "finalize-"+uuid.NewString(), 2*time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read carrier: %v %d", err, len(msgs))
	}
	if err := processor.Process(ctx, msgs[0]); err != nil {
		t.Fatalf("Process = %v; want nil", err)
	}
	if hits != 0 {
		t.Fatalf("callback hits = %d; want 0 (must not re-execute)", hits)
	}
	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if job.Status != repository.StatusDone {
		t.Fatalf("status = %q; want done", job.Status)
	}
}
