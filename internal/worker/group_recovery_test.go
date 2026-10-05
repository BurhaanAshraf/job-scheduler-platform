package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
	"github.com/google/uuid"
)

func TestIsNoGroupError(t *testing.T) {
	if isNoGroupError(nil) {
		t.Error("nil is not a NOGROUP error")
	}
	if isNoGroupError(errors.New("connection refused")) {
		t.Error("connection error is not a NOGROUP error")
	}
	if !isNoGroupError(errors.New("read from stream: NOGROUP No such key 'jobs:ready' or consumer group 'workers' in XREADGROUP with GROUP option")) {
		t.Error("NOGROUP reply not detected")
	}
}

// The consumer group vanishes AFTER worker startup (key eviction, FLUSHDB,
// Redis restore/failover). The running worker must recreate it and drain
// the backlog instead of error-looping forever.
func TestWorker_RecoversWhenConsumerGroupDeleted(t *testing.T) {
	ctx, pool, redisClient := resilienceDeps(t)
	jobRepo := repository.NewJobRepository(pool)
	execRepo := repository.NewJobExecutionRepository(pool)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cb := server.URL
	payload := json.RawMessage(`{"k":"recovery"}`)
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type: "t", Payload: payload, RunAt: time.Now().UTC(),
		MaxAttempts: 2, IdempotencyKey: "group-recovery-" + uuid.NewString(), CallbackURL: &cb,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})

	processor := NewProcessor(jobRepo, execRepo, redisClient, executor.NewHTTPExecutor(), validator.Config{AllowPrivateIPs: true})
	w := NewWorker(redisClient, processor, "recovery-"+uuid.NewString(), slog.Default())
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()

	// Let the worker start and block on XREADGROUP, then kill the group
	// underneath it and enqueue a job into the groupless stream.
	time.Sleep(300 * time.Millisecond)
	if err := redisClient.XGroupDestroy(ctx, stream.ReadyStream, stream.ConsumerGroup).Err(); err != nil {
		t.Fatalf("destroy group: %v", err)
	}
	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), []byte(`{"k":"recovery"}`), 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		g, err := jobRepo.GetByID(ctx, jobID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if g.Status == repository.StatusDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job status=%q 10s after group loss; worker did not recover", g.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}
