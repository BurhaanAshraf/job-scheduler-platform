package scheduler_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
)

// 6.5: the poller stays quiet on empty (no error logs, clean cancel).
func TestRun_EmptyQueueNoErrorLogs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := testSchedulerRedis(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("db pool: %v", err)
	}
	t.Cleanup(pool.Close)
	cronRepo := repository.NewCronJobRepository(pool)

	if err := redisClient.Del(ctx, stream.ScheduledSet, stream.ScheduledPayloads, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("clean redis: %v", err)
	}

	const lockKey = "scheduler:test:empty-quiet"
	lock := scheduler.NewLeaderLock(redisClient, lockKey, "quiet-1", 10*time.Second)
	if err := redisClient.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("clean lock: %v", err)
	}
	if ok, err := lock.Acquire(ctx); err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}

	var buf bytes.Buffer
	testLog := slog.New(slog.NewJSONHandler(&buf, nil))
	s := scheduler.New(redisClient, cronRepo, 100*time.Millisecond, lock, testLog)
	defer redisClient.Del(ctx, lockKey)

	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	time.Sleep(2 * time.Second)
	cancel()
	if err := <-done; err != context.Canceled && err != nil {
		// Run returns ctx.Err(); accept canceled.
		t.Fatalf("Run: %v", err)
	}
	out := buf.String()
	if bytes.Contains([]byte(out), []byte(`"level":"ERROR"`)) {
		t.Fatalf("empty poller emitted error logs: %s", out)
	}
}

// 9.3: two schedulers, one leader dispatches; the other stays idle.
func TestRun_TwoSchedulersSingleDispatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := testSchedulerRedis(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("db pool: %v", err)
	}
	t.Cleanup(pool.Close)
	cronRepo := repository.NewCronJobRepository(pool)

	if err := redisClient.Del(ctx, stream.ScheduledSet, stream.ScheduledPayloads, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("clean redis: %v", err)
	}
	const lockKey = "scheduler:test:two-instances"
	if err := redisClient.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("clean lock: %v", err)
	}

	var bufA, bufB bytes.Buffer
	lockA := scheduler.NewLeaderLock(redisClient, lockKey, "sched-A", 10*time.Second)
	lockB := scheduler.NewLeaderLock(redisClient, lockKey, "sched-B", 10*time.Second)
	sA := scheduler.New(redisClient, cronRepo, 100*time.Millisecond, lockA, slog.New(slog.NewJSONHandler(&bufA, nil)))
	sB := scheduler.New(redisClient, cronRepo, 100*time.Millisecond, lockB, slog.New(slog.NewJSONHandler(&bufB, nil)))

	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- sA.Run(ctx) }()
	time.Sleep(300 * time.Millisecond) // let A win the race deterministically
	go func() { doneB <- sB.Run(ctx) }()
	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-doneA
	<-doneB

	aAcquired := bytes.Contains(bufA.Bytes(), []byte("acquired scheduler leadership"))
	bAcquired := bytes.Contains(bufB.Bytes(), []byte("acquired scheduler leadership"))
	if aAcquired == bAcquired {
		t.Fatalf("expected exactly one leader: A=%v B=%v", aAcquired, bAcquired)
	}
}

// 8.5: crash between DB commit and Redis enqueue must not duplicate.
func TestTickCronJobs_CrashBetweenCommitAndEnqueueNoDuplicate(t *testing.T) {
	ctx := context.Background()
	redisClient := testSchedulerRedis(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("db pool: %v", err)
	}
	t.Cleanup(pool.Close)
	cronRepo := repository.NewCronJobRepository(pool)
	if err := redisClient.Del(ctx, stream.ScheduledSet, stream.ScheduledPayloads, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("clean redis: %v", err)
	}

	cron, err := cronRepo.Create(ctx, repository.CreateCronJobInput{
		CronExpression: "* * * * *",
		JobTemplate:    []byte(`{"type":"email","payload":{"to":"a@b.c"},"max_attempts":3,"callback_url":"https://example.com/callback"}`),
		NextRunAt:      time.Now().UTC().Add(-time.Minute),
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("create cron: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM jobs WHERE idempotency_key LIKE 'cron:%'")
		pool.Exec(context.Background(), "DELETE FROM cron_jobs WHERE id = $1", cron.ID)
	})

	// Simulate the DB half committing without the Redis enqueue: call the
	// transactional constructor directly and deliberately skip EnqueueDue
	// (the crash window).
	inst, ok, err := cronRepo.CreateDueInstance(ctx, cron.ID, time.Now().UTC(), scheduler.NextRunAt)
	if err != nil {
		t.Fatalf("CreateDueInstance: %v", err)
	}
	if !ok {
		t.Fatal("expected first tick to create an instance")
	}

	// Recovery tick: next_run_at already advanced, so no duplicate row.
	inst2, ok2, err := cronRepo.CreateDueInstance(ctx, cron.ID, time.Now().UTC(), scheduler.NextRunAt)
	if err != nil {
		t.Fatalf("recovery tick: %v", err)
	}
	if ok2 {
		t.Fatalf("recovery created duplicate instance %v (first %v)", inst2.ID, inst.ID)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1", inst.IdempotencyKey).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("idempotency key %q has %d rows, want 1", inst.IdempotencyKey, n)
	}
	_ = logger.New
}
