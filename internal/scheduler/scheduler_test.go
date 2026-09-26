package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func testSchedulerRedis(t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})

	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("redis ping failed: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func TestPromoteDue_EnqueuesAndRemovesJob(t *testing.T) {
	ctx := context.Background()

	redisClient := testSchedulerRedis(t)

	cleanupRedis := func() {
		if err := redisClient.Del(
			ctx,
			stream.ScheduledSet,
			stream.ScheduledPayloads,
			stream.ReadyStream,
		).Err(); err != nil {
			t.Fatalf("failed to clean redis: %v", err)
		}
	}

	cleanupRedis()
	t.Cleanup(cleanupRedis)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("error loading config file: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(pool.Close)

	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)

	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(callback.Close)

	callbackURL := callback.URL

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "scheduler-test",
		Payload:        []byte(`{"message":"due"}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", jobID)
		if err != nil {
			t.Errorf("failed to clean up job: %v", err)
		}
	})

	if err := stream.ScheduleJob(
		ctx,
		redisClient,
		jobID.String(),
		[]byte(`{"message":"due"}`),
		1,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("failed to schedule job: %v", err)
	}

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		"scheduler:test:promote-due",
		"test-promote-due",
		10*time.Second,
	)
	testLog := logger.New("scheduler-test")
	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		testLog,
	)

	if _, err := s.PromoteDue(
		ctx,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("PromoteDue failed: %v", err)
	}

	scheduledCount, err := redisClient.ZCard(
		ctx,
		stream.ScheduledSet,
	).Result()
	if err != nil {
		t.Fatalf("failed to inspect scheduled set: %v", err)
	}

	if scheduledCount != 0 {
		t.Fatalf("expected scheduled set to be empty, got %d", scheduledCount)
	}

	messages, err := redisClient.XRange(
		ctx,
		stream.ReadyStream,
		"-",
		"+",
	).Result()
	if err != nil {
		t.Fatalf("failed to inspect ready stream: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 ready message, got %d", len(messages))
	}

	values := messages[0].Values

	if values["job_id"] != jobID.String() {
		t.Fatalf(
			"job_id = %v, want %s",
			values["job_id"],
			jobID.String(),
		)
	}

	if values["payload"] != `{"message":"due"}` {
		t.Fatalf(
			"payload = %v, want %s",
			values["payload"],
			`{"message":"due"}`,
		)
	}
}

func TestRun_PromotesDueJobAutomatically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := testSchedulerRedis(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("error loading config: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(pool.Close)

	cronRepo := repository.NewCronJobRepository(pool)

	if err := redisClient.Del(
		ctx,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
		stream.ReadyStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	t.Cleanup(func() {
		_ = redisClient.Del(
			context.Background(),
			stream.ScheduledSet,
			stream.ScheduledPayloads,
			stream.ReadyStream,
		).Err()
	})

	jobID := uuid.NewString()
	runAt := time.Now().UTC().Add(2 * time.Second)

	if err := stream.ScheduleJob(
		ctx,
		redisClient,
		jobID,
		[]byte(`{"message":"future"}`),
		1,
		runAt,
	); err != nil {
		t.Fatalf("ScheduleJob failed: %v", err)
	}

	const lockKey = "scheduler:test:promote-due"

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		lockKey,
		"test-promote-due",
		10*time.Second,
	)

	if err := redisClient.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("failed to clean stale leader lock: %v", err)
	}

	acquired, err := leaderLock.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire leader lock: %v", err)
	}

	if !acquired {
		t.Fatal("test scheduler should acquire leader lock")
	}

	defer redisClient.Del(ctx, lockKey)

	testLog := logger.New("scheduler-test")

	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		testLog,
	)

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx)
	}()

	time.Sleep(1 * time.Second)

	count, err := redisClient.XLen(ctx, stream.ReadyStream).Result()
	if err != nil {
		t.Fatalf("failed to inspect ready stream: %v", err)
	}

	if count != 0 {
		t.Fatalf(
			"job was promoted before run_at: ready stream contains %d messages",
			count,
		)
	}

	time.Sleep(1500 * time.Millisecond)

	count, err = redisClient.XLen(ctx, stream.ReadyStream).Result()
	if err != nil {
		t.Fatalf("failed to inspect ready stream: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected 1 ready message after run_at, got %d",
			count,
		)
	}

	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestRun_EmptyQueueDoesNotError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := testSchedulerRedis(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(pool.Close)

	cronRepo := repository.NewCronJobRepository(pool)

	if err := redisClient.Del(
		ctx,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
		stream.ReadyStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	const lockKey = "scheduler:test:empty-queue"

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		lockKey,
		"test-empty-queue",
		10*time.Second,
	)

	if err := redisClient.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("failed to clean leader lock: %v", err)
	}

	acquired, err := leaderLock.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire leader lock: %v", err)
	}

	if !acquired {
		t.Fatal("test scheduler should acquire leader lock")
	}
	testLog := logger.New("scheduler-test")

	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		testLog,
	)

	defer redisClient.Del(ctx, lockKey)

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx)
	}()

	time.Sleep(1500 * time.Millisecond)

	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}
func TestRun_CronJobDueNowCreatesInstance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := testSchedulerRedis(t)

	if err := redisClient.Del(
		ctx,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
		stream.ReadyStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	t.Cleanup(func() {
		_ = redisClient.Del(
			context.Background(),
			stream.ScheduledSet,
			stream.ScheduledPayloads,
			stream.ReadyStream,
		).Err()
	})

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(pool.Close)

	cronRepo := repository.NewCronJobRepository(pool)

	now := time.Now().UTC().Truncate(time.Second)

	cronJob, err := cronRepo.Create(ctx, repository.CreateCronJobInput{
		CronExpression: "* * * * *",
		JobTemplate: []byte(`{
			"type": "cron-run-test",
			"payload": {"message": "cron"},
			"max_attempts": 3
		}`),
		NextRunAt: now,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("failed to create cron job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM cron_jobs WHERE id = $1",
			cronJob.ID,
		)
		if err != nil {
			t.Errorf("failed to clean up cron job: %v", err)
		}
	})

	const lockKey = "scheduler:test:cron-run"

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		lockKey,
		"test-cron-run",
		10*time.Second,
	)

	if err := redisClient.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("failed to clean stale leader lock: %v", err)
	}

	acquired, err := leaderLock.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire leader lock: %v", err)
	}

	if !acquired {
		t.Fatal("test scheduler should acquire leader lock")
	}

	defer redisClient.Del(ctx, lockKey)

	testLog := logger.New("scheduler-test")

	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		testLog,
	)

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx)
	}()

	deadline := time.Now().Add(2 * cfg.PollInterval)

	for time.Now().Before(deadline) {
		var count int

		err := pool.QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM jobs
			WHERE idempotency_key LIKE $1
			`,
			fmt.Sprintf("cron:%d:%%", cronJob.ID),
		).Scan(&count)
		if err != nil {
			t.Fatalf("failed to count cron instances: %v", err)
		}

		if count == 1 {
			cancel()

			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run returned %v, want context.Canceled", err)
			}

			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	t.Fatal("cron job did not produce an instance within one tick interval")
}
func TestTickCronJobs_EveryMinuteCreatesExactlyThreeInstances(t *testing.T) {
	ctx := context.Background()

	redisClient := testSchedulerRedis(t)

	if err := redisClient.Del(
		ctx,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
		stream.ReadyStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	t.Cleanup(func() {
		_ = redisClient.Del(
			context.Background(),
			stream.ScheduledSet,
			stream.ScheduledPayloads,
			stream.ReadyStream,
		).Err()
	})

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(pool.Close)

	cronRepo := repository.NewCronJobRepository(pool)

	start := time.Date(
		2026,
		9,
		23,
		10,
		0,
		0,
		0,
		time.UTC,
	)

	cronJob, err := cronRepo.Create(ctx, repository.CreateCronJobInput{
		CronExpression: "* * * * *",
		JobTemplate: []byte(`{
			"type": "cron-test",
			"payload": {"message": "tick"},
			"max_attempts": 3
		}`),
		NextRunAt: start,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("failed to create cron job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM cron_jobs WHERE id = $1",
			cronJob.ID,
		)
		if err != nil {
			t.Errorf("failed to clean up cron job: %v", err)
		}
	})

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		"scheduler:test:cron-ticks",
		"test-cron-ticks",
		10*time.Second,
	)

	testLog := logger.New("scheduler-test")

	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		testLog,
	)

	ticks := []time.Time{
		start,
		start.Add(1 * time.Minute),
		start.Add(2 * time.Minute),
	}

	for i, tick := range ticks {
		created, err := s.TickCronJobs(ctx, tick)
		if err != nil {
			t.Fatalf("tick %d failed: %v", i+1, err)
		}

		if created != 1 {
			t.Fatalf(
				"tick %d created %d instances, want 1",
				i+1,
				created,
			)
		}
	}

	var count int
	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM jobs
		WHERE idempotency_key LIKE $1
		`,
		fmt.Sprintf("cron:%d:%%", cronJob.ID),
	).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count cron instances: %v", err)
	}

	if count != 3 {
		t.Fatalf("cron instances = %d, want exactly 3", count)
	}

	var nextRunAt time.Time
	err = pool.QueryRow(
		ctx,
		"SELECT next_run_at FROM cron_jobs WHERE id = $1",
		cronJob.ID,
	).Scan(&nextRunAt)
	if err != nil {
		t.Fatalf("failed to read next_run_at: %v", err)
	}

	wantNextRunAt := start.Add(3 * time.Minute)

	if !nextRunAt.Equal(wantNextRunAt) {
		t.Fatalf(
			"next_run_at = %v, want %v",
			nextRunAt,
			wantNextRunAt,
		)
	}
}
