package scheduler_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
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
		client.Close()
		t.Fatalf("redis ping failed: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
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
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("failed to schedule job: %v", err)
	}

	s := scheduler.New(redisClient, cfg.PollInterval)

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
		runAt,
	); err != nil {
		t.Fatalf("ScheduleJob failed: %v", err)
	}

	s := scheduler.New(redisClient, cfg.PollInterval)

	done := make(chan error, 1)

	go func() {
		done <- s.Run(ctx)
	}()

	// The job must not be promoted before its scheduled time.
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

	// Wait until the job becomes due and allow the poller to run.
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

	if err := redisClient.Del(
		ctx,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
		stream.ReadyStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	s := scheduler.New(redisClient, cfg.PollInterval)

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
