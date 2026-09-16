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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestProcessor_Process_SuccessUpdatesPostgresAndAcknowledges(t *testing.T) {
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
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	t.Cleanup(func() {
		redisClient.Close()
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}

	// Use a clean stream for this integration test.
	if err := redisClient.Del(ctx, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("failed to clean Redis stream: %v", err)
	}

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	payload := []byte(`{"type":"email","to":"test@example.com","message":"hello"}`)

	var receivedPayload []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read callback body: %v", err)
			return
		}

		receivedPayload = body

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)

	idempotencyKey := "worker-test-" + uuid.NewString()
	callbackURL := server.URL
	runAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)

	createdJobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(payload),
		RunAt:          runAt,
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    &callbackURL,
	})

	jobID := createdJobID
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	messageID, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
	)
	if err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	messages, err := stream.ReadNext(
		ctx,
		redisClient,
		"worker-test-"+uuid.NewString(),
	)
	if err != nil {
		t.Fatalf("failed to read queued job: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	message := messages[0]

	if message.ID != messageID {
		t.Fatalf("message ID = %q, want %q", message.ID, messageID)
	}

	processor := NewProcessor(
		jobRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	if err := processor.Process(ctx, message); err != nil {
		t.Fatalf("Process() failed: %v", err)
	}

	if string(receivedPayload) != string(payload) {
		t.Fatalf(
			"callback payload = %q, want %q",
			string(receivedPayload),
			string(payload),
		)
	}

	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to retrieve processed job: %v", err)
	}

	if job.Status != repository.StatusDone {
		t.Fatalf(
			"job status = %q, want %q",
			job.Status,
			repository.StatusDone,
		)
	}

	pending, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.ReadyStream,
		Group:  stream.ConsumerGroup,
		Start:  message.ID,
		End:    message.ID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("failed to inspect pending message: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf(
			"expected message %q to have zero pending entries, got %d",
			message.ID,
			len(pending),
		)
	}
}

func TestProcessor_Process_FailureRecordsErrorAndLeavesMessagePending(t *testing.T) {
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
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	t.Cleanup(func() {
		redisClient.Close()
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}

	if err := redisClient.Del(ctx, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("failed to clean Redis stream: %v", err)
	}

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	payload := []byte(`{"type":"email","message":"should fail"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)

	callbackURL := server.URL
	idempotencyKey := "worker-failure-test-" + uuid.NewString()

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	messageID, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
	)
	if err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	consumer := "worker-failure-test-" + uuid.NewString()

	messages, err := stream.ReadNext(
		ctx,
		redisClient,
		consumer,
	)
	if err != nil {
		t.Fatalf("failed to read queued job: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	message := messages[0]

	processor := NewProcessor(
		jobRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	err = processor.Process(ctx, message)
	if err == nil {
		t.Fatal("expected Process() to return callback failure")
	}

	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to retrieve failed job: %v", err)
	}

	if job.LastError == nil {
		t.Fatal("expected last_error to be recorded")
	}

	if *job.LastError == "" {
		t.Fatal("expected last_error to be non-empty")
	}

	pending, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.ReadyStream,
		Group:  stream.ConsumerGroup,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("failed to inspect pending message: %v", err)
	}

	if len(pending) != 1 {
		t.Fatalf(
			"expected failed message %q to remain pending, got %d pending entries",
			messageID,
			len(pending),
		)
	}
}

func TestProcessor_ClaimsAndProcessesStaleMessage(t *testing.T) {
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
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	t.Cleanup(func() {
		redisClient.Close()
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}

	if err := redisClient.Del(ctx, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("failed to clean Redis stream: %v", err)
	}

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	payload := []byte(`{"type":"email","message":"recovered"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)

	callbackURL := server.URL
	idempotencyKey := "worker-claim-test-" + uuid.NewString()

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	messageID, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
	)
	if err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	deadConsumer := "worker-dead-" + uuid.NewString()

	// Worker A receives the message but never acknowledges it.
	messages, err := stream.ReadNext(
		ctx,
		redisClient,
		deadConsumer,
	)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Wait until the message satisfies the stale threshold.
	time.Sleep(10 * time.Millisecond)

	liveConsumer := "worker-live-" + uuid.NewString()

	claimed, err := stream.Claim(
		ctx,
		redisClient,
		liveConsumer,
		5*time.Millisecond,
		messageID,
	)
	if err != nil {
		t.Fatalf("failed to claim stale message: %v", err)
	}

	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed message, got %d", len(claimed))
	}

	if claimed[0].ID != messageID {
		t.Fatalf(
			"claimed message ID = %q, want %q",
			claimed[0].ID,
			messageID,
		)
	}

	if claimed[0].JobID != jobID.String() {
		t.Fatalf(
			"claimed job ID = %q, want %q",
			claimed[0].JobID,
			jobID.String(),
		)
	}

	processor := NewProcessor(
		jobRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	if err := processor.Process(ctx, claimed[0]); err != nil {
		t.Fatalf("failed to process claimed message: %v", err)
	}

	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to retrieve processed job: %v", err)
	}

	if job.Status != repository.StatusDone {
		t.Fatalf(
			"job status = %q, want %q",
			job.Status,
			repository.StatusDone,
		)
	}

	pending, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.ReadyStream,
		Group:  stream.ConsumerGroup,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("failed to inspect pending message: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf(
			"expected claimed message %q to be acknowledged",
			messageID,
		)
	}
}

func TestWorker_EndToEndJobExecution(t *testing.T) {
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
		t.Fatalf("failed to create database pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	t.Cleanup(func() {
		redisClient.Close()
	})

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}

	if err := redisClient.Del(ctx, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("failed to clean Redis stream: %v", err)
	}

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	payload := []byte(`{"type":"email","message":"end-to-end"}`)

	callbackCalled := make(chan []byte, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read callback body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		callbackCalled <- body

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)

	callbackURL := server.URL
	idempotencyKey := "worker-e2e-" + uuid.NewString()

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: idempotencyKey,
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), payload); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	processor := NewProcessor(jobRepo, redisClient, executor.NewHTTPExecutor())

	worker := NewWorker(redisClient, processor, "worker-e2e-"+uuid.NewString(), slog.Default())

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerDone := make(chan error, 1)

	go func() {
		workerDone <- worker.Run(workerCtx)
	}()

	deadline := time.Now().Add(5 * time.Second)

	for {
		job, err := jobRepo.GetByID(ctx, jobID)
		if err != nil {
			t.Fatalf("failed to retrieve job: %v", err)
		}

		if job.Status == repository.StatusDone {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("job did not reach done status within 5 seconds")
		}

		time.Sleep(10 * time.Millisecond)
	}

	select {
	case received := <-callbackCalled:
		if string(received) != string(payload) {
			t.Fatalf("callback payload = %q, want %q", string(received), string(payload))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("callback was not received")
	}

	cancel()

	select {
	case err := <-workerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}
}
