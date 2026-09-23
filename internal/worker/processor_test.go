package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/retry"
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
			ctx,
			`DELETE FROM job_executions WHERE job_id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean job executions: %v", err)
		}

		_, err = pool.Exec(
			ctx,
			`DELETE FROM jobs WHERE id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean job: %v", err)
		}
	})

	messageID, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
		1,
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
	executionRepo := repository.NewJobExecutionRepository(pool)
	processor := NewProcessor(
		jobRepo,
		executionRepo,
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

func TestProcessor_Process_FailureSchedulesRetry(t *testing.T) {
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

	if err := redisClient.Del(ctx, stream.ReadyStream, stream.ScheduledSet, stream.ScheduledPayloads).Err(); err != nil {
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
			"DELETE FROM job_executions WHERE job_id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job executions: %v", err)
		}

		_, err = pool.Exec(
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
		1,
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
	executionRepo := repository.NewJobExecutionRepository(pool)
	processor := NewProcessor(
		jobRepo,
		executionRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	beforeProcess := time.Now().UTC()

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

	if len(pending) != 0 {
		t.Fatalf(
			"expected failed message %q to be acknowledged after retry scheduling, got %d pending entries",
			messageID,
			len(pending),
		)
	}

	scheduledScore, err := redisClient.ZScore(
		ctx,
		stream.ScheduledSet,
		jobID.String(),
	).Result()
	if err != nil {
		t.Fatalf("expected job in scheduled set: %v", err)
	}

	expectedRunAt := beforeProcess.Add(retry.Backoff(1))
	actualRunAt := time.Unix(int64(scheduledScore), 0)

	if actualRunAt.Before(expectedRunAt.Add(-1*time.Second)) ||
		actualRunAt.After(expectedRunAt.Add(1*time.Second)) {
		t.Fatalf(
			"unexpected retry time: got %v, expected around %v",
			actualRunAt,
			expectedRunAt,
		)
	}

	readyMessages, err := redisClient.XRange(
		ctx,
		stream.ReadyStream,
		"-",
		"+",
	).Result()
	if err != nil {
		t.Fatalf("failed to inspect ready stream: %v", err)
	}

	for _, readyMessage := range readyMessages {
		if readyMessage.ID == messageID {
			continue // original message
		}

		if readyMessage.Values["job_id"] == jobID.String() {
			t.Fatalf("failed job was immediately re-enqueued in ready stream")
		}
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
			"DELETE FROM job_executions WHERE job_id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job executions: %v", err)
		}

		_, err = pool.Exec(
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
		1,
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
	executionRepo := repository.NewJobExecutionRepository(pool)
	processor := NewProcessor(
		jobRepo,
		executionRepo,
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
			"DELETE FROM job_executions WHERE job_id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job executions: %v", err)
		}

		_, err = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	if _, err := stream.EnqueueDue(ctx, redisClient, jobID.String(), payload, 1); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}
	executionRepo := repository.NewJobExecutionRepository(pool)
	processor := NewProcessor(jobRepo, executionRepo, redisClient, executor.NewHTTPExecutor())

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

func TestProcessor_Process_ExhaustedJobMovesToDeadLetter(t *testing.T) {
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

	if err := redisClient.Del(
		ctx,
		stream.ReadyStream,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
		stream.DeadLetterStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	payload := []byte(`{"type":"email","message":"always fails"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)

	callbackURL := server.URL

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: "worker-dlq-test-" + uuid.NewString(),
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			"DELETE FROM job_executions WHERE job_id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job executions: %v", err)
		}

		_, err = pool.Exec(
			context.Background(),
			"DELETE FROM jobs WHERE id = $1",
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	executionRepo := repository.NewJobExecutionRepository(pool)

	processor := NewProcessor(
		jobRepo,
		executionRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	var finalMessageID string

	for attempt := 1; attempt <= 3; attempt++ {
		job, err := jobRepo.GetByID(ctx, jobID)
		if err != nil {
			t.Fatalf(
				"failed to retrieve job before attempt %d: %v",
				attempt,
				err,
			)
		}

		messageID, err := stream.EnqueueDue(
			ctx,
			redisClient,
			jobID.String(),
			payload,
			job.QueueGeneration,
		)
		if err != nil {
			t.Fatalf(
				"failed to enqueue attempt %d: %v",
				attempt,
				err,
			)
		}

		if attempt == 3 {
			finalMessageID = messageID
		}

		consumer := fmt.Sprintf(
			"worker-dlq-test-%d-%s",
			attempt,
			uuid.NewString(),
		)

		messages, err := stream.ReadNext(
			ctx,
			redisClient,
			consumer,
		)
		if err != nil {
			t.Fatalf(
				"failed to read attempt %d: %v",
				attempt,
				err,
			)
		}

		if len(messages) != 1 {
			t.Fatalf(
				"expected 1 message for attempt %d, got %d",
				attempt,
				len(messages),
			)
		}

		err = processor.Process(ctx, messages[0])
		if err == nil {
			t.Fatalf(
				"expected attempt %d to fail",
				attempt,
			)
		}

		// Attempts 1 and 2 schedule a retry.
		// Remove the scheduled Redis entry so the next loop
		// can manually simulate the scheduler promotion.
		if attempt < 3 {
			if err := redisClient.ZRem(
				ctx,
				stream.ScheduledSet,
				jobID.String(),
			).Err(); err != nil {
				t.Fatalf(
					"failed to remove simulated retry after attempt %d: %v",
					attempt,
					err,
				)
			}

			if err := redisClient.HDel(
				ctx,
				stream.ScheduledPayloads,
				jobID.String(),
			).Err(); err != nil {
				t.Fatalf(
					"failed to remove scheduled payload after attempt %d: %v",
					attempt,
					err,
				)
			}
		}
	}

	// Final database state.
	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to retrieve exhausted job: %v", err)
	}

	if job.Attempts != 3 {
		t.Fatalf(
			"expected exactly 3 attempts, got %d",
			job.Attempts,
		)
	}

	if job.Status != repository.StatusDead {
		t.Fatalf(
			"expected job status %q, got %q",
			repository.StatusDead,
			job.Status,
		)
	}

	if job.LastError == nil || *job.LastError == "" {
		t.Fatal("expected last_error to be recorded")
	}

	// Exactly three execution rows.
	var executionCount int

	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*)
		 FROM job_executions
		 WHERE job_id = $1`,
		jobID,
	).Scan(&executionCount)
	if err != nil {
		t.Fatalf(
			"failed to count job executions: %v",
			err,
		)
	}

	if executionCount != 3 {
		t.Fatalf(
			"expected exactly 3 execution rows, got %d",
			executionCount,
		)
	}

	// Execution attempts must be exactly 1, 2, 3.
	rows, err := pool.Query(
		ctx,
		`SELECT attempt_number
		 FROM job_executions
		 WHERE job_id = $1
		 ORDER BY attempt_number ASC`,
		jobID,
	)
	if err != nil {
		t.Fatalf("failed to query execution attempts: %v", err)
	}
	defer rows.Close()

	expectedAttempt := 1

	for rows.Next() {
		var attemptNumber int

		if err := rows.Scan(&attemptNumber); err != nil {
			t.Fatalf("failed to scan execution attempt: %v", err)
		}

		if attemptNumber != expectedAttempt {
			t.Fatalf(
				"expected execution attempt %d, got %d",
				expectedAttempt,
				attemptNumber,
			)
		}

		expectedAttempt++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("failed while reading execution attempts: %v", err)
	}

	if expectedAttempt != 4 {
		t.Fatalf(
			"expected execution attempts 1, 2, and 3",
		)
	}

	// Exactly one dead-letter entry.
	deadLetters, err := redisClient.XRange(
		ctx,
		stream.DeadLetterStream,
		"-",
		"+",
	).Result()
	if err != nil {
		t.Fatalf(
			"failed to inspect dead-letter stream: %v",
			err,
		)
	}

	var deadLetterCount int

	for _, message := range deadLetters {
		if message.Values["job_id"] == jobID.String() {
			deadLetterCount++
		}
	}

	if deadLetterCount != 1 {
		t.Fatalf(
			"expected exactly 1 dead-letter entry for job %q, got %d",
			jobID,
			deadLetterCount,
		)
	}

	// No scheduled retry remains.
	_, err = redisClient.ZScore(
		ctx,
		stream.ScheduledSet,
		jobID.String(),
	).Result()

	if !errors.Is(err, redis.Nil) {
		t.Fatalf(
			"expected exhausted job to have no scheduled retry, got err=%v",
			err,
		)
	}

	// Final Redis message was acknowledged.
	pending, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream.ReadyStream,
		Group:  stream.ConsumerGroup,
		Start:  finalMessageID,
		End:    finalMessageID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf(
			"failed to inspect final pending message: %v",
			err,
		)
	}

	if len(pending) != 0 {
		t.Fatalf(
			"expected exhausted message %q to be acknowledged, got %d pending entries",
			finalMessageID,
			len(pending),
		)
	}
}

func TestWorker_AttemptIncrementSurvivesCrash(t *testing.T) {
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

	if err := redisClient.Del(
		ctx,
		stream.ReadyStream,
		stream.ScheduledSet,
		stream.ScheduledPayloads,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	if err := stream.EnsureConsumerGroup(ctx, redisClient); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}
	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(callbackStarted)

		<-callbackRelease

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)

	callbackURL := server.URL

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{"message":"crash-test"}`),
		RunAt:          time.Now().UTC().Add(-time.Second),
		MaxAttempts:    3,
		IdempotencyKey: "worker-crash-" + uuid.NewString(),
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			context.Background(),
			`DELETE FROM job_executions WHERE job_id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job executions: %v", err)
		}

		_, err = pool.Exec(
			context.Background(),
			`DELETE FROM jobs WHERE id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean up test job: %v", err)
		}
	})

	payload := []byte(`{"message":"crash-test"}`)

	if _, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
		1,
	); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	workerBinary := filepath.Join(t.TempDir(), "worker")

	projectRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	projectRoot = filepath.Join(projectRoot, "..", "..")

	build := exec.Command(
		"go",
		"build",
		"-o",
		workerBinary,
		"./cmd/worker",
	)
	build.Dir = projectRoot

	var buildOutput bytes.Buffer
	build.Stdout = &buildOutput
	build.Stderr = &buildOutput

	if err := build.Run(); err != nil {
		t.Fatalf(
			"failed to build worker: %v\n%s",
			err,
			buildOutput.String(),
		)
	}

	workerCmd := exec.Command(workerBinary)
	workerCmd.Env = append(
		os.Environ(),
		"JOB_SCHEDULER_DB_DSN="+dsn,
		"REDIS_ADDR="+redisAddr,
	)

	if err := workerCmd.Start(); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	workerKilled := false

	defer func() {
		if !workerKilled && workerCmd.Process != nil {
			_ = workerCmd.Process.Kill()
			_ = workerCmd.Wait()
		}
	}()

	select {
	case <-callbackStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never reached callback")
	}

	var attempts int

	if err := pool.QueryRow(
		ctx,
		`SELECT attempts FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&attempts); err != nil {
		t.Fatalf("failed to read attempt count: %v", err)
	}

	if attempts != 1 {
		t.Fatalf(
			"expected attempts=1 while callback was executing, got %d",
			attempts,
		)
	}

	if err := workerCmd.Process.Kill(); err != nil {
		t.Fatalf("failed to kill worker: %v", err)
	}

	_ = workerCmd.Wait()
	workerKilled = true

	close(callbackRelease)

	var attemptsAfterRestart int

	if err := pool.QueryRow(
		ctx,
		`SELECT attempts FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&attemptsAfterRestart); err != nil {
		t.Fatalf(
			"failed to read attempt count after worker crash: %v",
			err,
		)
	}

	if attemptsAfterRestart != 1 {
		t.Fatalf(
			"expected attempts=1 after worker crash, got %d",
			attemptsAfterRestart,
		)
	}
}

func TestProcessor_Process_StaleMessageIsAcknowledgedWithoutExecution(t *testing.T) {
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

	payload := []byte(`{"message":"stale-message-test"}`)

	callbackCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobRepo := repository.NewJobRepository(pool)
	executionRepo := repository.NewJobExecutionRepository(pool)

	callbackURL := server.URL

	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "test",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: "stale-message-" + uuid.NewString(),
		CallbackURL:    &callbackURL,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	t.Cleanup(func() {
		_, err := pool.Exec(
			ctx,
			`DELETE FROM job_executions WHERE job_id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean job executions: %v", err)
		}

		_, err = pool.Exec(
			ctx,
			`DELETE FROM jobs WHERE id = $1`,
			jobID,
		)
		if err != nil {
			t.Errorf("failed to clean job: %v", err)
		}
	})

	firstMessageID, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
		1,
	)
	if err != nil {
		t.Fatalf("failed to enqueue first message: %v", err)
	}

	firstMessages, err := stream.ReadNext(
		ctx,
		redisClient,
		"stale-test-worker",
	)
	if err != nil {
		t.Fatalf("failed to read first message: %v", err)
	}

	if len(firstMessages) != 1 {
		t.Fatalf("expected 1 first message, got %d", len(firstMessages))
	}

	processor := NewProcessor(
		jobRepo,
		executionRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	if err := processor.Process(ctx, firstMessages[0]); err != nil {
		t.Fatalf("first process failed: %v", err)
	}

	if callbackCount != 1 {
		t.Fatalf("expected callback count=1 after first execution, got %d", callbackCount)
	}

	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job after first execution: %v", err)
	}

	if job.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", job.Attempts)
	}

	if job.Status != repository.StatusDone {
		t.Fatalf(
			"expected status=%q, got %q",
			repository.StatusDone,
			job.Status,
		)
	}

	// Create a second Redis message for the same job to simulate
	// a stale/duplicate delivery.
	secondMessageID, err := stream.EnqueueDue(
		ctx,
		redisClient,
		jobID.String(),
		payload,
		1,
	)
	if err != nil {
		t.Fatalf("failed to enqueue stale message: %v", err)
	}

	secondMessages, err := stream.ReadNext(
		ctx,
		redisClient,
		"stale-test-worker-2",
	)
	if err != nil {
		t.Fatalf("failed to read stale message: %v", err)
	}

	if len(secondMessages) != 1 {
		t.Fatalf("expected 1 stale message, got %d", len(secondMessages))
	}

	if secondMessages[0].ID != secondMessageID {
		t.Fatalf(
			"expected stale message ID %q, got %q",
			secondMessageID,
			secondMessages[0].ID,
		)
	}

	if err := processor.Process(ctx, secondMessages[0]); err != nil {
		t.Fatalf("processing stale message failed: %v", err)
	}

	// The stale message must not execute the callback again.
	if callbackCount != 1 {
		t.Fatalf(
			"expected callback count to remain 1, got %d",
			callbackCount,
		)
	}

	job, err = jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get final job: %v", err)
	}

	if job.Attempts != 1 {
		t.Fatalf(
			"expected attempts to remain 1, got %d",
			job.Attempts,
		)
	}

	if job.Status != repository.StatusDone {
		t.Fatalf(
			"expected final status=%q, got %q",
			repository.StatusDone,
			job.Status,
		)
	}

	var executionCount int
	err = pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM job_executions WHERE job_id = $1`,
		jobID,
	).Scan(&executionCount)
	if err != nil {
		t.Fatalf("failed to count executions: %v", err)
	}

	if executionCount != 1 {
		t.Fatalf(
			"expected exactly 1 execution row, got %d",
			executionCount,
		)
	}

	pending, err := redisClient.XPendingExt(
		ctx,
		&redis.XPendingExtArgs{
			Stream: stream.ReadyStream,
			Group:  stream.ConsumerGroup,
			Start:  "-",
			End:    "+",
			Count:  100,
		},
	).Result()
	if err != nil {
		t.Fatalf("failed to inspect pending messages: %v", err)
	}

	for _, entry := range pending {
		if entry.ID == firstMessageID || entry.ID == secondMessageID {
			t.Fatalf("message %q remains pending", entry.ID)
		}
	}
}
