package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
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

func quarantineHarness(t *testing.T) (context.Context, *pgxpool.Pool, *redis.Client) {
	t.Helper()
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Skip("JOB_SCHEDULER_DB_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	r := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = r.Close() })
	if err := r.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}
	if err := r.Del(ctx, stream.ReadyStream, stream.ScheduledSet, stream.ScheduledPayloads).Err(); err != nil {
		t.Fatalf("clean redis: %v", err)
	}
	if err := stream.EnsureConsumerGroup(ctx, r); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	return ctx, pool, r
}

func quarantineProcessor(pool *pgxpool.Pool, r *redis.Client) *Processor {
	return NewProcessor(
		repository.NewJobRepository(pool),
		repository.NewJobExecutionRepository(pool),
		r,
		executor.NewHTTPExecutor(),
		validator.Config{AllowPrivateIPs: true},
	)
}

func cleanupQuarantineJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM job_executions WHERE job_id = $1", jobID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", jobID)
	})
}

func TestProcessor_Quarantine_PoisonsDeadLetter(t *testing.T) {
	ctx, pool, r := quarantineHarness(t)
	jobRepo := repository.NewJobRepository(pool)
	p := quarantineProcessor(pool, r)

	payload := []byte(`{"poison":true}`)
	cb := "http://127.0.0.1:9/hook"
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "poison",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: "quarantine-" + uuid.NewString(),
		CallbackURL:    &cb,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	cleanupQuarantineJob(t, ctx, pool, jobID)

	if _, err := stream.EnqueueDue(ctx, r, jobID.String(), payload, 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	msgs, err := stream.ReadNext(ctx, r, "quarantine-poison")
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: msgs=%d err=%v", len(msgs), err)
	}

	if err := p.Quarantine(ctx, msgs[0], 5); err != nil {
		t.Fatalf("quarantine: %v", err)
	}

	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != repository.StatusDead {
		t.Fatalf("status = %q, want dead", job.Status)
	}
	if job.LastError == nil || !strings.Contains(*job.LastError, "poison") {
		t.Fatalf("last_error = %v, want poison explanation", job.LastError)
	}

	pending, err := stream.ListStalePending(ctx, r, 0, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	for _, pe := range pending {
		if pe.ID == msgs[0].ID {
			t.Fatalf("message %q still pending after quarantine", pe.ID)
		}
	}
}

func TestProcessor_Quarantine_MissingAndUnparseableAck(t *testing.T) {
	ctx, pool, r := quarantineHarness(t)
	p := quarantineProcessor(pool, r)

	// Valid UUID, no such job.
	ghost := uuid.NewString()
	if _, err := stream.EnqueueDue(ctx, r, ghost, []byte(`{}`), 1); err != nil {
		t.Fatalf("enqueue ghost: %v", err)
	}
	// Unparseable job reference.
	if _, err := stream.EnqueueDue(ctx, r, "not-a-job", []byte(`{}`), 1); err != nil {
		t.Fatalf("enqueue bad id: %v", err)
	}
	msgs, err := stream.ReadNextWithTimeout(ctx, r, "quarantine-ghost", 2*time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}

	for _, m := range msgs {
		if err := p.Quarantine(ctx, m, 9); err != nil {
			t.Fatalf("quarantine %q: %v", m.JobID, err)
		}
	}

	pending, err := stream.ListStalePending(ctx, r, 0, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("want 0 pending after dropping ghost messages, got %d", len(pending))
	}
}

func TestReclaimStale_QuarantinesPoison(t *testing.T) {
	ctx, pool, r := quarantineHarness(t)
	jobRepo := repository.NewJobRepository(pool)
	p := quarantineProcessor(pool, r)

	payload := []byte(`{"stuck":true}`)
	cb := "http://127.0.0.1:9/hook"
	jobID, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "stuck",
		Payload:        json.RawMessage(payload),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: "reclaim-poison-" + uuid.NewString(),
		CallbackURL:    &cb,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	cleanupQuarantineJob(t, ctx, pool, jobID)

	msgID, err := stream.EnqueueDue(ctx, r, jobID.String(), payload, 1)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Delivery 1 via a normal read, then 4 claims to reach the trigger of 5.
	if _, err := stream.ReadNext(ctx, r, "poison-victim"); err != nil {
		t.Fatalf("initial read: %v", err)
	}
	for i := 0; i < maxPoisonDeliveries-1; i++ {
		if _, err := r.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream.ReadyStream,
			Group:    stream.ConsumerGroup,
			Consumer: "poison-prober",
			MinIdle:  0,
			Messages: []string{msgID},
		}).Result(); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
	}

	w := NewWorker(r, p, "poison-reclaimer", slog.Default())
	w.staleAfter = 0 // same package: skip the 60s idle wait in tests
	w.reclaimStale(ctx)

	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != repository.StatusDead {
		t.Fatalf("status = %q, want dead after %d deliveries", job.Status, maxPoisonDeliveries)
	}

	pending, err := stream.ListStalePending(ctx, r, 0, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	for _, pe := range pending {
		if pe.ID == msgID {
			t.Fatalf("poison message %q still pending after reclaim", msgID)
		}
	}
}
