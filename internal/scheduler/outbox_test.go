package scheduler_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func testOutboxHarness(t *testing.T) (context.Context, *redis.Client, *pgxpool.Pool, *scheduler.Scheduler, *repository.JobRepository) {
	t.Helper()
	ctx := context.Background()
	redisClient := testSchedulerRedis(t)
	if err := redisClient.Del(ctx, stream.ScheduledSet, stream.ScheduledPayloads, stream.ReadyStream).Err(); err != nil {
		t.Fatalf("clean redis: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(context.Background(), stream.ScheduledSet, stream.ScheduledPayloads, stream.ReadyStream).Err()
	})

	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Fatal("JOB_SCHEDULER_DB_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)
	lock := scheduler.NewLeaderLock(redisClient, "scheduler:test:outbox", "test-outbox", 10*time.Second)
	s := scheduler.New(redisClient, cronRepo, 100*time.Millisecond, lock, logger.New("scheduler-test"))
	s.SetJobRepository(jobRepo)
	s.SetOutboxGracePeriod(0)
	return ctx, redisClient, pool, s, jobRepo
}

func outboxTestJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobRepo *repository.JobRepository, runAt time.Time) uuid.UUID {
	t.Helper()
	cb := "http://example.com/hook"
	id, err := jobRepo.Create(ctx, repository.CreateJobInput{
		Type:           "outbox-test",
		Payload:        []byte(`{"a":1}`),
		RunAt:          runAt,
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
		CallbackURL:    &cb,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id)
	})
	return id
}

func outboxCountForJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_outbox WHERE job_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

// streamHasJobID scans the ready stream for an entry carrying jobID. It
// ignores other tests' entries sharing the same stream and never consumes.
func streamHasJobID(t *testing.T, ctx context.Context, c *redis.Client, jobID string) bool {
	t.Helper()
	entries, err := c.XRange(ctx, stream.ReadyStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	for _, e := range entries {
		if v, _ := e.Values["job_id"].(string); v == jobID {
			return true
		}
	}
	return false
}

func TestReconcileOutbox_CrashRecovery(t *testing.T) {
	ctx, redisClient, pool, s, jobRepo := testOutboxHarness(t)

	jobID := outboxTestJob(t, ctx, pool, jobRepo, time.Now().UTC().Add(-time.Hour))

	// Simulate the crash: DB committed (outbox row written), Redis enqueue
	// never happened. The reconciler must finish the handoff. It may also
	// reap other tests' leftover rows from the shared database, so assert
	// on this test's job rather than exact counts.
	n, err := s.ReconcileOutbox(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n < 1 {
		t.Fatalf("reconciled = %d, want >= 1", n)
	}

	if !streamHasJobID(t, ctx, redisClient, jobID.String()) {
		t.Fatalf("reconciled job %q not found in ready stream", jobID)
	}
	if left := outboxCountForJob(t, ctx, pool, jobID); left != 0 {
		t.Fatalf("want 0 outbox rows, got %d", left)
	}

	// Second pass is a no-op: nothing left to claim.
	if n, err := s.ReconcileOutbox(ctx); err != nil || n != 0 {
		t.Fatalf("second reconcile: want (0, nil), got (%d, %v)", n, err)
	}
}

func TestReconcileOutbox_SkipsTerminalJob(t *testing.T) {
	ctx, redisClient, pool, s, jobRepo := testOutboxHarness(t)

	jobID := outboxTestJob(t, ctx, pool, jobRepo, time.Now().UTC().Add(-time.Hour))
	if err := jobRepo.Cancel(ctx, jobID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	n, err := s.ReconcileOutbox(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n != 0 {
		t.Fatalf("reconciled = %d, want 0 for cancelled job", n)
	}
	if left := outboxCountForJob(t, ctx, pool, jobID); left != 0 {
		t.Fatalf("stale outbox row not cleaned, %d left", left)
	}
	if streamHasJobID(t, ctx, redisClient, jobID.String()) {
		t.Fatalf("cancelled job %q must not dispatch", jobID)
	}
}

func TestReconcileOutbox_ScheduledKindUsesDelayedQueue(t *testing.T) {
	ctx, redisClient, pool, s, jobRepo := testOutboxHarness(t)

	jobID := outboxTestJob(t, ctx, pool, jobRepo, time.Now().UTC().Add(time.Hour))

	n, err := s.ReconcileOutbox(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n != 1 {
		t.Fatalf("reconciled = %d, want 1", n)
	}

	score, err := redisClient.ZScore(ctx, stream.ScheduledSet, jobID.String()).Result()
	if err != nil {
		t.Fatalf("future job must land in the delayed set: %v", err)
	}
	if score <= float64(time.Now().UTC().Unix()) {
		t.Fatalf("delayed score %v not in the future", score)
	}
	if left := outboxCountForJob(t, ctx, pool, jobID); left != 0 {
		t.Fatalf("want 0 outbox rows, got %d", left)
	}
}

func TestReconcileOutbox_NilRepoIsNoop(t *testing.T) {
	ctx, redisClient, _, _, _ := testOutboxHarness(t)
	lock := scheduler.NewLeaderLock(redisClient, "scheduler:test:outbox-nil", "test-nil", 10*time.Second)
	s := scheduler.New(redisClient, nil, 100*time.Millisecond, lock, logger.New("scheduler-test"))
	if n, err := s.ReconcileOutbox(ctx); err != nil || n != 0 {
		t.Fatalf("nil repo: want (0, nil), got (%d, %v)", n, err)
	}
}
