package worker

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestReclaimStaleEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Skip("JOB_SCHEDULER_DB_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	r := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = r.Close() }()
	if err := r.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}

	p := NewProcessor(
		repository.NewJobRepository(pool),
		repository.NewJobExecutionRepository(pool),
		r,
		executor.NewHTTPExecutor(),
		validator.Config{},
	)
	w := NewWorker(r, p, "reclaim-test", slog.Default())
	// No stale messages: returns quietly.
	w.reclaimStale(ctx)
}

func TestReclaimStaleListError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Skip("JOB_SCHEDULER_DB_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	// Unreachable Redis: ListStalePending fails, reclaim logs and returns.
	bad := redis.NewClient(&redis.Options{Addr: "localhost:1"})
	defer func() { _ = bad.Close() }()

	p := NewProcessor(
		repository.NewJobRepository(pool),
		repository.NewJobExecutionRepository(pool),
		bad,
		executor.NewHTTPExecutor(),
		validator.Config{},
	)
	w := NewWorker(bad, p, "reclaim-test", slog.Default())
	w.reclaimStale(ctx)
}
