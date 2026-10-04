package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var createdJobIDs []string

func mustCreateJob(t *testing.T, repo *JobRepository, key string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	id, err := repo.Create(ctx, CreateJobInput{
		Type:           "coverage",
		Payload:        json.RawMessage(`{"a":1}`),
		RunAt:          time.Now().UTC().Add(time.Hour),
		MaxAttempts:    3,
		IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	createdJobIDs = append(createdJobIDs, id.String())
	return id
}

func TestMain(m *testing.M) {
	code := m.Run()
	// Best-effort residue cleanup so the shared local DB stays quiet.
	if dsn := os.Getenv("JOB_SCHEDULER_DB_DSN"); dsn != "" && len(createdJobIDs) > 0 {
		ctx := context.Background()
		if pool, err := pgxpool.New(ctx, dsn); err == nil {
			for _, id := range createdJobIDs {
				_, _ = pool.Exec(ctx, `DELETE FROM job_executions WHERE job_id = $1`, id)
				_, _ = pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1`, id)
			}
			pool.Close()
		}
	}
	os.Exit(code)
}

func TestConflictError(t *testing.T) {
	e := &ConflictError{Message: "boom"}
	if e.Error() == "" {
		t.Fatal("empty Error()")
	}
	if !errors.Is(e, ErrConflict) {
		t.Fatal("ConflictError should match ErrConflict via Is")
	}
	if errors.Is(e, ErrNotFound) {
		t.Fatal("ConflictError must not match ErrNotFound")
	}
}

func TestList(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(testPool(t))
	mustCreateJob(t, repo, "list-"+uuid.NewString())

	got, err := repo.List(ctx, 10, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected at least one job")
	}
}

func TestGetByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(testPool(t))
	key := "idem-" + uuid.NewString()
	id := mustCreateJob(t, repo, key)

	got, err := repo.GetByIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if got.ID != id {
		t.Fatalf("id mismatch: %v vs %v", got.ID, id)
	}
	if _, err := repo.GetByIdempotencyKey(ctx, "no-such-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCancel(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(testPool(t))

	id := mustCreateJob(t, repo, "cancel-"+uuid.NewString())
	if err := repo.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel scheduled: %v", err)
	}
	// Second cancel: no longer cancellable.
	if err := repo.Cancel(ctx, id); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("expected ErrNotCancellable, got %v", err)
	}
	if err := repo.Cancel(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestRetry(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(testPool(t))

	// Retry on a live (non-dead) job: status error, not NotFound.
	id := mustCreateJob(t, repo, "retry-"+uuid.NewString())
	err := repo.Retry(ctx, id)
	if err == nil || !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("expected ErrNotCancellable, got %v", err)
	}
	if err := repo.Retry(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Dead job retries cleanly.
	deadID := mustCreateJob(t, repo, "retry-dead-"+uuid.NewString())
	msg := "boom"
	if err := repo.UpdateStatus(ctx, deadID, StatusDead, &msg); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if err := repo.Retry(ctx, deadID); err != nil {
		t.Fatalf("Retry dead: %v", err)
	}
}

func TestScheduleRetry(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(testPool(t))

	id := mustCreateJob(t, repo, "schedretry-"+uuid.NewString())
	msg := "try again"
	// Scheduled (not running): no row matches -> ErrNotFound.
	if _, err := repo.ScheduleRetry(ctx, id, time.Now().UTC().Add(time.Minute), &msg); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Running job reschedules and bumps generation.
	if err := repo.UpdateStatus(ctx, id, StatusRunning, nil); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	gen, err := repo.ScheduleRetry(ctx, id, time.Now().UTC().Add(time.Minute), &msg)
	if err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}
	if gen < 2 {
		t.Fatalf("expected bumped generation, got %d", gen)
	}
}
