package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func outboxJob(t *testing.T, repo *JobRepository, runAt time.Time) uuid.UUID {
	t.Helper()
	cb := "http://example.com/hook"
	id, err := repo.Create(context.Background(), CreateJobInput{
		Type:           "outbox-test",
		Payload:        json.RawMessage(`{"a":1}`),
		RunAt:          runAt,
		MaxAttempts:    3,
		IdempotencyKey: "outbox-" + uuid.NewString(),
		CallbackURL:    &cb,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	t.Cleanup(func() {
		_, _ = repo.pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", id)
	})
	return id
}

func claimAll(t *testing.T, repo *JobRepository) []OutboxEntry {
	t.Helper()
	entries, err := repo.ClaimOutbox(context.Background(), 100, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim outbox: %v", err)
	}
	return entries
}

// findEntry scopes assertions to this test's job: the shared test database
// accumulates outbox rows from other tests (their normal paths never run),
// so global emptiness is never asserted.
func findEntry(entries []OutboxEntry, id uuid.UUID) *OutboxEntry {
	for i := range entries {
		if entries[i].JobID == id {
			return &entries[i]
		}
	}
	return nil
}

func outboxCountFor(t *testing.T, repo *JobRepository, id uuid.UUID) int {
	t.Helper()
	var n int
	err := repo.pool.QueryRow(
		context.Background(),
		`SELECT COUNT(*) FROM job_outbox WHERE job_id = $1`,
		id,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func TestOutbox_CreateWritesImmediateEntry(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	repo := NewJobRepository(pool)

	id := outboxJob(t, repo, time.Now().UTC().Add(-time.Hour))

	e := findEntry(claimAll(t, repo), id)
	if e == nil {
		t.Fatalf("no outbox entry for job %q", id)
	}
	if e.Kind != OutboxImmediate {
		t.Fatalf("kind = %q, want immediate", e.Kind)
	}
	if e.QueueGeneration != 1 {
		t.Fatalf("generation = %d, want 1", e.QueueGeneration)
	}

	if err := repo.DeleteOutboxEntries(context.Background(), id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n := outboxCountFor(t, repo, id); n != 0 {
		t.Fatalf("want 0 outbox rows after delete, got %d", n)
	}
}

func TestOutbox_FutureJobIsScheduledKind(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	repo := NewJobRepository(pool)

	runAt := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	id := outboxJob(t, repo, runAt)

	e := findEntry(claimAll(t, repo), id)
	if e == nil {
		t.Fatalf("no outbox entry for job %q", id)
	}
	if e.Kind != OutboxScheduled {
		t.Fatalf("kind = %q, want scheduled", e.Kind)
	}
}

func TestOutbox_RetryWritesEntryWithBumpedGeneration(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	repo := NewJobRepository(pool)

	id := outboxJob(t, repo, time.Now().UTC().Add(-time.Hour))
	if err := repo.DeleteOutboxEntries(context.Background(), id); err != nil {
		t.Fatalf("clear submit entry: %v", err)
	}
	if err := repo.UpdateStatus(context.Background(), id, StatusDead, nil); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	if err := repo.Retry(context.Background(), id); err != nil {
		t.Fatalf("retry: %v", err)
	}

	e := findEntry(claimAll(t, repo), id)
	if e == nil {
		t.Fatalf("no outbox entry after retry for job %q", id)
	}
	if e.QueueGeneration != 2 {
		t.Fatalf("generation = %d, want 2", e.QueueGeneration)
	}
	if e.Kind != OutboxImmediate {
		t.Fatalf("kind = %q, want immediate", e.Kind)
	}
	job, err := repo.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Attempts != 0 || job.Status != StatusScheduled {
		t.Fatalf("job not reset: attempts=%d status=%q", job.Attempts, job.Status)
	}
}

func TestOutbox_GracePeriodExcludesFreshRows(t *testing.T) {
	pool := testDBPool(t)
	defer pool.Close()
	repo := NewJobRepository(pool)

	outboxJob(t, repo, time.Now().UTC().Add(-time.Hour))

	// A reconciler asking only for rows older than an hour must not see a
	// row written milliseconds ago (lets the normal path win the race).
	entries, err := repo.ClaimOutbox(context.Background(), 100, time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("fresh rows leaked past grace period: %d", len(entries))
	}
}
