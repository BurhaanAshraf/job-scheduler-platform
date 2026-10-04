package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func testCronTemplate(t *testing.T) json.RawMessage {
	t.Helper()
	cb := "https://example.com/hook"
	tpl, _ := json.Marshal(map[string]any{
		"type":         "cron-coverage",
		"payload":      map[string]any{"a": 1},
		"max_attempts": 2,
		"callback_url": cb,
	})
	return tpl
}

func stubNextRun(_ string, from time.Time) (time.Time, error) {
	return from.Add(time.Minute), nil
}

func TestCronCreateAndListDue(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	repo := NewCronJobRepository(pool)
	var cronIDs []int64
	t.Cleanup(func() {
		for _, id := range cronIDs {
			_, _ = pool.Exec(context.Background(), `DELETE FROM jobs WHERE idempotency_key LIKE 'cron:' || $1 || ':%'`, id)
			_, _ = pool.Exec(context.Background(), `DELETE FROM cron_jobs WHERE id = $1`, id)
		}
	})

	due, err := repo.Create(ctx, CreateCronJobInput{
		CronExpression: "*/1 * * * *",
		JobTemplate:    testCronTemplate(t),
		NextRunAt:      time.Now().UTC().Add(-time.Minute),
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cronIDs = append(cronIDs, due.ID)
	if due.ID == 0 || !due.Enabled {
		t.Fatalf("bad created job: %+v", due)
	}

	// Future + disabled jobs are not due.
	future, err := repo.Create(ctx, CreateCronJobInput{
		CronExpression: "*/1 * * * *",
		JobTemplate:    testCronTemplate(t),
		NextRunAt:      time.Now().UTC().Add(time.Hour),
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("Create future: %v", err)
	}
	cronIDs = append(cronIDs, future.ID)
	disabled, err := repo.Create(ctx, CreateCronJobInput{
		CronExpression: "*/1 * * * *",
		JobTemplate:    testCronTemplate(t),
		NextRunAt:      time.Now().UTC().Add(-time.Minute),
		Enabled:        false,
	})
	if err != nil {
		t.Fatalf("Create disabled: %v", err)
	}
	cronIDs = append(cronIDs, disabled.ID)

	listed, err := repo.ListDue(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	seenDue, seenDisabled := false, false
	for _, c := range listed {
		if c.ID == due.ID {
			seenDue = true
		}
		if c.ID == disabled.ID {
			seenDisabled = true
		}
	}
	if !seenDue {
		t.Fatal("due job missing from ListDue")
	}
	if seenDisabled {
		t.Fatal("disabled job must not be listed as due")
	}

	// Disable the due job: disappears + UpdateEnabled path covered.
	if err := repo.UpdateEnabled(ctx, due.ID, false); err != nil {
		t.Fatalf("UpdateEnabled: %v", err)
	}
	if err := repo.UpdateEnabled(ctx, 987654321, true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCreateDueInstance(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	repo := NewCronJobRepository(pool)
	var cronIDs []int64
	t.Cleanup(func() {
		for _, id := range cronIDs {
			_, _ = pool.Exec(context.Background(), `DELETE FROM jobs WHERE idempotency_key LIKE 'cron:' || $1 || ':%'`, id)
			_, _ = pool.Exec(context.Background(), `DELETE FROM cron_jobs WHERE id = $1`, id)
		}
	})

	cron, err := repo.Create(ctx, CreateCronJobInput{
		CronExpression: "*/1 * * * *",
		JobTemplate:    testCronTemplate(t),
		NextRunAt:      time.Now().UTC().Add(-time.Minute),
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cronIDs = append(cronIDs, cron.ID)

	inst, created, err := repo.CreateDueInstance(ctx, cron.ID, time.Now().UTC(), stubNextRun)
	if err != nil {
		t.Fatalf("CreateDueInstance: %v", err)
	}
	if !created {
		t.Fatal("expected instance creation")
	}
	if inst.Type != "cron-coverage" || inst.MaxAttempts != 2 {
		t.Fatalf("bad instance: %+v", inst)
	}

	// Second tick immediately: next_run_at advanced, nothing due.
	if _, created, err := repo.CreateDueInstance(ctx, cron.ID, time.Now().UTC(), stubNextRun); err != nil || created {
		t.Fatalf("expected skip, got created=%v err=%v", created, err)
	}

	// Unknown id: clean skip.
	if _, created, err := repo.CreateDueInstance(ctx, 987654321, time.Now().UTC(), stubNextRun); err != nil || created {
		t.Fatalf("expected skip for unknown id, got created=%v err=%v", created, err)
	}
}

func TestCreateDueInstanceBadTemplate(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	repo := NewCronJobRepository(pool)
	var cronIDs []int64
	t.Cleanup(func() {
		for _, id := range cronIDs {
			_, _ = pool.Exec(context.Background(), `DELETE FROM jobs WHERE idempotency_key LIKE 'cron:' || $1 || ':%'`, id)
			_, _ = pool.Exec(context.Background(), `DELETE FROM cron_jobs WHERE id = $1`, id)
		}
	})

	cron, err := repo.Create(ctx, CreateCronJobInput{
		CronExpression: "*/1 * * * *",
		JobTemplate:    json.RawMessage(`{"type":"","payload":null,"max_attempts":0}`),
		NextRunAt:      time.Now().UTC().Add(-time.Minute),
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cronIDs = append(cronIDs, cron.ID)
	if _, _, err := repo.CreateDueInstance(ctx, cron.ID, time.Now().UTC(), stubNextRun); err == nil {
		t.Fatal("expected template validation error, got nil")
	}
}
