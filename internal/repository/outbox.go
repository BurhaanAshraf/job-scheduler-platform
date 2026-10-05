package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// Transactional outbox for the DB -> Redis handoff.
//
// Every mutation that must be followed by a Redis enqueue (job submit, cron
// spawn, dead-job retry) inserts an outbox row in the SAME transaction as
// the job row. The normal path deletes the row right after a successful
// enqueue; the scheduler's reconciler re-enqueues anything left past a grace
// period (crash between COMMIT and enqueue). Duplicate enqueues are safe:
// the worker's StartExecution generation guard turns a same-generation
// redelivery into an ack-and-forget.
//
// Steady state is EMPTY. A growing job_outbox means Redis is down or the
// reconciler is not running.

const (
	OutboxImmediate = "immediate"
	OutboxScheduled = "scheduled"
)

type OutboxEntry struct {
	JobID           uuid.UUID
	QueueGeneration int64
	RunAt           time.Time
	Kind            string
	CreatedAt       time.Time
}

// dbTx is satisfied by both *pgxpool.Pool and pgx.Tx, so outbox writes can
// join whatever transaction the caller already holds.
type dbTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func insertOutboxTx(
	ctx context.Context,
	db dbTx,
	jobID uuid.UUID,
	generation int64,
	runAt time.Time,
	kind string,
) error {
	if kind != OutboxImmediate && kind != OutboxScheduled {
		return fmt.Errorf("invalid outbox kind %q", kind)
	}
	if _, err := db.Exec(
		ctx,
		`INSERT INTO job_outbox (job_id, queue_generation, run_at, kind)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (job_id) DO UPDATE SET
		   queue_generation = EXCLUDED.queue_generation,
		   run_at = EXCLUDED.run_at,
		   kind = EXCLUDED.kind,
		   created_at = NOW()`,
		jobID,
		generation,
		runAt,
		kind,
	); err != nil {
		return fmt.Errorf("insert outbox entry for job %q: %w", jobID, err)
	}
	return nil
}

// ClaimOutbox returns up to limit unclaimed entries older than olderThan,
// locking them SKIP LOCKED so concurrent reconcilers never double-claim.
// Rows stay in the table until DeleteOutboxEntries runs after a successful
// enqueue; a crash between enqueue and delete only causes a safe duplicate.
func (r *JobRepository) ClaimOutbox(
	ctx context.Context,
	limit int,
	olderThan time.Time,
) ([]OutboxEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(
		ctx,
		`SELECT job_id, queue_generation, run_at, kind, created_at
		 FROM job_outbox
		 WHERE created_at <= $1
		 ORDER BY created_at ASC
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		olderThan,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select outbox entries: %w", err)
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.JobID, &e.QueueGeneration, &e.RunAt, &e.Kind, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox entries: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return entries, nil
}

// DeleteOutboxEntries removes rows after a successful enqueue. Best-effort
// by design: leftovers are reaped by the reconciler, so callers log and move
// on instead of failing the request.
func (r *JobRepository) DeleteOutboxEntries(ctx context.Context, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if _, err := r.pool.Exec(ctx, `DELETE FROM job_outbox WHERE job_id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("delete outbox entries: %w", err)
	}
	return nil
}
