-- Transactional outbox for the DB -> Redis handoff.
--
-- Every job mutation that must be followed by a Redis enqueue (API submit,
-- cron instance spawn, dead-job retry) inserts an outbox row in the SAME
-- Postgres transaction as the job row. The normal path deletes the row right
-- after a successful enqueue; the scheduler's reconciler (leader only)
-- re-enqueues anything left unclaimed past a grace period. Duplicate
-- enqueues are safe: the worker's StartExecution generation guard turns a
-- same-generation redelivery into an ack-and-forget.
--
-- Steady state is EMPTY: a growing table means Redis is down or the
-- reconciler is not running -- alert on it, not just queue_depth.

CREATE TABLE job_outbox (
  job_id           UUID        PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  queue_generation BIGINT      NOT NULL DEFAULT 1,
  run_at           TIMESTAMPTZ NOT NULL,
  kind             TEXT        NOT NULL CHECK (kind IN ('immediate', 'scheduled')),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_outbox_created_at
  ON job_outbox(created_at);
