-- Scope execution attempts to a queue generation (run). Manual Retry()
-- resets jobs.attempts to 0 and bumps queue_generation, so a retried run
-- must be able to record attempt 1..N again without colliding with the
-- previous run's rows. Existing rows keep generation 1 via the default.

ALTER TABLE job_executions
  ADD COLUMN queue_generation BIGINT NOT NULL DEFAULT 1;

ALTER TABLE job_executions
  DROP CONSTRAINT IF EXISTS job_executions_job_attempt_unique;

ALTER TABLE job_executions
  ADD CONSTRAINT job_executions_job_attempt_unique
  UNIQUE (job_id, queue_generation, attempt_number);

CREATE INDEX IF NOT EXISTS idx_job_executions_job_id_generation
  ON job_executions(job_id, queue_generation);
