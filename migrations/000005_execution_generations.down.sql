ALTER TABLE job_executions
  DROP CONSTRAINT IF EXISTS job_executions_job_attempt_unique;

ALTER TABLE job_executions
  ADD CONSTRAINT job_executions_job_attempt_unique
  UNIQUE (job_id, attempt_number);

DROP INDEX IF EXISTS idx_job_executions_job_id_generation;

ALTER TABLE job_executions
  DROP COLUMN IF EXISTS queue_generation;
