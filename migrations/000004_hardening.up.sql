-- Hardening constraints and indexes. Safe to apply on existing data
-- (assumes app already writes valid statuses/attempts).

-- Job status allow-list.
ALTER TABLE jobs
  ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('pending', 'scheduled', 'running', 'done', 'dead', 'cancelled'));

ALTER TABLE jobs
  ADD CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
  ADD CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0 AND max_attempts <= 100);

-- Execution status allow-list.
ALTER TABLE job_executions
  ADD CONSTRAINT job_executions_status_check
  CHECK (status IN ('running', 'done', 'failed'));

-- Lookups used by API + worker.
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON jobs(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at, id);
CREATE INDEX IF NOT EXISTS idx_job_executions_job_id ON job_executions(job_id);
ALTER TABLE job_executions
  ADD CONSTRAINT job_executions_job_attempt_unique UNIQUE (job_id, attempt_number);

-- Exactly-once-per-occurrence guard is enforced in app via 23505 handling,
-- but a unique index makes it race-safe at the DB level.
-- (jobs.idempotency_key already UNIQUE.)

-- API keys: one hashed value must be unique; client names are informational.
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_hashed_key ON api_keys(hashed_key);
CREATE INDEX IF NOT EXISTS idx_api_keys_client_name ON api_keys(client_name);

-- Cron defaults for rows created before defaults existed.
ALTER TABLE cron_jobs ALTER COLUMN enabled SET DEFAULT true;
CREATE INDEX IF NOT EXISTS idx_cron_jobs_enabled_next_run_at
  ON cron_jobs(enabled, next_run_at);
