CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    idempotency_key TEXT UNIQUE NOT NULL,
    callback_url TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_jobs_status_run_at
ON jobs(status , run_at);

CREATE TABLE job_executions (

    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error TEXT,
    response_code INTEGER
);

CREATE TABLE cron_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cron_expression TEXT NOT NULL,
    job_template JSONB NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_cron_jobs_next_run_at
ON cron_jobs(next_run_at);