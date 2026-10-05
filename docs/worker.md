# Worker Service — In Depth

The `worker` binary (`cmd/worker/main.go`, logic in `internal/worker/`) is
the execution engine: it owns the Redis consumer group, claims stream
messages, POSTs payloads to tenant callbacks, and records every attempt. Any
number of workers run concurrently with no coordination — Redis consumer
groups guarantee each message is delivered to exactly one consumer at a time.

## Main loop (`internal/worker/worker.go`)

1. **Startup**: `stream.EnsureConsumerGroup` creates group `workers` on stream
   `jobs:ready` with ID `0` (recreates redeliver history — group loss never
   silently drops jobs), ignoring `BUSYGROUP` on restart.
2. **Blocking read**: `XREADGROUP GROUP workers <consumer> COUNT 10 BLOCK 2s`
   on its own goroutine; consumer name is `hostname-uuid`. Socket
   `ReadTimeout` is 10 s, comfortably above the 2 s block, so long polls
   never surface as `i/o timeout` errors.
3. **Process** each message via `Processor.Process` (`internal/worker/processor.go`);
   any error is logged with `job_id` and the loop continues — one bad job
   never crashes the worker.
4. **Reclaim tick** (every 30 s): `ListStalePending` finds PEL entries idle
   > 60 s (crashed/hung workers) and `XCLAIM`s up to 10 per tick to this
   consumer for redrive.
5. **Group recovery**: a `NOGROUP` read error recreates the consumer group in
   place instead of error-looping until restart (covers key eviction,
   `FLUSHDB`, Redis restore/failover).
6. Graceful shutdown on `SIGINT`/`SIGTERM` via context cancellation.

## Processing one message (`Processor.Process`)

Ordered for crash safety — every state change is durable **before** the
irreversible step:

1. Parse `job_id`, `payload`, `queue_generation` from the stream fields.
   Unparseable / job deleted from Postgres / missing callback → return
   without ACK (message stays in PEL for reclaim; known poison limitation,
   see below).
2. `StartExecution`: **atomically** `attempts = attempts+1, status='running'`
   guarded by `WHERE queue_generation = $n AND status IN
   ('pending','scheduled')`. The increment happens *before* the HTTP call, so
   even a crash mid-callback is reflected in the count. Returns 0 rows when
   the message is stale (already superseded by cancel/retry/cron respawn) →
   ACK-and-forget without executing.
3. Stale guard: if `job.Attempts >= job.MaxAttempts` (e.g. redelivered after
   DLQ), move straight to dead-letter without executing.
4. `job_executions` row `Create{attempt_number, queue_generation,
   started_at, status='running'}` — the audit trail, independent of the
   `jobs` row.
5. `executor.ExecuteJob`: SSRF-validated POST (see [security](security.md)):
   10 s total timeout, `Idempotency-Key: <jobID>:<attempt>` +
   `X-Job-ID`/`X-Job-Attempt` headers, same-host 307/308 redirects only,
   responses over 1 MiB are failures, not truncations.
6. **Success**: `Complete(done, code)` → `UpdateStatus(done)` → metric
   `jobs_completed_total` → `XACK`.
7. **Failure, attempts left**: compute `nextRunAt =
   retry.NextRunAt(now, attempt)` (full-jitter backoff, 1 s → cap 15 min);
   `ScheduleJob` the payload into `jobs:scheduled` with generation+1, then
   `ScheduleRetry` in Postgres (sets `run_at`, `last_error`, generation+1);
   if the DB step fails, compensate by removing the Redis entry so it can
   never promote stale; then `XACK` and return the error for logging.
8. **Failure, exhausted**: `DeadLetter` (`XADD` to `jobs:dead` stream),
   `UpdateStatus(dead, last_error)`, metric `jobs_failed_total`, `XACK`.

## Retry math (`internal/retry/retry.go`)

`Backoff(n) = 1s * 2^n` capped at 15 min (pure, table-tested 0–6 + cap).
Production uses `BackoffWithJitter`: attempt ≤ 1 → exactly 1 s; otherwise
uniform in `[Backoff(n-1)/2, Backoff(n-1)]` — full jitter so a mass failure
doesn't thundering-herd the callbacks. With `max_attempts=3`, a job executes
at most 3 times and produces exactly 3 `job_executions` rows before going
`dead`.

## Failure semantics

- **At-least-once delivery**: any return without `XACK` leaves the PEL entry
  for reclaim; combined with idempotency keys, redelivery is safe.
- **Exactly-once effects** are the *tenant's* job (keyed by the
  `Idempotency-Key` header), not the platform's claim.
- **Poison messages** (bad stream fields, deleted job rows) are reclaimed
  forever without DLQ — the one known gap; delivery-count-based quarantine
  is the documented next step.
- ** DLQ is explicit**: nothing retries forever; a human (or automation)
  calls `POST /v1/jobs/{id}/retry`, which resets attempts to 0 and bumps the
  generation so in-flight redeliveries of the old generation go stale.

## Scaling

Workers are horizontally scalable (`docker compose up --scale worker=N`, ECS
2–6 on CPU). Throughput scales linearly until Postgres (`DB_MAX_CONNS` per
service) or the callback endpoints saturate. The reclaim batch (10 per 30 s)
bounds recovery speed after a mass crash — size it up if you run dozens of
workers.
