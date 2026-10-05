# Scheduler Service — In Depth

The `scheduler` binary (`cmd/scheduler/main.go`, logic in
`internal/scheduler/`) is the timekeeper: it promotes due delayed jobs and
spawns cron instances. Exactly one scheduler acts at a time (Redis leader
lock); the rest stand by. It serves no traffic and exposes no ports.

## Tick loop (`internal/scheduler/scheduler.go`)

Every `SCHEDULER_POLL_INTERVAL` (default 500 ms), **if leader**:

1. `PromoteDueWithIDs(now)` — runs the Lua script
   (`internal/stream/schedule.lua.go`) that atomically moves every entry with
   score ≤ `now` from the `jobs:scheduled` sorted set into the `jobs:ready`
   stream (max 500 per tick; corrupt payloads and orphaned members are
   quarantined, never head-of-line-blocking). Returns promoted IDs for
   per-job `job promoted` log lines.
2. `TickCronJobs(now)` — `ListDue` (`enabled AND next_run_at <= now`, indexed)
   then `CreateDueInstance` per cron row: `SELECT … FOR UPDATE` →
   re-check due+enabled → `INSERT` the job instance (idempotency key
   `cron:<id>:<RFC3339Nano occurrence>`) → `UPDATE next_run_at =
   nextRun(occurrence)` (fast-forwards past `now` after downtime) → `COMMIT`.
   Duplicate key (`23505`) means another scheduler already spawned this tick
   → skip. Then `EnqueueDue` the instance to the stream.
3. `ReconcileOutbox` (every 30 s) — claims `job_outbox` rows past the
   grace period and finishes interrupted DB→Redis handoffs (stale rows for
   gone/terminal/superseded jobs are deleted, not dispatched).
4. `QueueBacklog` → `queue depth` log line (consumer-group lag + pending;
   `XLEN` fallback before the group exists).

Standbys do nothing but attempt lock acquisition — no error spam, no dispatch.

## Cron (`internal/scheduler/cron.go`, `internal/repository/cron_jobs.go`)

- Expressions are standard 5-field (`robfig/cron` `ParseStandard`); `nextRunAt`
  is computed at create time and advanced inside the same transaction that
  spawns the instance, so a crash between spawn and advance cannot double-fire
  (the unique idempotency key is the second line of defense).
- `PATCH …/cron-jobs/{id} {"enabled":false}` stops future spawns without
  deleting history; the tick re-checks `enabled` under row lock.
- Poison templates (bad payload that fails tick-time validation) roll the
  transaction back without advancing — they retry every tick and log loudly
  rather than silently skipping. Fix the template or disable the cron.
- Determinism is unit-tested with simulated ticks (`every minute → exactly 3
  instances`), not wall-clock sleeps.

## Leader election (`internal/scheduler/leader.go`)

- **Acquire**: `SET scheduler:leader <instance-id> NX PX <30 s>`. Second
  contender gets `false`.
- **Heartbeat**: goroutine renews at TTL/2 via check-and-extend Lua (`GET ==
  id → PEXPIRE`), so only the owner can extend. A failed renewal logs `lost
  scheduler leadership` and the loops idle until re-acquired.
- **Release**: check-and-delete Lua (`GET == id → DEL`) — instance A can never
  delete instance B's lock after its own TTL expired.
- **Handoff**: standby acquires within ~TTL of a leader crash (unit-tested at
  small TTLs; production TTL is 30 s, so worst-case dispatch pause is
  ~30.5 s). Promotion Lua and cron unique keys make a brief split-brain
  overlap safe (no double-promote, no duplicate instances).
- **Identity**: `SCHEDULER_INSTANCE_ID`, defaulting to `hostname-uuid` — every
  replica must have a distinct ID. Never set a static shared value (compose
  and Terraform both leave it unset for exactly this reason).
- Every acquire/release logs instance + timestamp for incident reconstruction.

## Delayed jobs (the sorted-set path)

`POST /v1/jobs` with future `run_at` → `ScheduleJob` Lua (`ZADD` +
`HSET`-payload atomically, so a crash can't leave a member without data).
Sub-second `run_at` is truncated to whole seconds (unix scores) — promotion
may be up to ~1 s early, well within the 500 ms poll granularity contract.
`DELETE` removes the `ZADD` member best-effort; the worker's generation guard
covers the race.

## Known limitations

- Crash between cron `COMMIT` and stream `EnqueueDue` (or API submit/retry
  equivalents) leaves an outbox row that the reconciler re-enqueues within
  ~30 s — closed, not just logged. The remaining micro-window is a lost race
  producing a safe duplicate delivery, never a loss.
- No fencing token: correctness during overlap rests on Lua atomicity + DB
  uniqueness, not on the lock itself. Sufficient at this scale, documented
  rather than hidden.
