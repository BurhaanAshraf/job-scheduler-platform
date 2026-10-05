# Queue Core: Redis Streams, Retries & Dead Letters

Redis is the live dispatch plane; Postgres is the system of record. Nothing
is *true* until it is in Postgres; Redis entries are hints that make
dispatch fast. All key patterns are documented in `docs/redis-keys.md`.

## Keys and structures

| Key | Type | Purpose |
|---|---|---|
| `jobs:ready` | Stream | live dispatch; consumed by group `workers` |
| `jobs:scheduled` | Sorted set (score = unix `run_at`) | delayed jobs + backoff retries |
| `jobs:scheduled:data` | Hash (`job_id` → JSON) | payloads for scheduled members |
| `jobs:dead` | Stream | dead-letter *notifications* (`job_id` + payload) |
| `metrics:*` | Strings (counters) | `jobs_submitted/completed/failed_total` |
| `rate_limit:<client>` | Sorted set | sliding-window rate buckets (60/min) |
| `scheduler:leader` | String (`NX PX`) | leader-election lock |

Stream entries carry `job_id`, `payload`, `queue_generation`. `MAXLEN ~
100000` bounds memory — if producers outpace workers with zero consumers,
the oldest *unacked* entries trim. Operate workers before load, and alert on
`queue_depth`.

Every DB→Redis handoff (submit, cron spawn, dead-job retry) is covered by a
transactional outbox (`job_outbox`, written in the same transaction as the
job row). The normal path deletes its row right after enqueueing; the
leader scheduler's reconciler re-enqueues leftovers past a 30 s grace period
(duplicate delivery is safe via the worker generation guard). Steady state
is an empty table — alert on growth, not just `queue_depth`.

## Consumer groups, the right way

- Group `workers` is created with ID **`0`**, not `$`: after group loss (key
  eviction, `FLUSHDB`, restore/failover) the recreated group redelivers
  history instead of skipping it. Worker-side dedupe (generation guard,
  stale ACK-and-forget) makes redelivery safe.
- `BUSYGROUP` on re-create is ignored, not fatal — restarts are routine.
- A `NOGROUP` read error at runtime recreates the group in place
  (`internal/worker/worker.go`), so the worker self-heals instead of
  error-looping until redeploy.

## The promotion Lua script (atomicity core)

`PromoteDueWithIDs` runs one script: `ZRANGEBYSCORE -inf now LIMIT 0 500` →
per member `HGET` payload → `pcall(cjson.decode)` → `XADD` → `ZREM` → `HDEL`.
Atomicity means two schedulers can never double-promote the same job
(concurrency-tested). Two poison rules keep one bad entry from stalling the
world: undecodable payloads are quarantined (`ZREM`+`HDEL`, DB row survives
for reconciliation), and orphaned members (payload lost) are `ZREM`ed instead
of returned forever.

## Retry and dead-letter flow

1. Attempt `n` fails and `n < max_attempts` → payload re-`ZADD`ed at
   `now + BackoffWithJitter(n)` with `queue_generation+1`; Postgres
   `ScheduleRetry` advances `run_at`, `last_error`, generation. The enqueue
   lands *first* so a crash between the two leaves a reclaimable stream
   message, never a lost retry.
2. Attempt `n == max_attempts` fails → `XADD jobs:dead`, Postgres
   `status='dead'`, `jobs_failed_total++`, `XACK`.
3. `GET /v1/dead-letters` lists the dead; `POST /v1/jobs/{id}/retry` resets
   attempts to 0, bumps generation, and re-enqueues immediately.

`job_executions` holds one row per attempt (`attempt_number` scoped by
`(job_id, queue_generation)`), so a retried dead job starts a clean sequence
without colliding with its history.

## Backlog accounting (`internal/stream/backlog.go`)

`queue_depth` = consumer-group lag + pending count — actual undelivered work,
not `XLEN` (which includes acked history up to `MAXLEN`). Before the group
exists it falls back to `XLEN`, which overcounts; the metric self-corrects
once workers start.

## Failure dictionary

| Failure | Behavior |
|---|---|
| Worker crash mid-callback | attempt already counted; PEL reclaim redrives after ~60–90 s |
| Redis down | API `/healthz` 503; submit/dispatch block until back (no silent loss: Postgres rows persist) |
| Scheduler crash | standby takes over within ~TTL; due jobs promote late, never twice |
| Poison payload in ZSET | quarantined by Lua; tick continues |
| Callback 500/timeout | backoff retry → DLQ after `max_attempts` |
| Crash between DB commit and Redis enqueue | outbox row reconciled within ~30 s, redelivered safely |
| Poison message (5+ deliveries, no progress) | quarantined to DLQ with explanatory `last_error`, PEL slot cleared |
| Callback > 1 MiB body | treated as failure (retried), never a false `done` |
| 301/302/303 redirect | refused (would drop payload); 307/308 same-host followed |
