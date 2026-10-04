# Redis Key Naming Convention

All keys live in a single Redis instance (DB 0). This file is the canonical
list — every key pattern the system uses, who writes it, and who reads it.

## Streams (queue core, §5)

| Key | Type | Writers | Readers |
|---|---|---|---|
| `jobs:ready` | Stream, consumer group `workers` | Scheduler promotion Lua script, worker retry path (`XADD`, capped `MAXLEN ~ 100000`) | Workers (`XREADGROUP`), depth gauge |
| `jobs:dead` | Stream (no group) | Worker, when attempts ≥ max_attempts | `GET /v1/dead-letters` (via Postgres `status='dead'`) |

Consumer group `workers` on `jobs:ready` is created idempotently at worker
startup (`XGROUP CREATE MKSTREAM`, `BUSYGROUP` ignored).

## Delayed jobs (§6)

| Key | Type | Purpose |
|---|---|---|
| `jobs:scheduled` | Sorted set, score = run_at unix time | Future jobs; promotion Lua script atomically `ZRANGEBYSCORE` → `XADD` → `ZREM` |
| `jobs:scheduled:data` | Hash job_id → payload JSON | Payloads for scheduled entries, removed together with the set member |

## Coordination (§9)

| Key | Type | Purpose |
|---|---|---|
| `scheduler:leader` | String (`SET key instance_id NX PX`) | Leader-election lock; value is the owner's `SCHEDULER_INSTANCE_ID`, renewed at ~½ TTL via check-and-extend Lua, released with compare-and-delete Lua |

## Rate limiting (§4)

| Key | Type | Purpose |
|---|---|---|
| `rate_limit:<client_id>` | Sorted set of request timestamps + `PEXPIRE` window | Sliding-window counter per API client (Lua `ZCARD`/`ZADD`), checked by auth middleware |

## Metrics (§10)

| Key | Type | Purpose |
|---|---|---|
| `metrics:jobs_submitted_total` | String counter | Scraped into `/metrics` as `jobs_submitted_total` |
| `metrics:jobs_completed_total` | String counter | Scraped into `/metrics` as `jobs_completed_total` |
| `metrics:jobs_failed_total` | String counter | Scraped into `/metrics` as `jobs_failed_total` |

`queue_depth` is **not** stored: it is computed live as
undelivered + unacknowledged entries of `jobs:ready`
(`internal/stream.QueueBacklog`). `XLEN` alone is unsuitable — acknowledged
history stays in the stream, so a raw length grows forever.
