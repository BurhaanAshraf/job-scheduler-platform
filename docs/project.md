# Job Scheduler Platform — Complete Guide

Durable background-job platform in Go: tenants submit jobs over HTTP to run
now, later, or on cron schedules; workers deliver them as webhooks with
retries, exponential backoff, a dead-letter queue, per-client rate limits,
and Prometheus observability. Start here, then go deep per concept:

- API behavior: [api.md](api.md) · Worker execution: [worker.md](worker.md) ·
  Scheduler, cron & leadership: [scheduler.md](scheduler.md) ·
  Redis Streams, retries & DLQ: [queue.md](queue.md) ·
  Auth, rate limiting & SSRF: [security.md](security.md) ·
  System design: [architecture.md](architecture.md) ·
  Market comparison: [comparison.md](comparison.md) ·
  Redis key map: [redis-keys.md](redis-keys.md) · Load results: [load-test-results.md](load-test-results.md)

## What it is (30 seconds)

A webhook-based job queue. A "job" is an HTTP POST your URL will receive:
`POST /v1/jobs {type, payload, run_at, max_attempts, idempotency_key,
callback_url}` → the platform stores it in Postgres, dispatches it through
Redis Streams at the right time, POSTs the payload to your URL, retries with
backoff on failure, and parks it in a dead-letter queue when attempts run
out. Cron definitions spawn instances of the same flow on a schedule.

## The three binaries

| Binary | Role | Scales | State |
|---|---|---|---|
| `api` | auth, validate, persist, dispatch | horizontally (stateless) | none local |
| `scheduler` | promote due jobs, spawn cron, 1 leader | 1 active + standbys | Redis lock |
| `worker` | claim stream, POST callbacks, record attempts | horizontally (consumer group) | none local |

Plus `callback` (demo sink), `apikey` (key provisioning CLI), `migrate`
(one-shot schema migrations). Shared code lives in `internal/` by domain:
`api` (middleware/errors), `config`, `db`, `executor` (webhook HTTP),
`health`, `logger`, `metrics`, `ratelimit`, `redisclient`, `repository`
(Postgres), `retry` (backoff math), `scheduler` (tick/cron/leader),
`stream` (Lua + groups + backlog), `validator` (SSRF), `worker`.

## Technology map (why each exists)

| Choice | Reason |
|---|---|
| Go 1.27, stdlib `net/http` | single static binaries, cheap goroutines for poll/read/reclaim loops, no framework lock-in |
| PostgreSQL 16 + `pgx` | system of record: jobs, executions audit trail, cron defs, API keys; `SELECT … FOR UPDATE` for atomic cron spawn |
| Redis 7 Streams + Lua | blocking consumer-group reads, atomic promote/schedule scripts, leader lock, rate-limit windows, metric counters |
| `robfig/cron` | 5-field cron parsing + next-run math (isolated, unit-tested) |
| Docker multi-stage → `scratch` | ~8–30 MB images, UID 65532, no shell; per-service Dockerfiles for independent deploys |
| Docker Compose | full local stack incl. demo sink + optional Caddy TLS profile |
| AWS ap-south-2: VPC, RDS, ElastiCache, ECR, ECS Fargate, ALB, Secrets Manager, CloudWatch, Budgets | managed data plane, rolling deploys, alarms → SNS |
| Terraform (S3 state, DynamoDB locks) | every AWS resource reviewed as code; dev/prod via tfvars |
| GitHub Actions OIDC | no static AWS keys; lint + `-race` tests + image builds + `terraform validate` per PR; `:sha` deploys on merge |
| Prometheus exposition | `jobs_*_total`, `queue_depth` without an agent |
| `openapi.yaml` + Swagger UI + static dashboard | contract, interactive docs, and ops UI served by the API itself |

## How to explain it in depth (interview path)

1. **Submit path**: auth → rate limit → decode/validate (incl. SSRF DNS
   check) → Postgres `INSERT` → Redis `XADD`/`ZADD` → `201`. Idempotent
   replays return `200` same id; divergent replays `409`.
2. **Dispatch**: scheduler tick (500 ms) promotes due ZSET members via one
   Lua script (atomic, poison-safe, 500/tick cap); leader lock (`SET NX PX`
   + heartbeat + atomic release) ensures exactly one dispatcher.
3. **Execution**: consumer group `workers` (`XREADGROUP BLOCK 2 s`), claim →
   `attempts+1` **before** the call (crash-visible) → `job_executions` row →
   SSRF-pinned POST (10 s, jittered-backoff retries, 307/308 same-host
   redirects only) → `done` + `XACK`, or backoff re-queue, or `dead` + DLQ.
4. **Recovery**: PEL reclaim (60 s idle, 10/30 s), `NOGROUP` self-heal, group
   ID `0` redelivery, generation fencing against stale/cancelled work.
5. **Multi-tenancy**: SHA-256-hashed keys, 60/min sliding windows with
   `Retry-After`, per-key idempotency namespaces.
6. **Ops**: health-gated ALB, lag-based `queue_depth`, 5 alarms + budget
   guardrail → SNS, JSON logs keyed by `job_id`, dashboard with retry button.
7. **Tradeoffs** (say these explicitly): Postgres+Redis over one system
   (auditability vs ops load); at-least-once + idempotency over exactly-once
   (honest about crashes); webhooks over embedded tasks (integration ease vs
   flexibility); one Redis leader over consensus (simplicity vs fencing —
   uniqueness keys cover the gap); public-subnet tasks over NAT (cost vs
   textbook topology); transactional outbox + reconciler closing the DB→Redis
   handoff (duplicates safe via the worker generation guard).

## Verify it yourself

```bash
cp .env.example .env        # set POSTGRES_PASSWORD
docker compose up -d --build
# provision a key (README has the one-liner), then:
E2E_API_KEY=<raw> go run ./tools/e2e          # 20 endpoint checks
STRESS_API_KEYS=<k1,k2> go run ./tools/stress # 1400 mixed ops, 0 unexpected
LOADTEST_API_KEYS=<5 keys> go run ./tools/loadtest  # 1200 req / 4 min sustained
go test ./... -p 1 -count=1                  # full suite (needs PG+Redis)
```

Current numbers: E2E 20/20 · stress 1400 ops 0 unexpected (p99 ~21 ms) ·
sustained 1200 req 98.75 % success, 0 errors, p50 18 ms / p95 19 ms /
p99 22 ms · unit suite all packages green (see [load-test-results.md](load-test-results.md)).
