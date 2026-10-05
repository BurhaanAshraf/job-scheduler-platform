# Load test results

## Local sustained submit soak (2026-10-05, current code)

Target: local Compose stack (api, scheduler, worker, postgres, redis,
callback sink). Tool: `LOADTEST_API_KEYS=<5 keys> go run ./tools/loadtest`
(1200 requests paced over 4 min, concurrency 10, callbacks to the in-network
sink, workers executing live).

| Metric | Value |
|---|---|
| Submitted | 1200 |
| Success (200/201) | 1185 |
| Rate limited (429) | 15 (limiter shedding excess 5 req/s over quota, with `Retry-After`) |
| Errors (5xx/network/timeout) | 0 |
| Success rate | 98.75% |
| Throughput | 5.00 req/s (by design: 5 keys × 60 req/min) |
| Latency p50 | 18ms |
| Latency p95 | 19ms |
| Latency p99 | 22ms |
| Latency max | 34ms |

Workers drained the queue to `queue_depth 0`; sampled jobs landed `done`
with sink receipts (`Idempotency-Key: <jobID>:<attempt>` on every delivery).

## Mixed-endpoint stress (2026-10-05, current code)

Tool: `STRESS_API_KEYS=<7 keys> STRESS_OPS=1400 STRESS_WORKERS=20 go run
./tools/stress` — concurrent submit/get/list/cancel/retry/dead-letters/cron
+ validation (400), auth (401), missing (404), and over-quota (429) paths.

| Metric | Value |
|---|---|
| Operations | 1400 across 16 endpoint cases, 20 workers on distinct client IPs, 7 keys (hardened build) |
| Behaved exactly as specified | 540 |
| Correctly rate-limited (429) | 1067 (per-key quotas + per-IP throttle shedding load as designed) |
| Unexpected status / error | **0** |
| Latency p50 / p95 / p99 / max | 4.2ms / 27ms / 36ms / 75ms |

Also verified live against the hardened build: two keys sharing one
`client_name` hold independent quotas (key A: 60×201 then 429; key B still
201), and 310 unauthenticated requests from one IP yield 300×401 then
10×429 at the IP throttle. `job_outbox` steady-state row count after
1,700+ jobs: **0**.

## Endpoint E2E (2026-10-05, current code)

Tool: `E2E_API_KEY=<key> go run ./tools/e2e` — 20/20 pass: health, metrics,
immediate/future submit, get, list, idempotent replay (200 same id),
conflict (409), cancel pending (204), cancel running (409), dead retry,
dead-letters, cron create/disable, rate-limit 429 + `Retry-After`, SSRF 400,
auth 401s, bad id 400, max_attempts 400s.

## Live AWS (dev) — 2026-10-04 (prior run, kept for reference)

Target: ALB `dev-job-scheduler-alb-146113635.ap-south-2.elb.amazonaws.com`
(ECS Fargate: 1× api, 1× scheduler, 1× worker — all `256 CPU / 512 MiB`).
Tool: `tools/loadtest` variant pointed at the live ALB (5 min, 1 API key,
callback `https://httpbin.org/post`, pacing 1 req/s ≈ the 60 req/min/key limit).

### Results (300 requests, 5 min)

| Metric | Value |
|---|---|
| Submitted | 300 |
| Success (200/201) | 296 |
| Rate limited (429) | 4 (expected: pacing sits exactly on the 60/min/key limit) |
| Errors (5xx/network) | 0 |
| Success rate | 98.67% |
| Throughput | 1.00 req/s (capped by design: 1 key × 60 req/min) |
| Latency p50 | 40ms |
| Latency p95 | 118ms |
| Latency p99 | 176ms |
| Latency max | 231ms |

### Notes

- Latency is end-to-end over the public internet (client → ALB → Fargate in
  ap-south-2), including Postgres + Redis Streams writes per submit.
- The 4× 429s are the rate limiter working as specified (sliding window,
  `Retry-After` header), not failures.
- Workers drained all 296 jobs to completion via real outbound webhook
  callbacks through the NAT gateway (also proves private-subnet egress).
- Earlier local baseline (2026-10-03, `loadtest5.go`, 1200 req / 5 keys):
  93% success (83× 429 by design), p50 17ms, p99 20ms, 5 req/s.
- Full multi-key live soak (5 keys, ~1200 req) is pending: the account hit an
  ECS `BlockedException` on new task launches mid-session, so extra API keys
  (which require one-off ECS tasks to provision) could not be created yet.
