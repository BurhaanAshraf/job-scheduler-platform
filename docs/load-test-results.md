# Load test results — live AWS (dev)

Date: 2026-10-04. Target: ALB `dev-job-scheduler-alb-146113635.ap-south-2.elb.amazonaws.com`
(ECS Fargate: 1× api, 1× scheduler, 1× worker — all `256 CPU / 512 MiB`).
Tool: `tools/loadtest` variant pointed at the live ALB (5 min, 1 API key,
callback `https://httpbin.org/post`, pacing 1 req/s ≈ the 60 req/min/key limit).

## Results (300 requests, 5 min)

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

## Notes

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
