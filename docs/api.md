# API Service — In Depth

The `api` binary (`cmd/api/`) is the platform's only public surface: a
stateless HTTP server that authenticates tenants, validates submissions,
persists jobs to Postgres, and hands live work to Redis. It never executes
jobs itself.

- Entrypoint: `cmd/api/main.go` (wires config → pool → Redis → router)
- Handlers: `cmd/api/handlers.go` (~800 lines, all `/v1` logic)
- Router: `cmd/api/router.go` (middleware chain + `/docs`, `/dashboard`)
- Contract: `openapi.yaml` (served verbatim at `GET /openapi.yaml`)
- Interactive docs: `cmd/api/swagger-ui.html` served at `GET /docs`
- Dashboard: `cmd/api/dashboard.html` (embedded copy of `web/dashboard.html`;
  `TestEmbeddedAssetsInSync` fails CI on drift)

## Request lifecycle (`POST /v1/jobs`)

1. **Global middleware** (`internal/api/middleware.go`): panic recovery
   (500 JSON, process survives) → request logging (method, path, status,
   latency, `X-Request-ID`).
2. **Auth** (`internal/api/auth.go`): `Authorization: Bearer <raw-key>` is
   SHA-256 hashed and looked up in `api_keys`; missing/unknown/revoked →
   `401`. The client name is attached to the request context.
0. **IP throttle** (`internal/api/iplimit.go`, outside auth): 300
   req/min per client IP before anything else runs, so unauthenticated floods
   cost `429`s instead of free database lookups.
3. **Rate limit** (`internal/api/ratelimit.go` +
   `internal/ratelimit/limiter.go`): sliding-window `ZADD`/`ZREMRANGEBYSCORE`/
   `ZCARD` in Redis, 60 req/min per API-key ID (unique per key, never the
   non-unique client name); over quota → `429` with `Retry-After`.
4. **Decode** (`decodeJSONBody`): `MaxBytesReader` 1 MiB → oversized is `413`
   (`REQUEST_TOO_LARGE`), unknown fields and trailing garbage are `400`.
5. **Validate** (`validateCreateJobRequest`): type ≤128 chars, payload valid
   JSON, `max_attempts` 1–100, idempotency key present ≤128 chars,
   `callback_url` present and SSRF-clean (syntactic private-host check plus
   full DNS-resolving `validator.ValidateCallbackURL`). Unresolvable DNS is
   accepted here — delivery-time validation is authoritative.
6. **Persist** (`repository.JobRepository.Create`): one `INSERT` with status
   `pending`. Duplicate `idempotency_key` → `23505` → conflict path.
7. **Dispatch**: `run_at` in the future → `stream.ScheduleJob` (`ZADD` score =
   unix time + `HSET` payload, atomically via Lua); otherwise
   `stream.EnqueueDue` (`XADD` to `jobs:ready`). Redis failure after a
   successful `INSERT` → `500` with an explicit "needs reconciliation" log
   (documented outbox gap — see [architecture](architecture.md)).
8. **Respond**: `201 {"id"}` on create; `200 {"id"}` on byte-identical replay;
   `409 IDEMPOTENCY_KEY_CONFLICT` when the key exists with different
   type/payload/max_attempts/callback (payloads compared semantically because
   Postgres `jsonb` normalizes whitespace).

## Endpoint reference

| Method & path | Auth | Success | Notes |
|---|---|---|---|
| `POST /v1/jobs` | key | `201` / `200` replay | `run_at` omitted or past = immediate |
| `GET /v1/jobs/{id}` | key | `200` job | bad UUID `400`, unknown `404` |
| `GET /v1/jobs?status=&limit=&offset=` | key | `200` list | `limit` capped at 100, default 20 |
| `DELETE /v1/jobs/{id}` | key | `204` | only `pending`/`scheduled`; else `409`; also `ZREM`s the delayed entry (worker generation guard is the backstop) |
| `POST /v1/jobs/{id}/retry` | key | `200` job | only `dead`: resets attempts to 0, `run_at=now`, generation+1, immediate enqueue |
| `GET /v1/dead-letters?limit=&offset=` | key | `200` list | exhausted jobs, same paging rules |
| `POST /v1/cron-jobs` | key | `201 {"id"}` | 5-field cron + template (type/payload/max_attempts/**callback_url required**) |
| `PATCH /v1/cron-jobs/{id}` | key | `204` | `{"enabled":bool}`; disable stops spawning, keeps history |
| `GET /healthz` | none | `200 {"status":"ok"}` | `503` when Postgres or Redis is unreachable (2 s each, sequential) |
| `GET /metrics` | none | Prometheus text | `jobs_submitted/completed/failed_total`, `queue_depth` |
| `GET /docs`, `/openapi.yaml`, `/dashboard` | none (page) | `200` HTML/YAML | dashboard API calls still need a key |

Error shape is uniform: `{"error":{"code":"...","message":"..."}}`.
`400` validation, `401` auth, `404` unknown id, `409` state/idempotency
conflict, `413` body > 1 MiB, `429` + `Retry-After` over quota.

## Idempotency design

`idempotency_key` is `UNIQUE NOT NULL` per table (client-scoped in practice:
one row per key). Replays are safe because the conflict path compares the
full effective request, not just key existence. The one deliberate
simplification: `run_at` is **not** part of the comparison, so replaying a
key with a different schedule returns the original job (`200`) instead of
`409` — resubmission never forks a duplicate.

## Configuration

`JOB_SCHEDULER_DB_DSN` (or legacy `DB_DSN`), `REDIS_ADDR`, `REDIS_PASSWORD`,
`API_PORT`, `LOG_LEVEL` (default `info`), `DB_MAX_CONNS` (default `10`),
`SCHEDULER_POLL_INTERVAL` (default `500ms`, scheduler use),
`JOB_SCHEDULER_ALLOW_PRIVATE_IPS` (dev only — lets validation and delivery
accept the compose demo sink; **never set in production**).
