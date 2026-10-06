# Job Scheduler Platform

[![CI](https://github.com/BurhaanAshraf/job-scheduler-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/BurhaanAshraf/job-scheduler-platform/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.27-blue)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Docker Compose](https://img.shields.io/badge/docker-compose-ready-blue)](docker-compose.yml)

A durable background-job platform. Submit work over HTTP and the platform runs it now, later, or on a recurring schedule — delivering it to your webhook, retrying with backoff on failure, and parking exhausted work in a dead-letter queue for inspection and retry.

## Table of contents

- [What it does](#what-it-does)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quickstart — run the project step by step](#quickstart--run-the-project-step-by-step)
- [Usage](#usage)
  - [A. Single (one-off) jobs](#a-single-one-off-jobs)
  - [B. Cron (recurring) jobs](#b-cron-recurring-jobs)
- [Configuration](#configuration)
- [API reference](#api-reference)
- [Testing and verification](#testing-and-verification)
- [Project structure](#project-structure)
- [Essential commands and files](#essential-commands-and-files)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## What it does

Many services need work done outside the request that triggered it: send an email, notify a partner, generate a nightly report. Doing that inline makes users wait on downstream systems and loses the work on crash.

This platform takes that work as a recorded job, holds it until its time, `POST`s its payload to a `callback_url` you choose, retries with jittered exponential backoff, and on exhausted retries parks it in a dead-letter list you can inspect and re-queue — instead of failing silently.

## Features

- One-off jobs: run immediately (`run_at` in the past/omitted) or at a future time.
- Recurring jobs: 5-field cron expressions (`minute hour day month weekday`) that spawn job instances.
- Idempotent submissions via `idempotency_key` (`201` create, `200` replay, `409` conflict).
- Automatic retries with jittered exponential backoff (`max_attempts` 1–100).
- Dead-letter queue (`GET /v1/dead-letters`) with explicit retry (`POST /v1/jobs/{id}/retry`).
- Cancel pending work (`DELETE /v1/jobs/{id}`).
- Per-API-key rate limiting (60 req/min, `429` + `Retry-After` header).
- SSRF protection on `callback_url` (strict in production; relaxed in local Compose only for the demo sink).
- Health checks (`GET /healthz`), Prometheus metrics (`GET /metrics`), structured logs.
- Ops dashboard (`GET /dashboard`) and interactive docs (`GET /docs`).

## Architecture

```text
client -> API (cmd/api) -> PostgreSQL (jobs, cron_jobs, api_keys)
                        -> Redis (jobs:ready stream, jobs:scheduled zset)
scheduler (cmd/scheduler, leader-elected) -> promotes due jobs, spawns cron instances
worker (cmd/worker) -> POSTs payload to callback_url, records attempts, retries or dead-letters
demo receiver (cmd/callback) -> POST /hook sink, GET / viewer, GET /hits JSON
```

Job lifecycle: `pending/scheduled -> running -> done`, or `-> dead` after `max_attempts`, then back to `scheduled` via explicit retry. `cancelled` is terminal for not-yet-started work.

Details: `docs/project.md`, `docs/architecture.md`, `docs/api.md`, `docs/scheduler.md`, `docs/worker.md`, `docs/queue.md`, `docs/security.md`.

## Prerequisites

| Requirement | Version / note |
|---|---|
| Docker + Compose v2 | `docker compose version` must work |
| Terminal + `curl` | for all examples below |
| Go toolchain (optional) | `go 1.27.1` per `go.mod`; only for `go build ./...`, `go test ./...`, `go run ./tools/*`, `go run ./cmd/*` |
| AWS CLI + Terraform (optional) | only for `terraform/` production deploy |

No other installation is needed to run the full stack — Postgres and Redis run as Compose services.

## Quickstart — run the project step by step

All commands run from the repository root.

**1. Clone and configure.**

```bash
git clone https://github.com/BurhaanAshraf/job-scheduler-platform.git
cd job-scheduler-platform
cp .env.example .env
# Edit .env and set a strong POSTGRES_PASSWORD
```

**2. Start the whole platform.**

```bash
docker compose up -d --build
docker compose ps
```

This builds images from `Dockerfile.api`, `Dockerfile.scheduler`, `Dockerfile.worker`, `Dockerfile.callback`, `Dockerfile.apikey`, `Dockerfile.migrate`, starts `postgres` and `redis` with health checks, applies `migrations/` via the `migrate` service, then starts `api`, `scheduler`, `worker`, and `callback`.

**3. Confirm the platform is awake.**

```bash
curl -s localhost:4000/healthz
# {"status":"ok"}
```

`ok` means both PostgreSQL and Redis are reachable. The dashboard is at `http://localhost:4000/dashboard`, API docs at `http://localhost:4000/docs`.

**4. Create an API key.**

```bash
docker compose --profile tools run --rm apikey --client-name demo
# API key: <your-key>   <- printed once, save it; only a hash is stored
```

Export it for the examples below:

```bash
export API_KEY=<your-key>
```

**5. Submit work (see [Usage](#usage)).**

- One immediate job: [A. Single jobs](#a-single-one-off-jobs)
- One recurring schedule: [B. Cron jobs](#b-cron-recurring-jobs)

**6. Watch deliveries.**

- Dashboard: `http://localhost:4000/dashboard` (enter `$API_KEY` when asked)
- Demo receiver page: `http://localhost:8080`
- Demo receiver JSON feed: `curl -s localhost:8080/hits`

**7. Stop when finished.**

```bash
docker compose stop     # keep data in postgres_data / redis_data volumes
docker compose down     # stop everything, keep volumes
docker compose down -v  # also discard all data and start fresh
```

## Usage

> Auth: every `/v1/*` call needs `Authorization: Bearer $API_KEY`.
> Local demo callbacks use `http://callback:8080/hook` (the in-network Compose sink). This works locally because `docker-compose.yml` sets `JOB_SCHEDULER_ALLOW_PRIVATE_IPS=true` for `api` and `worker`. In production that flag is omitted and only public URLs are accepted.

### A. Single (one-off) jobs

**A1. Submit a job that runs immediately.**

`run_at` in the past (or omitted) means "run now":

```bash
curl -s -X POST localhost:4000/v1/jobs \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{
    "type": "demo",
    "payload": {"hello": "world"},
    "run_at": "2020-01-01T00:00:00Z",
    "max_attempts": 3,
    "idempotency_key": "demo-001",
    "callback_url": "http://callback:8080/hook"
  }'
# {"id":"<job-uuid>"} with HTTP 201
```

Save the id: `export JOB_ID=<job-uuid>`.

Field rules (`openapi.yaml`): `type` (max 128 chars, required), `payload` (object, required), `max_attempts` (1–100, required), `idempotency_key` (max 128 chars, required), `callback_url` (valid URL, required), `run_at` (RFC3339, optional).

**A2. Submit a job for the future.**

```bash
curl -s -X POST localhost:4000/v1/jobs \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{
    "type": "reminder",
    "payload": {"to": "user@example.com"},
    "run_at": "2030-01-01T00:00:00Z",
    "max_attempts": 3,
    "idempotency_key": "demo-future-001",
    "callback_url": "http://callback:8080/hook"
  }'
```

**A3. Read back and verify delivery.**

```bash
curl -s localhost:4000/v1/jobs/$JOB_ID -H "Authorization: Bearer $API_KEY"
curl -s "localhost:4000/v1/jobs?limit=20" -H "Authorization: Bearer $API_KEY"
curl -s "localhost:4000/v1/jobs?status=done&limit=20" -H "Authorization: Bearer $API_KEY"
curl -s localhost:8080/hits
```

Statuses: `pending, scheduled, running, done, dead, cancelled`. The immediate job should reach `done` within seconds and appear in `/hits` and on `http://localhost:8080`.

**A4. Idempotency (safe client retries).**

Repeating the exact A1 request returns the same job with HTTP `200` instead of duplicating. Changing anything except the key returns HTTP `409`:

```bash
# Same body as A1 -> HTTP 200, same {"id"}
curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:4000/v1/jobs \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{
    "type": "demo",
    "payload": {"hello": "world"},
    "run_at": "2020-01-01T00:00:00Z",
    "max_attempts": 3,
    "idempotency_key": "demo-001",
    "callback_url": "http://callback:8080/hook"
  }'
```

**A5. Cancel work that has not started yet.**

```bash
curl -s -w "%{http_code}\n" -X DELETE localhost:4000/v1/jobs/$JOB_ID \
  -H "Authorization: Bearer $API_KEY"
# 204 on success; 409 if already running/done/dead; 404 if unknown
```

Tip: cancel the future job from A2 to see a `204`.

**A6. Dead letters and retry.**

Failed jobs exhaust `max_attempts` and land in the dead-letter list:

```bash
curl -s localhost:4000/v1/dead-letters -H "Authorization: Bearer $API_KEY"
curl -s -X POST localhost:4000/v1/jobs/$JOB_ID/retry \
  -H "Authorization: Bearer $API_KEY"
# 200 + {"status":"scheduled","attempts":0,...} ; 409 unless status is dead
```

To force one locally, point a job at a sink path that never succeeds (e.g. `http://callback:8080/hook-fail`) with `"max_attempts": 1`, wait a few seconds, then list dead letters and retry it. The retry button on `http://localhost:4000/dashboard` does the same.

### B. Cron (recurring) jobs

Cron jobs are templates the scheduler instantiates on schedule. Required shape (`openapi.yaml` `CreateCronJobRequest`): `cron_expression` (5-field `minute hour day month weekday`) + `job_template` (`type`, `payload`, `max_attempts` 1–100, `callback_url`).

**B1. Create a schedule (every minute demo).**

```bash
curl -s -X POST localhost:4000/v1/cron-jobs \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{
    "cron_expression": "* * * * *",
    "job_template": {
      "type": "minute-tick",
      "payload": {"from": "cron"},
      "max_attempts": 3,
      "callback_url": "http://callback:8080/hook"
    }
  }'
# {"id":1} with HTTP 201
```

Save the id: `export CRON_ID=<id>`. Other examples: `*/5 * * * *` (every 5 min, as in `tools/e2e/main.go`), `0 2 * * *` (daily 02:00).

**B2. Verify instances are spawning.**

Within ~60–70 seconds the scheduler creates a job instance from the template:

```bash
curl -s "localhost:4000/v1/jobs?limit=20" -H "Authorization: Bearer $API_KEY"
curl -s localhost:8080/hits
```

Look for `"type":"minute-tick"` jobs progressing to `done`, and matching deliveries in `/hits` / `http://localhost:8080`. The dashboard at `http://localhost:4000/dashboard` also shows queue depth and recent jobs refreshing live.

**B3. Pause and resume without losing history.**

```bash
# Pause: stops future spawns, keeps history
curl -s -w "%{http_code}\n" -X PATCH localhost:4000/v1/cron-jobs/$CRON_ID \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{"enabled": false}'
# 204

# Resume
curl -s -w "%{http_code}\n" -X PATCH localhost:4000/v1/cron-jobs/$CRON_ID \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{"enabled": true}'
# 204
```

Bad expressions (`not-a-cron`), missing `job_template`, or a template without `callback_url` return HTTP `400`.

**When to use which:** single jobs for "do this once" (emails, webhooks, delayed reminders); cron jobs for "do this on a timetable" (nightly reports, polling, heartbeats). Both deliver to the same `callback_url` mechanism and share retries, dead letters, and dashboard visibility.

## Configuration

Copy `.env.example` to `.env`. Compose reads `.env` to build DSNs; Go services read the same names via `internal/config` (see `.env.example` comments).

| Variable | Used by | Default (Compose) | Meaning |
|---|---|---|---|
| `POSTGRES_PASSWORD` | `postgres`, `migrate`, `api`, `scheduler`, `worker`, `apikey` | _none — you set it_ | DB password; embedded into `JOB_SCHEDULER_DB_DSN` |
| `JOB_SCHEDULER_DB_DSN` | all Go services | `postgres://burhaan:${POSTGRES_PASSWORD}@postgres:5432/job_scheduler?sslmode=disable` | Primary Postgres DSN (`DB_DSN` accepted as legacy fallback) |
| `REDIS_ADDR` | all Go services | `redis:6379` in Compose, `localhost:6379` in `.env.example` | Redis address |
| `REDIS_PASSWORD` | all Go services | empty (no auth) | Redis password |
| `API_PORT` | `api` | `4000` | API listen port |
| `LOG_LEVEL` | all Go services | `info` | `debug, info, warn, error` |
| `DB_MAX_CONNS` | all Go services | `10` | Per-service DB pool size |
| `SCHEDULER_POLL_INTERVAL` | `scheduler` (+ others accept it) | `500ms` | Dispatch tick; lower = faster, chattier |
| `SCHEDULER_INSTANCE_ID` | `scheduler` | empty → `hostname-uuid` | Leader-election identity; leave empty when scaling |
| `JOB_SCHEDULER_ALLOW_PRIVATE_IPS` | `api`, `worker` | `"true"` in `docker-compose.yml` only | Dev-only SSRF relaxation for `http://callback:8080/hook`; never set in production |
| `SINK_FAIL_FIRST` | `callback` | `0` | Fail the first N `/hook` calls (retry demo) |
| `PORT` | `callback` | `8080` | Demo sink listen port |

## API reference

Contract: `openapi.yaml` (served verbatim at `GET /openapi.yaml`, rendered at `GET /docs`).

| Method & path | Auth | Success | Description |
|---|---|---|---|
| `GET /healthz` | no | `200 {"status":"ok"}` | DB + Redis reachability |
| `GET /metrics` | no | `200` text | Prometheus exposition |
| `GET /dashboard` | page: no, data: yes | `200` HTML | Ops dashboard (`web/dashboard.html`) |
| `GET /docs`, `GET /openapi.yaml` | no | `200` | Swagger UI + raw spec |
| `POST /v1/jobs` | yes | `201` / `200` replay | Create one-off job |
| `GET /v1/jobs?status=&limit=&offset=` | yes | `200` list | List/filter jobs |
| `GET /v1/jobs/{id}` | yes | `200` | Get job by UUID |
| `DELETE /v1/jobs/{id}` | yes | `204` | Cancel pending/scheduled job |
| `POST /v1/jobs/{id}/retry` | yes | `200` | Re-queue a `dead` job |
| `GET /v1/dead-letters?limit=&offset=` | yes | `200` list | List exhausted jobs |
| `POST /v1/cron-jobs` | yes | `201 {"id"}` | Create schedule |
| `PATCH /v1/cron-jobs/{id}` `{"enabled":bool}` | yes | `204` | Pause/resume schedule |

Errors are `{"error":{"code","message"}}` with `400` (validation/SSRF), `401` (auth), `404` (not found), `409` (idempotency conflict / bad state transition), `413` (>1 MiB body), `429` (+ `Retry-After`), `5xx` (server).

## Testing and verification

Tests run serially (`-p 1`) against real Postgres + Redis.

```bash
# Unit + integration (needs DB/Redis reachable; adjust password to your .env)
export JOB_SCHEDULER_DB_DSN="postgres://burhaan:<password>@localhost:5432/job_scheduler?sslmode=disable"
export REDIS_ADDR="localhost:6379" API_PORT=4000 LOG_LEVEL=info DB_MAX_CONNS=10 SCHEDULER_POLL_INTERVAL=500ms
go test ./... -p 1 -count=1
```

Live harnesses (each needs provisioned keys via env — never committed):

```bash
E2E_API_KEY=<key> go run ./tools/e2e
STRESS_API_KEYS=<k1,k2> go run ./tools/stress             # STRESS_OPS=1000 STRESS_WORKERS=20
LOADTEST_API_KEYS=<5 keys> go run ./tools/loadtest       # LOADTEST_MINUTES=4 LOADTEST_TOTAL=1200
```

Recent verified results are recorded in `docs/load-test-results.md`.

Static checks and builds:

```bash
go build ./...
go vet ./...
golangci-lint run
```

Or via `Makefile`: `make build`, `make test`, `make vet`, `make lint`, `make migrate-up`, `make migrate-down`, `make run-api`, `make run-scheduler`, `make run-worker`.

## Project structure

```text
cmd/api/            public API server (handlers.go, router.go, main.go)
cmd/scheduler/      leader-elected dispatcher + cron spawner
cmd/worker/         webhook delivery + retries + dead-lettering
cmd/apikey/         one-shot key provisioning (Dockerfile.apikey, profile tools)
cmd/callback/       demo sink: POST /hook, GET /hits, GET /reset, GET /
internal/           config, api middleware/auth/ratelimit, db, webhook,
                    health, logging, metrics, ratelimit, repository,
                    retry, scheduler, stream, validator, jobrunner
migrations/         numbered SQL up/down schema changes
tools/e2e/          20 endpoint checks against a live stack
tools/stress/       concurrent mixed-endpoint stress runner
tools/loadtest/     sustained throughput/latency soak
web/dashboard.html  dashboard source (embedded by cmd/api)
openapi.yaml        API contract
docker-compose.yml  local stack (postgres, redis, migrate, api, scheduler, worker, callback, apikey, proxy)
Dockerfile.*        one per service (api, scheduler, worker, callback, apikey, migrate)
.env.example        documented settings template (copy to .env)
Caddyfile           TLS reverse proxy for public hosting (profile public)
terraform/          VPC, Postgres, Redis, ECR, ECS + ALB, IAM, alarms (dev.tfvars, prod.tfvars)
.github/workflows/ lint, test, build, deploy pipelines
docs/               project, api, scheduler, worker, queue, security,
                    architecture (+ .mmd), comparison, load-test-results, redis-keys
```

## Essential commands and files

Assumes the stack is up and your shell is at the repo root.

```bash
# Operate the stack
docker compose logs -f api worker scheduler
docker compose up --scale worker=2
docker compose exec -T postgres psql -U burhaan -d job_scheduler
docker compose exec -T redis redis-cli

# Provision keys (prints raw key once)
docker compose --profile tools run --rm apikey --client-name demo

# Inspect deliveries
curl -s localhost:8080/hits
curl -s localhost:4000/metrics
curl -s localhost:4000/openapi.yaml | head -20

# Migrations (needs migrate CLI + JOB_SCHEDULER_DB_DSN exported)
migrate -path=./migrations -database="$JOB_SCHEDULER_DB_DSN" up
migrate -path=./migrations -database="$JOB_SCHEDULER_DB_DSN" down 1

# Local Go runs without Docker (need env from .env + local Postgres/Redis)
go run ./cmd/api
go run ./cmd/scheduler
go run ./cmd/worker

# Infra checks only (never apply from here without review)
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

Files worth knowing: `openapi.yaml`, `docker-compose.yml`, `.env.example`, `migrations/`, `cmd/` + `internal/`, `tools/e2e|stress|loadtest`, `web/dashboard.html`, `Caddyfile`, `terraform/` (`dev.tfvars`, `prod.tfvars`), `.github/workflows/`, `docs/` (start with `docs/project.md`).

## Deployment

Production runs the same images on AWS (Mumbai): VPC, managed Postgres + Redis in private subnets, ECR repos, ECS services behind an ALB with health-gated targets, secrets in the managed secret store, CloudWatch alarms (failure rate, 5xx, backlog, task health, spend) to one channel. All of it is declared in `terraform/` with `dev.tfvars` / `prod.tfvars` and reviewed like app code. CI builds, lints, tests, and validates on every change; image promotion and rollback are by redeploying revisions after `GET /healthz` verifies green.

Public hosting helper: `DOMAIN=jobs.example.com docker compose --profile public up -d` (see `Caddyfile`; DNS must point at the host first).

## Troubleshooting

| Symptom | Check |
|---|---|
| `docker compose ps` shows unhealthy | `docker compose logs -f api worker scheduler migrate postgres redis`; confirm `.env` has `POSTGRES_PASSWORD` set |
| `curl localhost:4000/healthz` → `503` | Postgres/Redis not ready yet; wait for health checks, then retry |
| `401` on `/v1/*` | `Authorization: Bearer $API_KEY` header missing/wrong; re-run the `apikey` command and re-export |
| `400` SSRF on submit | Locally you must use `http://callback:8080/hook`; `http://localhost:...` is rejected by design |
| `409` on submit | `idempotency_key` reused with different body — use a fresh key |
| `429` + `Retry-After` | 60 req/min/key exceeded; wait the seconds shown |
| No cron instances | `cron_expression` must be 5-field; `* * * * *` fires each minute — allow ~70s, then check `GET /v1/jobs` and `/hits` |
| Start completely fresh | `docker compose down -v` then `docker compose up -d --build` |

## Contributing

Branch from `main`, keep changes focused with a message explaining what and why, keep `go build ./...`, `go vet ./...`, `golangci-lint run`, and `go test ./... -p 1 -count=1` green before opening a PR. `main` stays releasable — every merge passes the full pipeline (static analysis, race-detector tests, coverage gates, secret/dependency scans, image builds, `terraform fmt/validate`).

## License

Released under the MIT license. See [LICENSE](LICENSE) for details.
