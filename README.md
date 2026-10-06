# Job Scheduler Platform

[![CI](https://github.com/BurhaanAshraf/job-scheduler-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/BurhaanAshraf/job-scheduler-platform/actions/workflows/ci.yml)

A durable background-job platform. You submit work over HTTP, and the
platform runs it now, later, or on a recurring schedule — delivering it to
your own web address, retrying intelligently when delivery fails, and
parking permanently failed work where you can inspect and re-run it.

## What it does

Many services need work done outside the request that triggered it: send an
email, notify a partner system, generate a nightly report. Doing that work
inside the original request makes users wait on downstream systems and loses
the work entirely whenever anything crashes. This platform takes that work
as a recorded job, holds it until its time, delivers it to a web address you
choose, retries with a growing delay when delivery fails, and gives up
visibly — into a dead-letter list you control — instead of silently.

## Key capabilities

- Run jobs immediately, at a future time, or on a recurring cron schedule.
- Safe client retries through idempotency keys, with conflicts reported
  rather than duplicated.
- Automatic retries with jittered exponential backoff and a capped delay,
  so struggling downstream systems get breathing room instead of hammering.
- A dead-letter queue with one-action manual retry for work that exhausts
  its attempts.
- Per-client request quotas with clear retry guidance when quotas are hit.
- Protection against malicious job definitions that try to reach internal
  network addresses from the workers.
- Health checks, Prometheus metrics, structured logs, and a built-in
  operations dashboard with documentation served by the API itself.

## How a job flows through the system

A client sends a job to the API with its type, data, scheduled time, retry
budget, idempotency key, and the web address to deliver to. The API
authenticates the client, checks its quota, validates the submission
including a live safety check of the delivery address, stores the job in
PostgreSQL, and hands it to Redis for dispatch — recording that intent in
the same database transaction so a crash can never lose it.

A single elected scheduler promotes jobs whose time has come and spawns
instances of cron schedules. Workers claim ready jobs and post their data
to the delivery address, recording every attempt. Success completes the job.
Failure re-queues it after a backoff delay until its retry budget runs out,
at which point it waits in the dead-letter list for an explicit human or
automated retry. A background reconciler finishes any handoff a crash
interrupted, so no committed job is ever stranded.

Every step of that journey is logged under the job's identifier, so the
full story of any job is one search away.

## Project structure

The repository is organized so each deployable piece and each area of
knowledge has exactly one home.

- Command entry points, one per deployable program: the public API server,
  the scheduler, the worker pool, the API key provisioning tool, the demo
  callback receiver, and the database migration runner.
- Internal libraries grouped by responsibility: request handling and
  middleware, configuration, database access, webhook delivery, health
  reporting, logging, metrics, rate limiting, data repositories, retry
  mathematics, scheduling and leader election, queue primitives, delivery
  address safety checks, and job processing.
- Versioned database migrations that move the schema forward and backward
  in numbered steps.
- Operational tooling for endpoint checks, mixed-endpoint stress runs, and
  sustained load soaks.
- Infrastructure as code describing the network, database, cache, container
  registry and permissions, container hosting with load balancing,
  deployment identity, and cost guardrails, with separate settings for
  development and production environments.
- Documentation with one guide per concept: the overall project, the API,
  the worker, the scheduler, the queue and retry behavior, security, the
  architecture, a market comparison, measured load results, and a map of the
  live data structures.

## Prerequisites

You need container tooling with Compose support, plus a terminal. That is
all for running the full platform locally. Working on the Go code directly
additionally needs a recent Go toolchain. Deploying to production needs
command-line access to an AWS account.

## Running the project, step by step

**1. Prepare your private settings.** Copy the provided example environment
file to your own local file and choose a strong database password inside
it. This local file is never committed and never baked into images.

```bash
cp .env.example .env
# then edit .env and set POSTGRES_PASSWORD
```

**2. Start the whole platform.** One command builds every service image,
starts PostgreSQL and Redis with health checks, applies the database schema
automatically, and brings up the API, scheduler, worker, and demo receiver.

```bash
docker compose up -d --build
docker compose ps   # every service should report healthy
```

**3. Confirm the platform is awake.** A healthy answer here means both the
database and the cache are reachable from the application.

```bash
curl localhost:4000/healthz   # {"status":"ok"}
```

**4. Create your first API key.** One command provisions a key through the
platform's own tooling and prints it exactly once — save it somewhere safe,
since only an irreversible fingerprint is stored and the original can never
be recovered. It needs nothing installed beyond Compose itself.

```bash
docker compose --profile tools run --rm apikey --client-name demo
# API key: <your-key>
```

Save the printed value as `API_KEY` for the steps below (`export
API_KEY=<your-key>`), or pass it inline where a command shows `$API_KEY`.

**5. Open the operations dashboard.** Go to
http://localhost:4000/dashboard and enter your key when asked. You will see
live queue numbers, recent jobs, and the dead-letter list, refreshing on its
own every few seconds. The interactive API reference lives at
http://localhost:4000/docs.

**6. Run your first job end to end.** Submit a job addressed to the included
demo receiver (replace the key below with yours), then watch it arrive: the
dashboard shows it completing, and the receiver's page at
http://localhost:8080 shows the delivery it got.

```bash
curl -X POST localhost:4000/v1/jobs -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' -d '{
    "type": "demo", "payload": {"hello":"world"},
    "run_at": "2020-01-01T00:00:00Z", "max_attempts": 3,
    "idempotency_key": "demo-001",
    "callback_url": "http://callback:8080/hook"}'
```

Repeating that exact request returns the same job instead of duplicating
it; changing anything but the key reports a conflict.

**7. Shut down when finished.** Your database content persists in its named
volume, so restarting later resumes exactly where you left off.

```bash
docker compose stop     # keep data
docker compose down     # stop everything
docker compose down -v  # also discard all data and start fresh
```

## Configuration in plain language

The platform reads a small set of environment settings. The database
connection address and the cache address tell each service where its
dependencies live. The HTTP port sets where the API listens. The log level
and the per-service database connection limit have sensible defaults and
only need changing for tuning. The scheduler's tick interval balances
dispatch speed against polling chatter, defaulting to half a second. One
development-only switch relaxes delivery-address safety so the in-network
demo receiver is accepted; production deployments leave it unset, which
keeps the safety checks strict from submission through delivery. The
scheduler's identity defaults to a unique value per machine, which leader
election depends on — never give replicas a shared fixed identity.

## Day-to-day use

Submit work through the jobs endpoint with its data, timing, retry budget,
idempotency key, and delivery address. Read any job back by its identifier,
browse and filter the job list, or cancel work that has not started yet.
Exhausted work appears in the dead-letter list and returns to the queue
through the retry action, available both in the API and as a button on the
dashboard. Recurring work is managed through the cron endpoints: create a
schedule from a cron expression plus a job template, and pause or resume it
without losing history. Quotas are generous per key; when you exceed yours
the platform tells you exactly how long to wait before retrying.

## Testing and verification

Correctness is checked at four levels. Unit and integration tests cover
every package against real PostgreSQL and Redis and run serially because
they share those services. An endpoint checker exercises every route and
error path against a live stack. A stress runner hammers all endpoints
concurrently and fails on any surprising response. A sustained load runner
measures throughput and latency over several minutes. Recent verified
results are recorded with the load documentation: twenty out of twenty
endpoint checks passing, fourteen hundred mixed operations with zero
surprises, and twelve hundred sustained submissions at ninety-nine percent
success with no errors and a ninety-ninth percentile latency in the low
tens of milliseconds.

## Essential commands and files

Everything below assumes the stack is running locally and your shell sits
at the repository root.

**Build and static checks.**

```bash
go build ./...
go vet ./...
golangci-lint run
```

**Full test suite.** Tests need the database and cache addresses, so export
them first (adjust the password to match your local environment file).
Running from your own machine also needs PostgreSQL and Redis reachable at
those addresses — install them locally, or rely on the pipeline, which
provides both as services.

```bash
export JOB_SCHEDULER_DB_DSN="postgres://burhaan:<password>@localhost:5432/job_scheduler?sslmode=disable"
export REDIS_ADDR="localhost:6379" API_PORT=4000 LOG_LEVEL=info DB_MAX_CONNS=10 SCHEDULER_POLL_INTERVAL=500ms
go test ./... -p 1 -count=1
```

**Live verification harnesses.** Each needs provisioned API keys passed
through the environment, so no secret is ever committed.

```bash
E2E_API_KEY=<key> go run ./tools/e2e
STRESS_API_KEYS=<k1,k2> go run ./tools/stress             # STRESS_OPS=1000 STRESS_WORKERS=20
LOADTEST_API_KEYS=<5 keys> go run ./tools/loadtest       # LOADTEST_MINUTES=4 LOADTEST_TOTAL=1200
```

**Operating the stack.**

```bash
docker compose logs -f api worker scheduler   # follow service logs
docker compose up --scale worker=3            # run three workers
docker compose exec -T postgres psql -U burhaan -d job_scheduler   # database shell
docker compose exec -T redis redis-cli        # cache shell
```

**Database migrations.** Schema changes ship as numbered files; the
migrate service applies them automatically on startup, and these run them
by hand when needed.

```bash
migrate -path=./migrations -database="$JOB_SCHEDULER_DB_DSN" up
migrate -path=./migrations -database="$JOB_SCHEDULER_DB_DSN" down 1
```

**Infrastructure (checked, never applied from here).**

```bash
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

**Files worth knowing by path.**

- `openapi.yaml` — the API contract; served verbatim and rendered at `/docs`.
- `web/dashboard.html` — the dashboard source; the API embeds a verified copy.
- `docker-compose.yml` — the full local stack, including the dev-only
  safety relaxation for the demo receiver.
- `.env.example` — every setting with an explanation; copy to `.env`.
- `migrations/` — numbered schema changes, each with an undo step.
- `cmd/` and `internal/` — service entry points and domain libraries.
- `tools/e2e`, `tools/stress`, `tools/loadtest` — the verification harnesses.
- `terraform/` — the whole cloud footprint, with per-environment settings
  in `dev.tfvars` and `prod.tfvars`.
- `.github/workflows/` — the pipelines that lint, test, build, and deploy.
- `docs/` — one guide per concept, starting with `docs/project.md`.

## Deploying to production

Production runs the same container images on managed infrastructure in the
Mumbai region: a virtual network, managed PostgreSQL and Redis in private
subnets, container image repositories, serverless container hosting behind
a load balancer with health-gated targets, secrets held in the managed
secret store and referenced by name, and alarms for failure rate, endpoint
errors, queue backlog, task health, and monthly spend — all notifying one
shared channel. Deploying builds and ships new image
revisions through short-lived cloud credentials, verifies the
live health endpoint before declaring success, and rolls back by redeploying
the previous revision. Deployments currently run by hand until the cloud
access for automation is wired up; the pipeline already builds, tests, and
validates everything on every change. Every piece of that infrastructure is declared in the
infrastructure directory and reviewed like application code, with separate
lean settings for development and production.

## Documentation map

Start with the project guide for the complete picture in one place, then
go deeper per interest: the API, the worker, the scheduler, the queue and
retry behavior, security, the system architecture, and a comparison with the
closest products on the market.
Operators will also want the live data-structure map and the measured load
results.

## Quality gates

Every change passes the same bar before it can merge: static analysis with
zero warnings, the full test suite with the race detector against real
services, a minimum coverage threshold on the core packages, secret and
dependency vulnerability scanning, successful builds of all container
images, and infrastructure formatting and validation.

## Contributing

Branch from the main line, keep each change focused with a message that
explains what and why, and make sure formatting, static analysis, and the
full test suite are green before proposing a merge. The main line always
stays releasable, because every merge has passed the full pipeline.

## License

Released under the MIT license. See the license file for details.
