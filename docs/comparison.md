# Market Comparison

Nearest-in-market products for a durable webhook/cron job platform: **Inngest**,
**Trigger.dev**, **Temporal**, **BullMQ/Sidekiq-style queues**, and **AWS
EventBridge Scheduler + SQS**. This project is closest to Inngest's
"reliable webhooks + retries + cron" core, minus the hosted control plane.

## Head-to-head

| Dimension | This platform | Inngest (nearest) | Trigger.dev | Temporal | BullMQ / Sidekiq | EventBridge + SQS |
|---|---|---|---|---|---|---|
| Execution model | webhook POST to your URL | function runs on their cloud / your infra | jobs on their cloud | workflows in your workers (SDK) | handler functions in your workers | target invocation (Lambda/SQS/HTTP) |
| Durable retries + backoff | yes (jittered exp., cap 15 min) | yes (sophisticated, per-step) | yes | yes (policies, heartbeating) | yes (manual/backoff strategies) | yes (retry policies + DLQ) |
| Dead-letter queue | yes, explicit retry API | yes (replay/failure mgmt) | yes | no DLQ concept (workflows run until fixed) | manual | yes (redrive) |
| Cron | 5-field, transactional spawn | yes | yes | schedules | repeatable (plugins) | yes (Scheduler, one-shot + cron) |
| Idempotency keys | yes (submit + delivery headers) | yes (event IDs) | yes (idempotency keys) | workflow IDs | job IDs (dedupe optional) | at-least-once, dedupe on FIFO only |
| Exactly-once effects | tenant-side (headers provided) | step-level dedupe | run-level | event-sourced history | tenant-side | tenant-side |
| Rate limiting | 60/min per API key (built-in) | throttling/concurrency controls | concurrency controls | task-queue rate limits | manual | quotas + API TPS |
| SSRF guard on webhooks | yes (DNS-pinned, redirect-safe) | n/a (they call you, allowlisted) | same | n/a (your workers call out) | n/a | n/a |
| Observability | Prometheus + dashboard + 6 alarms | hosted dashboard, traces | hosted dashboard | Web UI + visibility APIs | Arena/Bull Board (add-ons) | CloudWatch native |
| Multi-tenancy | bearer keys, per-key quotas | accounts/envs, usage billing | projects/envs | namespaces | DIY | IAM |
| Hosting | self-hosted (Compose/ECS, ~$15–40/mo) | SaaS (free tier → usage) | SaaS | self-host or Temporal Cloud | self-host Redis + workers | AWS-native, pay-per-use |
| Cold starts / latency | no cold starts; p99 ~22 ms submit | serverless cold starts possible | same | long-lived workers, fast | fast | Scheduler precise to the minute |
| Ops burden | you run PG + Redis + 3 services | near-zero | near-zero | cluster or cloud | Redis + workers | near-zero (inside AWS) |
| Step functions / sleeps | no (single webhook per attempt) | yes (`step.sleep`, waits) | yes (waits, delays) | yes (timers, signals) | manual | Step Functions separately |

## Where this project wins

- **Cost and control**: a side-project AWS bill and code you can read in a
  weekend, versus per-event SaaS pricing and black-box runners.
- **Webhook simplicity**: no worker SDK to adopt — any HTTPS endpoint is a
  job handler in any language. Integration is a `curl` command.
- **Security posture for webhooks**: submit- and delivery-time SSRF
  validation with DNS pinning and redirect fencing is rare in DIY queues and
  even in some managed ones.
- **Interview depth**: leader election, Lua atomicity, poison quarantine,
  generation fencing, and an explicit outbox gap read as senior distributed
  systems work, not CRUD.

## Where managed products win (say this honestly)

- **Durable step execution**: Inngest/Temporal/Trigger.dev checkpoint
  *inside* a run (`step.run`, sleeps, waits for events). This platform
  retries whole webhook attempts only — no sub-step state, no `sleep(30d)`.
- **Scale extremes**: managed control planes absorb 100k+ RPS and
  cross-region failover; this stack tops out where one RDS micro + one Redis
  node top out (fine for thousands of jobs/min, not millions).
- **Exactly-once / dedupe depth**: Temporal's event-sourced history and
  Inngest's step dedupe exceed "idempotency keys + at-least-once" — the cost
  is SDK coupling.
- **Ecosystem**: hosted dashboards with tracing, human-in-the-loop, schema
  registries, and 50+ integrations versus one clean dashboard and a retry
  button.
- **Zero ops**: no Postgres upgrades, Redis failovers, or 3 a.m. pages.

## Positioning line

> "Inngest's reliable-delivery core, self-hosted in Go for the price of a
> pizza — with the SSRF and exactly-once edges treated as first-class
> requirements instead of footnotes. What it deliberately lacks is durable
> *step* execution; what it proves is the queue, election, retry, and
> hardening fundamentals those platforms are built on."
