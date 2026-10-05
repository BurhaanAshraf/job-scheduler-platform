# Security — In Depth

Three layers: tenant auth, per-client rate limiting, and SSRF-safe webhook
delivery. Secrets never touch git, images, or task definitions in plaintext.

## API keys (`migrations/000002`, `internal/api/auth.go`)

- `api_keys(id, client_name, hashed_key UNIQUE, created_at, revoked_at)`.
  Only the **SHA-256 hex** of the key is stored — the raw value is
  unrecoverable from the database, and no plaintext column exists.
- Provisioning: `go run ./cmd/apikey` (or the README `psql` snippet) prints
  the raw key **once**; verification hashes the presented bearer token and
  compares. Revocation sets `revoked_at`; revoked and unknown keys both get
  `401` with no oracle distinguishing them.
- Every `/v1/*` route sits behind `APIKeyAuth`; `/healthz`, `/metrics`,
  `/docs`, `/openapi.yaml`, `/dashboard` are public by design (no tenant data
  on any of them; dashboard data calls still need a key).

## Rate limiting (`internal/ratelimit/limiter.go`)

- Sliding window in Redis per `rate_limit:key:<key-ID>`: `ZADD now member →
  ZREMRANGEBYSCORE older-than-window → ZCARD ≤ 60/min`, else `429` with
  `Retry-After: 60`. One atomic Lua script per request; no local state, so
  all API replicas enforce one shared quota. Buckets are keyed by the unique
  API-key row ID — `client_name` is not unique, so name-bucketing let one key
  eat another key's quota (fixed; regression-tested with colliding names).
- An outer IP throttle (`internal/api/iplimit.go`, 300 req/min per client IP
  from `X-Forwarded-For` or the connection address) bounds unauthenticated
  traffic before auth: key-guessing and scrape floods get `429`s instead of
  free rein. The budget is generous on purpose — per-key quotas still enforce
  fairness after auth. Behind an ALB, `X-Forwarded-For` carries the client IP;
  its leftmost entry is client-controlled, so this layer is a throttle, not
  an identity, and never gates authentication.
- Member uniqueness uses `math/rand` + millisecond timestamps — a same-ms
  collision would undercount by one entry (fail-open by a single request).
  Acceptable at 60/min granularity; `crypto/rand` is the drop-in hardening.

## SSRF protection (submit + delivery)

A job's `callback_url` makes *your worker* issue HTTP requests to
attacker-chosen URLs — the classic SSRF sink (cloud metadata
`169.254.169.254`, internal hosts, redirect chains). Defense is two-layered:

1. **Submit-time** (`cmd/api/handlers.go validateCallbackURL` +
   `internal/validator`): scheme must be http(s); hostname blocklist
   (`localhost`, `*.localdomain`, `host.docker.internal`,
   `host.containers.internal`, metadata hosts, `169.254.169.254`);
   then **DNS-resolve once** and check every IP against private/link-local
   CIDRs (`10/8`, `172.16/12`, `192.168/16`, `127/8`, `169.254/16`,
   `0.0.0.0/8`, `::/128`, `::1/128`, `fe80::/10`, `fc00::/7`). Hostnames are
   lowercased and trailing-dot-stripped before comparison.
2. **Delivery-time** (`internal/executor/http.go`): re-validates through a
   pinned transport whose dialer connects **only to the validated IPs**
   (DNS-rebinding protection), with a single resolution (no check/connect
   TOCTOU). Redirects: only same-host 307/308 are followed (method and body
   preserved); 301/302/303 are refused rather than silently downgraded to
   GET; cross-host redirects fail closed. 10 s total timeout; >1 MiB bodies
   fail instead of recording false success.

- `JOB_SCHEDULER_ALLOW_PRIVATE_IPS=true` disables both layers for the
  compose demo sink (`http://callback:8080/hook`). It is set **only** in
  `docker-compose.yml`; production task definitions omit it, keeping the
  guard strict end to end. Enabling it logs nothing special — treat the env
  var itself as the control.
- Residual risk (documented, not hidden): decimal/octal/hex IP spellings
  (`2130706433`) fail closed at DNS resolution (Go can't dial them either);
  fast-flux DNS between submit and delivery is caught by delivery-time
  re-validation; unauthenticated submit-time DNS lookups cost one upstream
  resolution per request (no cache — a deliberate correctness-over-speed
  choice at this scale).

## Supply chain and CI gates

- `gitleaks` secret scan + `govulncheck` on every push; 70 % coverage floor
  on `internal/`; `golangci-lint` (govet, staticcheck, errcheck, unused,
  misspell, unconvert, bodyclose, noctx, rowserrcheck); `-race` tests
  against real Postgres + Redis service containers; all images (including
  `migrate`) built in CI; `terraform fmt`+`validate` on every PR.
- Images run as UID 65532 on `scratch` (distroless-equivalent), no shell,
  no package manager; `.dockerignore` keeps secrets, infra, and docs out of
  build contexts. DB credentials travel as Secrets Manager ARNs, never env
  literals.
