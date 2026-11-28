# HookRelay

Developer-first webhook infrastructure. You send an event once over a REST API. HookRelay
stores it in PostgreSQL and delivers it to every subscribed endpoint with signed HTTP requests,
automatic exponential-backoff retries, and a full per-attempt delivery history.

> **Status: the backend MVP is complete (Phases 0–5 of 8).** Ingestion, endpoint management, signed delivery with
> retries, the dispatcher and reaper, the read APIs, manual retry and statistics are built and tested against Neon
> and Redis. The dashboard, load tests and deployment are next. See [Roadmap](#roadmap).

## How it works

```
Sender ──POST /v1/events──▶ API (Go) ──1 transaction──▶ PostgreSQL (Neon)   ◀── source of truth
                                                          │  events + deliveries (outbox)
                                       Dispatcher ◀───────┘
                                           │ enqueue
                                           ▼
                                     Redis / Asynq ──▶ Workers ──signed POST──▶ Your endpoints
```

- **No lost events.** The API writes the event and its deliveries in a single Postgres
  transaction (a transactional outbox) and never writes to Redis directly. Losing Redis loses nothing.
- **At-least-once delivery.** Postgres owns all delivery state and the retry schedule. Workers
  claim deliveries with conditional updates, so duplicate tasks are harmless.
- **Standard Webhooks signatures.** Requests carry `webhook-id`, `webhook-timestamp` and
  `webhook-signature` headers, signed with HMAC-SHA256.

The full design is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## What works today

| Area | State |
|---|---|
| Go service skeleton | `hookrelay api` with strict env config, JSON logging, request IDs, graceful shutdown, `/healthz`, `/readyz` |
| Database schema | 6 tables (projects, api_keys, endpoints, events, deliveries, delivery_attempts). Tenant isolation is enforced by composite foreign keys. Migrations are reversible. |
| Migrations | `hookrelay migrate up\|down\|status\|version` (goose, embedded, advisory-locked) |
| Data access | sqlc-generated, type-safe queries on pgx/v5; transaction helper |
| Secrets | API keys stored as SHA-256 hashes only; endpoint signing secrets encrypted with AES-256-GCM; secret types redact themselves in logs |
| Admin CLI | `hookrelay admin create-project` / `create-key` (the plaintext key is shown once) |
| Hosting | PostgreSQL 18 on **Neon**: pooled URL for the app, direct URL for migrations |
| CI | GitHub Actions: gofmt, go vet, golangci-lint, sqlc drift check, `go test -race` with Postgres and Redis |
| REST API | `POST /v1/events` (idempotent ingest with fan-out in one transaction) and `/v1/endpoints` CRUD. See [`docs/API.md`](docs/API.md). |
| Read APIs & stats | List and inspect events (exact payload), deliveries and every attempt, with cursor pagination and filters (status, endpoint, event, event type, date range). `POST /v1/deliveries/{id}/retry` gives a dead delivery one more attempt without touching its history. `GET /v1/stats`: counts by status, success/failure rates, p50/p95 latency. |
| Security | Bearer API keys (hash lookup; revoked keys rejected); SSRF checks on endpoint URLs at registration *and* at connect time (DNS-rebinding safe); signing secrets bound to their endpoint |
| Delivery worker | `hookrelay worker`: Asynq task handler that claims a delivery with a conditional update, signs it (Standard Webhooks HMAC-SHA256), POSTs the exact stored bytes, and records the attempt, fenced on `attempt_count`. Failures are rescheduled in PostgreSQL with exponential backoff and jitter, or marked `dead`. |
| Dispatcher & reaper | The dispatcher moves due deliveries from PostgreSQL into Redis (commit first, enqueue after; releases them if Redis is down). The reaper re-dispatches queued deliveries whose task never ran and recovers in-flight deliveries whose worker died. Both run inside `hookrelay worker`, or alone as `hookrelay dispatcher` / `hookrelay reaper`. |
| Mock receiver | `cmd/mockreceiver`: verifies signatures, logs `webhook-id`, and takes `?status=`, `?latency_ms=`, `?fail_rate=` |

Not built yet: the dashboard (Phase 6), k6 benchmarks (Phase 7) and deployment (Phase 8).

## Tech stack

Go · Chi · pgx/v5 · sqlc · goose · Asynq + Redis · PostgreSQL 18 (Neon) ·
Next.js + Tailwind + shadcn/ui (planned) · Docker · GitHub Actions · k6 (planned)

## Getting started

Prerequisites: Go 1.27+, GNU make, and Docker *or* a Neon account. On Windows, run `make` from Git Bash.

```bash
make env                 # create .env with a random HOOKRELAY_ENCRYPTION_KEY
make tools               # golangci-lint + sqlc (on Windows, download the sqlc release binary)
```

**Option A: local Postgres and Redis (Docker)**
```bash
make dev-up              # Postgres 18 + Redis 7, waits until healthy
make migrate
```

**Option B: Neon**
```bash
npm i -g neon@latest && neon login
neon env pull            # writes DATABASE_URL (pooled) and DATABASE_URL_UNPOOLED (direct) into .env
make migrate             # migrations use the direct URL automatically
```

Then:
```bash
make create-project NAME="Acme"   # prints a project ID and an API key (shown once)
make run-api                      # http://localhost:8080/healthz and /readyz
```

`/readyz` returns 200 only when both Postgres and Redis are reachable.

## Commands

| Command | What it does |
|---|---|
| `make dev-up` / `dev-down` / `dev-reset` | Start, stop, or wipe local Postgres and Redis |
| `make migrate` / `migrate-down` / `migrate-status` | Manage the schema |
| `make create-project NAME=…` | Create a project and its first API key |
| `make run-api` | Run the API server |
| `make run-worker` | Run the delivery worker (handlers + dispatcher + reaper) |
| `make sqlc` / `sqlc-check` | Regenerate data-access code / fail if it is stale |
| `make test` | Run tests (database integration tests run when `TEST_DATABASE_URL` is set) |
| `make ci` | gofmt check, vet, lint, sqlc check and tests: the same checks as CI |

## Repository layout

```
backend/
  cmd/hookrelay/        CLI entrypoint: api | worker | dispatcher | reaper | migrate | admin
  cmd/mockreceiver/     local webhook receiver for testing deliveries
  db/migrations/        goose SQL migrations (embedded into the binary)
  db/queries/           hand-written SQL for sqlc
  internal/config/      strict environment configuration
  internal/httpapi/     router, middleware, auth, /v1 handlers, health checks
  internal/store/       pgx pool, transactions, sqlc-generated code (sqlcgen/)
  internal/migrate/     migration runner
  internal/secrets/     AES-GCM encryption, API keys, signing secrets
  internal/delivery/    Asynq task + handler (claim, send, record), SSRF-guarded HTTP sender
  internal/dispatch/    dispatcher and reaper loops
  internal/history/     read APIs, manual retry, statistics
  internal/signing/     Standard Webhooks sign/verify
  internal/backoff/     retry schedule (exponential, full jitter, Retry-After)
docs/                   product spec, architecture, implementation plan
neon.ts                 Neon project configuration (`neon deploy`)
web/                    reserved for the Next.js dashboard
```

## Configuration

All configuration comes from environment variables; see [`.env.example`](.env.example).
Unknown or malformed `HOOKRELAY_*` variables stop the process at startup. Required:
`HOOKRELAY_ENV`, `DATABASE_URL`, `REDIS_URL`, `HOOKRELAY_ENCRYPTION_KEY`.
`.env` is git-ignored. Never commit it.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0 | Scaffolding, config, logging, health checks, CI | ✅ Done |
| 1 | Schema, migrations, sqlc, secrets, admin CLI | ✅ Done (live on Neon) |
| 2 | REST API: API-key auth, endpoint CRUD, event ingest with fan-out and idempotency | ✅ Done |
| 3 | Delivery core: signing, SSRF-safe HTTP sender, worker claim/record, retry scheduling | ✅ Done |
| 4 | Dispatcher, reaper, end-to-end reliability tests | ✅ Done |
| 5 | Read APIs, manual retry, stats | ✅ Done |
| 6 | Next.js dashboard | Next |
| 7 | k6 load tests and published benchmarks | |
| 8 | Deployment | |

Details are in [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md). The product scope is in
[`docs/PRODUCT_SPEC.md`](docs/PRODUCT_SPEC.md).

**Known gaps:** production Redis is not provisioned yet (Phase 8); locally, `make dev-up` runs it in Docker.
Ingestion and the read APIs do not need Redis; only delivery does.
