# HookRelay

Developer-first webhook infrastructure. You send an event once over a REST API. HookRelay
stores it in PostgreSQL and delivers it to every subscribed endpoint with signed HTTP requests,
automatic exponential-backoff retries, and a full per-attempt delivery history.

> **Status: early development. Phases 0–1 of 8 are complete.** The foundation, database schema,
> data access, secret management and admin CLI are built, and the schema is live on Neon. Event
> ingestion, webhook delivery and the dashboard are not built yet. See [Roadmap](#roadmap).

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

Not built yet: the event/endpoint REST API, delivery workers, retries, and the dashboard.

## Tech stack

Go · Chi · pgx/v5 · sqlc · goose · Asynq + Redis (planned) · PostgreSQL 18 (Neon) ·
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
| `make sqlc` / `sqlc-check` | Regenerate data-access code / fail if it is stale |
| `make test` | Run tests (database integration tests run when `TEST_DATABASE_URL` is set) |
| `make ci` | gofmt check, vet, lint, sqlc check and tests: the same checks as CI |

## Repository layout

```
backend/
  cmd/hookrelay/        CLI entrypoint: api | worker | migrate | admin
  db/migrations/        goose SQL migrations (embedded into the binary)
  db/queries/           hand-written SQL for sqlc
  internal/config/      strict environment configuration
  internal/httpapi/     router, middleware, health checks
  internal/store/       pgx pool, transactions, sqlc-generated code (sqlcgen/)
  internal/migrate/     migration runner
  internal/secrets/     AES-GCM encryption, API keys, signing secrets
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
| 2 | REST API: API-key auth, endpoint CRUD, event ingest with fan-out and idempotency | Next |
| 3 | Delivery core: signing, SSRF-safe HTTP sender, worker claim/record | |
| 4 | Dispatcher, retries with backoff, reaper, reliability tests | |
| 5 | Read APIs, manual retry, stats | |
| 6 | Next.js dashboard | |
| 7 | k6 load tests and published benchmarks | |
| 8 | Deployment | |

Details are in [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md). The product scope is in
[`docs/PRODUCT_SPEC.md`](docs/PRODUCT_SPEC.md).

**Known gaps:** the PostgreSQL integration tests have not yet run locally (Docker is not set up on
the dev machine) and will run for the first time in CI. Redis is not yet provisioned for the Neon
setup, so `/readyz` reports it as unavailable there.
