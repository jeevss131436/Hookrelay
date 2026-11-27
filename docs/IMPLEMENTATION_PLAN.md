# HookRelay — MVP Implementation Plan

This plan is for one developer. The phases are ordered so that the hardest correctness
work (outbox, claim/fence, retries) is built and tested **before** the dashboard. Each
phase ends in a working, testable state. The effort estimates assume part-time work and are rough.

Read `ARCHITECTURE.md` first. The design decisions D1–D8 referenced below are defined there.

---

## Phase 0 — Foundations (~2–3 days)

- [x] Monorepo layout as in `ARCHITECTURE.md §9`, with `backend/` holding a Go module and `web/` added later.
- [x] `docker-compose.yml` with `postgres:18` (matches Neon), `redis:7`, and (later) `mockreceiver`.
- [x] `internal/config`: parse env into a struct, fail fast on missing values, and validate the lease invariant (§6.3).
- [x] `cmd/hookrelay` with subcommand skeletons `api`, `worker`, `migrate`, `admin`.
- [x] Logging with `log/slog` (JSON handler) and request-ID middleware.
- [x] GitHub Actions: `go vet`, `staticcheck` or `golangci-lint`, and `go test ./...` with Postgres and Redis as service containers.
- [x] Makefile (or Taskfile) targets: `dev-up`, `migrate`, `sqlc`, `test`, `run-api`, `run-worker`.

**Done when:** `make dev-up && make test` passes locally and in CI.

> Status: code complete. Not yet verified: `docker compose` health, because Docker/WSL2 is not set up on the dev machine, and a first CI run on GitHub.

## Phase 1 — Schema and data access (~2–3 days)

- [x] goose migrations for every table and index in `ARCHITECTURE.md §4` (`backend/db/migrations`, embedded, with a reversible `Down`).
- [x] sqlc setup (`pgx/v5` driver) and initial queries for projects and api_keys. Endpoint queries move to Phase 2 alongside endpoint CRUD, so they are written against their real use.
- [x] `store.InTx(ctx, func(q *sqlcgen.Queries) error)` helper (commit on nil; rollback on error or panic).
- [x] `internal/secrets`: key parsing, AES-GCM encrypt/decrypt, API key generation and hashing, signing-secret generation, with unit tests.
- [x] `hookrelay admin create-project` (project + first key in one transaction) and `admin create-key`. The plaintext key is printed once.
- [x] `hookrelay migrate up|down|status|version`.

**Done when:** migrations apply cleanly to an empty DB, and the admin CLI produces a usable key.

> Status: complete. All PostgreSQL integration tests (migrations up/down/up, queries, constraints, tenant isolation, admin CLI) passed on a Neon test branch.

## Phase 2 — API: endpoints and ingest (~4–5 days)

- [x] Chi router, JSON error helpers, and auth middleware (hash lookup, `last_used_at` update throttled to once a minute).
- [x] Endpoint CRUD with URL validation (https in production) and SSRF checks (`internal/ssrf`), secret generation bound to the endpoint ID, and the reveal-secret route.
- [x] `internal/ingest`: a single transaction that inserts the event and fans out deliveries (D1, D5).
- [x] Idempotency: `ON CONFLICT DO NOTHING`, replay response, and `409` on payload mismatch (§6.4).
- [x] Payload limits (256 KB, must be a JSON object) and `event_type` validation (`^[a-zA-Z0-9._-]{1,100}$`).
- [x] API reference in `docs/API.md`.
- [x] Integration tests against real Postgres (run on a Neon test branch):
  - fan-out matches event-type filters, including the empty-filter-means-all case
  - concurrent identical `Idempotency-Key` requests produce exactly one event
  - disabled and deleted endpoints receive no deliveries

**Done when:** events can be posted with curl and the expected `pending` delivery rows appear.

> Status: complete. 87 tests passed against a Neon `integration-tests` branch; `TestReadyzIntegration` skipped because no Redis is available. A curl smoke test of the real binary passed.

## Phase 3 — Delivery core (~4–5 days)

- [x] `internal/signing`: Standard Webhooks sign/verify, tested against published test vectors.
- [x] `internal/delivery/sender`: shared `http.Client`, 10 s timeout, no redirects, SSRF-checking `Dialer.Control`, response body truncated to 4 KB, error classification (`timeout`, `connection`, `dns`, `tls`, `http_status`, `ssrf_blocked`).
- [x] Asynq `deliver` handler: **claim**, then send, then **record** (D3, D4), all fenced on `status` and `attempt_count`.
- [x] `internal/backoff`: exponential backoff with full jitter and `Retry-After` support. Unit tests use a seeded RNG.
- [x] `cmd/mockreceiver`: verifies signatures and lets you set `?fail_rate=`, `?latency_ms=` and `?status=`. Logs `webhook-id` so duplicates can be counted.
- [x] Failure path (pulled forward from Phase 4): `scheduled` with `next_attempt_at` from the DB clock, or `dead` once `max_attempts` is reached or the failure is permanent.
- [x] `hookrelay worker` (Asynq server, configurable concurrency, graceful shutdown of handlers) and `hookrelay admin enqueue-delivery` for manual hand-off.

**Done when:** manually enqueuing a delivery task makes a signed request reach mockreceiver, and the outcome is recorded.

> Status: complete. Delivery integration tests (success, 500, timeout, Retry-After, last attempt goes dead, retry then succeed, DNS rebinding, duplicate tasks, stale-worker fencing, foreign or stale tasks, endpoint disabled after dispatch, undecryptable secret, and two tests through real Redis/Asynq) pass against the Neon `integration-tests` branch and local Redis, also under `-race` (in a Linux container). Smoke test of the real binaries: admin enqueue, then the worker, then mockreceiver verified the signature, then `succeeded`. Design notes are in ARCHITECTURE §3.5. The backoff base changed from 30 s to 5 m to match the documented ~24 h window (§3.2).

## Phase 4 — Dispatcher, retries, reaper (~3–4 days)

- [x] Dispatcher loop: `SKIP LOCKED` batch moved to `queued` and committed, **then** enqueued (no Redis call inside a transaction); release on enqueue failure; `ErrTaskIDConflict` is success. Task IDs are unique per dispatch (ARCHITECTURE §3.6).
- [x] Reaper loop: stale `queued` (past the dispatch lease) goes to `scheduled`; expired `in_flight` gets a `worker_lost` attempt record and the normal retry policy (`scheduled` or `dead`).
- [x] Graceful shutdown for the worker (§6.5): stop the dispatcher and reaper loops before `asynq.Server.Shutdown()`.
- [x] Process roles: `worker` (handlers + dispatcher + reaper), `dispatcher`, `reaper`.
- [x] **End-to-end reliability tests.** These are the heart of the project:
  - [x] happy path: event, then delivered, then `succeeded`
  - [x] receiver returns 500 three times and then 200: four attempts, ending `succeeded` (Phase 3 handler test)
  - [x] receiver always fails: ends `dead` after `max_attempts` (use a tiny backoff base in tests)
  - [x] Redis down during dispatch: events are delivered after Redis recovers (simulated outage of the enqueue path, plus a real unreachable Redis)
  - [x] Redis flushed while tasks are queued: the reaper recovers them
  - [x] worker killed mid-delivery: the reaper recovers it and it ends `succeeded`
  - [x] two worker processes: no delivery is marked succeeded twice, and every attempt row is unique

**Done when:** all of the scenarios above pass in CI. **This completes the backend MVP.**

> Status: complete. Dispatcher, reaper and full-pipeline tests (real Postgres on the Neon `integration-tests` branch, real
> Redis/Asynq, mock receivers) pass, also under `-race`. Smoke test: `hookrelay worker` delivered API-ingested events with
> no manual step; SIGTERM shut it down cleanly (exit 0). Design: ARCHITECTURE §3.6.

## Phase 5 — Read APIs and operations (~2–3 days)

- [ ] List and get for events, deliveries (with attempts) and endpoints, with cursor pagination and filters.
- [ ] `POST /v1/deliveries/{id}/retry` and `POST /v1/endpoints/{id}/test`.
- [ ] `GET /v1/stats` (counts by status, success rate, p50/p95, hourly buckets).
- [ ] `/healthz` and `/readyz`.
- [ ] Cancel pending and scheduled deliveries when an endpoint is deleted.

## Phase 6 — Dashboard (~5–7 days)

- [ ] Next.js App Router, TypeScript, Tailwind and shadcn/ui in `web/`.
- [ ] Typed API client used **only server-side**. `HOOKRELAY_API_URL` and `HOOKRELAY_API_KEY` come from server env.
- [ ] Simple access gate for the single operator, e.g. HTTP basic auth in Next.js middleware via env credentials. A hosted auth provider (e.g. Neon Auth) comes post-MVP.
- [ ] Pages: Overview (stats), Endpoints (list, create, edit, reveal secret, send test), Events (list, detail), Deliveries (list with filters, detail with attempt timeline, retry button).
- [ ] Use polling or `router.refresh()` for freshness. No websockets in the MVP.

## Phase 7 — Performance and hardening (~3–4 days)

- [ ] k6 scripts in `loadtest/`: ingest throughput, end-to-end delivery latency (mockreceiver records receive time), and mixed success/failure load.
- [ ] Record results in `docs/BENCHMARKS.md`: hardware, config, events/s, deliveries/s, p50/p95/p99, duplicate rate.
- [ ] Tune pool sizes, Asynq concurrency and dispatcher batch size and interval based on what the measurements show.
- [ ] `EXPLAIN ANALYZE` the dispatcher, reaper and stats queries with about 1M delivery rows.

## Phase 8 — Deployment (~2–3 days)

- [ ] Multi-stage Dockerfile for the Go binary (distroless/static) and one for `web/`.
- [ ] CI: build and push images on `main`. Run migrations as a deploy step (`hookrelay migrate up`).
- [ ] Deploy target: any container host that can run two long-lived processes (for example Fly.io, Render or Railway). Use a managed Redis (for example Upstash or the host's Redis) and Neon Postgres: pooled `DATABASE_URL` for the app, `DATABASE_URL_UNPOOLED` for `hookrelay migrate` (see Pitfall 10). `neon deploy` applies `neon.ts` and pulls both URLs.
- [ ] Production config checklist: encryption key set, `ALLOW_PRIVATE_TARGETS=false`, HTTPS-only endpoints.

**Total: roughly 6–8 weeks part-time.**

---

## Post-MVP Backlog (rough priority order)

1. Automatically disable endpoints after N consecutive failures or X days of failing, and notify via the dashboard.
2. Per-endpoint concurrency and rate limits, to stop slow receivers from starving the pool.
3. Signing-secret rotation with dual signatures during an overlap window.
4. Retention job: delete or archive attempts and payloads older than N days. Partition `delivery_attempts` by month if needed.
5. Dashboard auth with a hosted provider (e.g. Neon Auth), plus project membership.
6. Prometheus `/metrics` (queue depth, attempt latency histogram, outcomes).
7. LISTEN/NOTIFY wake-up for the dispatcher, to cut dispatch latency below the poll interval.
8. Idempotency key expiry, an event type registry, payload schemas.
9. Bulk replay (for example "retry every dead delivery for endpoint X since T").
10. Client SDKs and signature-verification helpers (Node, Go, Python).
