# HookRelay — Architecture

This document covers the system design, how data moves through it, the database schema,
the API, and the reasoning behind the main decisions. Decisions are explained *before*
the design that depends on them.

---

## 1. Key Architectural Decisions

### D1. PostgreSQL owns all delivery state. Redis is only a dispatch channel.

**Problem.** If the API writes an event to Postgres and then enqueues a job in Redis, the
two writes are not atomic. A crash between them loses the event, or a Redis failure leaves
it stranded. This is the classic dual-write problem.

**Decision.** Use a **transactional outbox**. The `deliveries` table itself serves as the
outbox. Ingest writes the event and its delivery rows (`status = 'pending'`) in **one
Postgres transaction**, and never touches Redis. A separate **dispatcher** loop reads
due deliveries from Postgres and enqueues them into Asynq.

**Consequence.** Redis can lose everything and nothing is permanently lost. A **reaper**
finds deliveries that were handed to Redis but never completed, and makes them eligible
for dispatch again.

### D2. Postgres also owns the retry schedule. Asynq does not retry.

**Problem.** Asynq has built-in retries. If both Asynq and Postgres track retry state, the
two will drift apart. For example, Asynq may think a task has 3 retries left while
Postgres thinks the delivery is dead. And if Redis is lost, the retry state goes with it.

**Decision.** Asynq tasks are enqueued with `MaxRetry(0)`. When an attempt fails, the
worker computes the next attempt time and writes `status = 'scheduled'` and
`next_attempt_at` to Postgres. The dispatcher re-enqueues the delivery when it is due.
Asynq is used for what it does well: a fast work queue, bounded worker concurrency,
graceful shutdown, and per-task timeouts.

> **Note on Redis/Asynq.** With D1 and D2, Postgres alone could serve as the queue via
> `FOR UPDATE SKIP LOCKED`. Asynq is kept because it is part of the chosen stack and it
> provides worker-pool management and low-latency hand-off for free. Its role is narrow,
> so it could be removed later without changing the data model.

### D3. Workers claim deliveries with conditional updates (fencing).

At-least-once dispatch means a worker may receive the same delivery twice: from a
duplicate enqueue, from the reaper re-dispatching it, or from Asynq redelivering after a
crash. Before doing any HTTP work, a worker **claims** the delivery atomically:

```sql
UPDATE deliveries
SET status = 'in_flight', attempt_count = attempt_count + 1,
    locked_until = now() + interval '60 seconds', updated_at = now()
WHERE id = $1 AND status = 'queued'
RETURNING attempt_count, ...;
```

If no row comes back, the task is stale and the worker acks it without doing anything.
When the worker records the result, it includes `WHERE status = 'in_flight' AND
attempt_count = $n`. That way a worker that was presumed dead and resumes later cannot
overwrite newer state.

### D4. Never hold a DB transaction open during an outbound HTTP call.

The sequence is: claim (a short tx), then HTTP (no tx), then record the result (a short
tx). Holding a transaction across a 10 s HTTP call would tie up connections and row locks
and would collapse throughput.

### D5. Store the payload as raw bytes and sign those exact bytes.

`jsonb` normalizes whitespace and key order. If we stored `jsonb` and re-serialized it,
the body we sign might not match what the sender submitted. The payload is validated as
JSON at ingest and stored as `bytea`. The same bytes are signed and sent.

### D6. One Go binary, two process roles.

`hookrelay api` and `hookrelay worker` are built from the same codebase and the same
Docker image. The worker process runs the Asynq server, the dispatcher loop and the
reaper loop. Running more than one worker replica is safe: the dispatcher uses
`SKIP LOCKED`, and every reaper update is conditional. Keeping one binary avoids
microservice overhead for a solo developer.

### D7. sqlc on top of pgx/v5 for data access. goose for migrations.

sqlc generates type-safe Go from hand-written SQL. This suits a system where the SQL
(locking, conditional updates, partial indexes) *is* the design. pgx/v5 is the driver
and pool. goose runs plain-SQL migrations and is the only additional tool.
Migrations are owned by the Go repo, not the Neon console.

### D8. Use database time for all scheduling.

All comparisons of `next_attempt_at`, `locked_until` and similar columns use `now()`
inside SQL, never the Go process clock. This avoids bugs from clock skew between
replicas.

---

## 2. System Overview

```
                    ┌──────────────────────┐
  Sender app ──────▶│   API  (Go + Chi)    │◀──── Next.js dashboard
  POST /v1/events   │  auth, validation,   │      (server-side fetch,
                    │  ingest + fan-out    │       API key never in browser)
                    └─────────┬────────────┘
                              │ 1 tx: INSERT event + deliveries(pending)
                              ▼
                    ┌──────────────────────┐
                    │ PostgreSQL (Neon)    │  ◀── source of truth
                    │ events, deliveries,  │
                    │ attempts, endpoints  │
                    └───▲─────────┬────────┘
       claim / record   │         │ poll due deliveries (SKIP LOCKED)
                        │         ▼
  ┌─────────────────────┴───────────────────────────────┐
  │                 Worker process (Go)                  │
  │  ┌────────────┐   enqueue   ┌───────┐   consume      │
  │  │ Dispatcher │ ──────────▶ │ Redis │ ──────────┐    │
  │  └────────────┘             │(Asynq)│           ▼    │
  │  ┌────────────┐             └───────┘   ┌──────────┐ │
  │  │  Reaper    │                         │ Delivery │ │──▶ Receiver endpoints
  │  └────────────┘                         │ handlers │ │    (signed HTTP POST)
  │                                         └──────────┘ │
  └──────────────────────────────────────────────────────┘
```

| Component | Responsibility |
|---|---|
| **API** | Authenticate API keys, validate input, run ingest with fan-out in one transaction, serve CRUD and read endpoints for the dashboard. Stateless and horizontally scalable. |
| **Dispatcher** | Every ~500 ms, select due `pending`/`scheduled` deliveries (`FOR UPDATE SKIP LOCKED`, batch of 500), enqueue each to Asynq, and mark them `queued`. |
| **Delivery handler** | An Asynq handler. It claims the delivery, loads the event and endpoint, signs the body, POSTs it, records the attempt, and moves the delivery to `succeeded`, `scheduled` or `dead`. |
| **Reaper** | Every ~30 s, rescue stuck deliveries. `queued` for too long means Redis lost the task. `in_flight` past `locked_until` means the worker died. Both are moved back to `scheduled`. |
| **Dashboard** | Next.js App Router. All calls to the Go API happen server-side (Server Components and Route Handlers), so the API key never reaches the browser. |

---

## 3. Event Lifecycle

### 3.1 Happy path

1. `POST /v1/events` arrives. The API key is verified (hash lookup).
2. **Ingest tx:**
   - `INSERT INTO events ... ON CONFLICT (project_id, idempotency_key) DO NOTHING`. If the insert conflicts, return the existing event (see §6.4).
   - `INSERT INTO deliveries (...) SELECT ... FROM endpoints WHERE project matches AND enabled AND deleted_at IS NULL AND (event_types = '{}' OR $type = ANY(event_types))`, with `status='pending'` and `next_attempt_at = now()`.
   - Commit, then return `202 {id, deliveries: n}`.
3. **Dispatcher tick** (short tx):
   ```sql
   SELECT id, attempt_count FROM deliveries
   WHERE status IN ('pending','scheduled') AND next_attempt_at <= now()
   ORDER BY next_attempt_at LIMIT 500
   FOR UPDATE SKIP LOCKED;
   ```
   For each row, enqueue the Asynq task `deliver` with `TaskID = "{delivery_id}:{attempt_count}"`,
   `MaxRetry(0)` and `Timeout(30s)`. Then `UPDATE ... SET status='queued', queued_at=now()`. Commit.
4. **Worker** picks up the task and claims it (D3), which sets `in_flight`, increments `attempt_count` and sets `locked_until`.
5. The worker loads the event payload and endpoint, decrypts the secret, signs, and POSTs with a 10 s timeout through the SSRF-safe client.
6. **Record tx:** insert a `delivery_attempts` row, then update the delivery to `succeeded` (fenced by status and attempt_count).

### 3.2 Failure and retry

At step 6 on a failure (non-2xx, timeout or network error):
- If `attempt_count < max_attempts`, set `status='scheduled'` and `next_attempt_at = now() + backoff(attempt_count)`.
- Otherwise set `status='dead'` and `completed_at = now()`.

Backoff uses exponential growth with full jitter:
`delay = random(0, min(cap, base * 2^(n-1)))`, with a floor of `base/2`. Here `base = 5m` and `cap = 6h`.
With 10 attempts the total span is roughly 12–24 hours. If the response is `429` or `503`
and includes a `Retry-After` header, the delay is `max(computed, Retry-After)`, capped at `cap`.

> **Changed in Phase 3: `base` is 5 minutes, not 30 seconds.** The original `base = 30s` contradicted
> the window stated here and in PRODUCT_SPEC §4.3 ("up to 10 attempts over roughly 24 hours"): with
> 30 s the nine delays' ceilings sum to only ~4.3 h. With 5 m the ceilings are 5, 10, 20, 40, 80, 160,
> 320 min, then 6 h, 6 h: ~22.6 h worst case and ~11.5 h expected. The product-level window was kept,
> and the constant changed. `internal/backoff` has a test that pins this.

### 3.3 Delivery state machine

```
            ingest
              │
              ▼
          ┌────────┐  dispatcher   ┌────────┐  worker claim  ┌───────────┐
          │pending │ ────────────▶ │ queued │ ─────────────▶ │ in_flight │
          └────────┘               └────────┘                └─────┬─────┘
              ▲                      │ reaper (stale)             │
              │                      ▼                       2xx  │  fail
              │                 ┌───────────┐ ◀───────────────────┤  (attempts left)
 manual retry │                 │ scheduled │                     │
              │                 └───────────┘ ◀── reaper (lock    │
              │                      │             expired)       │
              │                      └── dispatcher ──▶ queued    │
          ┌───┴──┐                                       ┌────────▼──┐
          │ dead │ ◀────── fail (no attempts left) ───── │ succeeded │
          └──────┘                                       └───────────┘
   (also: cancelled, when an endpoint is deleted or disabled while pending)
```

Every transition is a conditional `UPDATE ... WHERE status = <expected>`. No transition is unconditional.

### 3.4 Failure scenarios and recovery

| Failure | What happens | Recovery |
|---|---|---|
| API crashes before commit | The client gets an error or a timeout. Nothing is persisted. | The client retries with the same `Idempotency-Key`. |
| API crashes after commit, before responding | The event is persisted. The client sees an error. | The client retries with the same `Idempotency-Key` and gets the original event back. No duplicate is created. |
| Redis down during dispatch | The enqueue fails and the dispatcher tx rolls back. Rows stay `pending`/`scheduled`. | They are dispatched on the next tick after Redis recovers. |
| Enqueue succeeds but dispatcher commit fails | The task is in Redis while the row is still `pending`. | The worker's claim (`WHERE status='queued'`) finds nothing, so the task is dropped. The next tick re-enqueues it. A duplicate TaskID is treated as success. |
| Redis data lost after a task was queued | The row stays `queued` forever. | The reaper moves `queued` rows older than 5 min to `scheduled`. |
| Worker crashes mid-HTTP | The row is `in_flight` and its lock expires. | The reaper moves it to `scheduled`. The attempt counts as consumed, and the receiver may get a duplicate (at-least-once). |
| Postgres down | Ingest returns 503. The dispatcher and workers error out. Asynq tasks fail with MaxRetry 0. | The rows still hold the correct state, and the reaper and dispatcher resume once the DB is back. |
| Receiver slow or hanging | The 10 s client timeout fires. | The attempt is recorded as `timeout` and retried later. |

### 3.5 Delivery worker (implemented in Phase 3)

```
Asynq task "delivery:deliver" {project_id, delivery_id, attempt_count}
  │  TaskID "{delivery_id}:{attempt_count}", MaxRetry(0), Timeout(HOOKRELAY_TASK_TIMEOUT)
  ▼
claim tx ── CancelQueuedDeliveryForInactiveEndpoint (endpoint disabled/deleted → cancelled, nothing sent)
        └─ ClaimDelivery: queued → in_flight, attempt_count+1, locked_until = now()+lease
           WHERE project_id AND id AND status='queued' AND attempt_count = task's count
           (no row → duplicate/stale/foreign task: ack, nothing sent)
        └─ GetDeliveryTarget: stored payload bytes, endpoint URL, secret ciphertext
  ▼  (no transaction open)
decrypt secret (AAD = project+endpoint) → sign (Standard Webhooks) → POST via SSRF-guarded client
  ▼
record tx ── InsertDeliveryAttempt (ON CONFLICT DO NOTHING: history is kept even if fenced out)
          └─ 2xx:  MarkDeliverySucceeded   } both WHERE status='in_flight' AND attempt_count = n;
          └─ else: MarkDeliveryFailed      } zero rows = fenced out (a newer attempt owns the row)
                   → 'dead' if permanent or attempt_count >= max_attempts, else 'scheduled'
                     with next_attempt_at = now() + backoff delay (DB clock)
```

Implementation decisions:
- **The claim is also fenced on `attempt_count`.** The task carries the count it was enqueued with, so each enqueue
  authorizes at most one attempt. Tasks for another project match nothing, because every query is scoped by `project_id`.
- **Permanent failures go straight to `dead`:** a destination blocked by the SSRF policy (including after DNS
  rebinding) and an unparseable stored URL. Every HTTP status other than 2xx, plus timeouts and DNS, TLS and
  connection errors, is retried, as PRODUCT_SPEC §4.3 requires. 3xx responses are not followed and count as `http_status` failures.
- **Internal errors (database unreachable, secret undecryptable) are not recorded as attempts.** The handler returns an
  error and Asynq archives the task without retrying. If the claim had committed, the row stays `in_flight` until its lease
  expires, and the Phase 4 reaper then recovers it.
- **The record transaction is detached from the task context** (its own 10 s timeout), so an attempt that already happened
  is recorded even during shutdown or after the task deadline.
- **Attempt timestamps:** `started_at` comes from the Go clock and `finished_at = started_at + monotonic duration`, so the
  `finished_at >= started_at` CHECK holds. Scheduling comparisons still use only `now()` (D8).
- **Response bodies** keep the first 4096 bytes and are sanitized to valid UTF-8 with no NUL bytes, because the column
  is `text`. Up to 64 KB more is drained so the connection can be reused. Error messages drop the URL and are capped at 1 KB.
- **SSRF at delivery time:** the dialer resolves the host itself and rejects the whole answer if any address is
  disallowed. `net.Dialer.Control` then re-checks the literal IP:port the socket connects to. Proxies are disabled
  (a proxy would make the proxy the connected address), and redirects are not followed. One shared `http.Client` is used.
- **Worker startup** pings Postgres and Redis (verifying Redis credentials) and fails fast. Asynq's `ShutdownTimeout`
  is `HOOKRELAY_SHUTDOWN_TIMEOUT`, and concurrency is `HOOKRELAY_WORKER_CONCURRENCY` (default 50).
- **No dispatcher yet:** `hookrelay admin enqueue-delivery` hands one pending or scheduled delivery to the queue (mark
  `queued`, commit, then enqueue). It is for manual testing and operations. Phase 4 adds the dispatcher and reaper.

---

## 4. Database Schema

The authoritative schema is
[`backend/db/migrations/00001_initial_schema.sql`](../backend/db/migrations/00001_initial_schema.sql).
This section summarizes it and records the decisions behind it. If the two disagree, the
migration wins and this section should be fixed.

| Table | Purpose | Key constraints |
|---|---|---|
| `projects` | Tenant boundary | `name` 1–100 chars |
| `api_keys` | Project API keys (hash only) | `key_hash` exactly 32 bytes, `UNIQUE`; `prefix` is the non-secret display prefix |
| `endpoints` | Delivery targets | `UNIQUE (project_id, id)` (composite-FK target); `url` ≤ 2048; `secret_ciphertext` (AES-256-GCM); soft delete via `deleted_at` |
| `events` | Immutable ingested events | `payload bytea` ≤ 256 KB; `payload_sha256` 32 bytes; `event_type ~ '^[a-zA-Z0-9._-]{1,100}$'`; `UNIQUE (project_id, idempotency_key)`; `UNIQUE (project_id, id)` |
| `deliveries` | One event → one endpoint, with state and retry schedule | `delivery_status` enum; composite FKs to events and endpoints; `UNIQUE (event_id, endpoint_id)`; `attempt_count` between 0 and `max_attempts`; `completed_at` set **iff** the status is terminal |
| `delivery_attempts` | One row per HTTP attempt | composite FK `(project_id, delivery_id)`; `UNIQUE (delivery_id, attempt_number)`; `response_body` ≤ 4096 bytes; `error_kind` from a fixed list; `finished_at >= started_at` |

**Indexes**
- `deliveries_due_idx (next_attempt_at) WHERE status IN ('pending','scheduled')` is the dispatcher hot path.
- `deliveries_stuck_idx (status, updated_at) WHERE status IN ('queued','in_flight')` serves the reaper.
- `(project_id, created_at DESC, id DESC)` on `events` and `deliveries`, and `(endpoint_id, created_at DESC, id DESC)` on `deliveries`, support cursor-paginated listings.
- Lookups by `event_id` on `deliveries` use the `UNIQUE (event_id, endpoint_id)` index, so there is no separate index.
- `delivery_attempts (project_id, finished_at)` serves per-project stats windows.
- `api_keys (project_id, created_at DESC)` serves key listings.

**Decisions made when implementing (Phase 1)**
- **IDs are `gen_random_uuid()` (v4), not UUIDv7.** The schema was written for PostgreSQL 16, which has no native `uuidv7()`, and generating IDs in Go would add a dependency. Listings order by `created_at, id`, so v4 randomness does not affect pagination. Neon runs PG 18, which has `uuidv7()`, so a later migration could switch the defaults.
- **Tenant isolation is enforced by the database, not only by queries.** Child tables reference parents through **composite foreign keys** `(project_id, id)`. Inserting a delivery whose event and endpoint belong to different projects, or an attempt whose `project_id` differs from its delivery's, is rejected by PostgreSQL.
- **`delivery_attempts` gains `project_id`** (it was not in the original sketch). It enables the composite FK above and a per-project stats index, without joining `deliveries`.
- **`updated_at` is maintained by a trigger** (`set_updated_at()`) on `endpoints` and `deliveries`, so no query can forget it.
- **Every migration has a working `Down`.** `00001`'s down section drops everything it created, including the enum and trigger function. Tests run up → down → up.
- **Migrations run under a PostgreSQL advisory lock** (goose session locker), so two deploys cannot migrate concurrently. Migrations are embedded in the binary (`hookrelay migrate up`).

**Notes**
- `project_id` is copied onto `deliveries` and `delivery_attempts` so dashboard queries avoid joins, and so every query can be tenant-scoped.
- The one deliberately unscoped query is `GetActiveAPIKeyByHash`. It is how a request's `project_id` is determined in the first place.
- `UNIQUE (event_id, endpoint_id)` makes fan-out safe to retry.
- `UNIQUE (delivery_id, attempt_number)` makes recording an attempt idempotent. A fenced-out worker's insert either succeeds as a historical record or conflicts harmlessly.
- Response headers are not stored in the MVP, to limit storage. They can be added as `jsonb` later.
- Stats for p50/p95 come from `percentile_cont` over `delivery_attempts` in a time window. This is fine at MVP scale. Rollup tables come later if needed.

### 4.1 Secret storage

| Secret | Format | Stored as | Why |
|---|---|---|---|
| API key | `hr_` + 43 base62 chars (~256 bits) | `key_hash = SHA-256(key)` plus an 11-char display `prefix` | The key has ~256 bits of entropy, so a fast hash is safe: brute force is infeasible, and bcrypt/argon2 would only slow down every request. Lookup is by hash (unique index). |
| Endpoint signing secret | `whsec_` + base64(32 random bytes) | `secret_ciphertext = 0x01 ‖ nonce(12) ‖ AES-256-GCM(secret) ‖ tag(16)` | It must be reversible, because HookRelay signs with it. The leading version byte allows a key rotation scheme later. |
| Encryption key | base64 of 32 bytes in `HOOKRELAY_ENCRYPTION_KEY` | env only, never in the DB | Required at startup; all-zero and wrong-length keys are rejected. Losing it makes stored endpoint secrets unrecoverable. |

`secrets.Cipher.Encrypt` accepts associated data. Phase 2 should pass the endpoint ID, so a
ciphertext copied onto another endpoint row fails to decrypt. The types `secrets.Key` and
`secrets.APIKey` redact themselves in `fmt` and `slog` output. The admin CLI prints the plaintext
API key once, to stdout, and logs only its prefix.

---

## 5. REST API

> The implemented routes, with request/response examples and the exact error codes, are documented in
> [`API.md`](API.md), which is authoritative for anything already built. The tables below are the full MVP plan.
>
> Phase 2 implementation decisions:
> - Endpoint IDs are generated in Go (`store.NewUUID`) so the signing secret's AES-GCM ciphertext can be bound, as associated data, to `(project_id, endpoint_id)` before the INSERT.
> - Endpoint URLs pass `internal/ssrf` at registration, including a DNS check of every resolved address. Phase 3's dialer must re-check at connect time.
> - Malformed path IDs and other projects' endpoints return `404`, the same as missing ones.
> - Disabling or deleting an endpoint cancels its `pending`/`scheduled` deliveries in the same transaction.
> - The endpoint limit (100) is enforced under a `SELECT … FOR UPDATE` lock on the project row.
> - `last_used_at` is updated at most once per minute per key.

**Conventions**
- Base path `/v1`. JSON in and out. Auth uses `Authorization: Bearer hr_<random>`. Every query is scoped to the key's project.
- List endpoints use cursor pagination (`?limit=50&cursor=<opaque>`, ordered by `created_at DESC, id DESC`) and return `{data: [...], next_cursor}`.
- Errors look like `{"error": {"code": "validation_failed", "message": "...", "details": {...}}}`.
- Every response carries an `X-Request-Id` header, which also appears in logs.

### Events
| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/events` | Ingest. Body `{event_type, payload}`. Optional header `Idempotency-Key`. Returns `202 {id, event_type, created_at, deliveries_created}`. |
| `GET` | `/v1/events` | List. Filters: `event_type`, `created_after`, `created_before`. |
| `GET` | `/v1/events/{id}` | Event with payload and a summary of its deliveries (status per endpoint). |

### Endpoints
| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/endpoints` | Create `{url, description?, event_types?}`. Returns the endpoint plus `secret`. |
| `GET` | `/v1/endpoints` | List. |
| `GET` | `/v1/endpoints/{id}` | Get, including 24 h delivery health counts. |
| `PATCH` | `/v1/endpoints/{id}` | Update `url`, `description`, `event_types`, `enabled`. |
| `DELETE` | `/v1/endpoints/{id}` | Soft delete. Cancels `pending`/`scheduled` deliveries. |
| `GET` | `/v1/endpoints/{id}/secret` | Reveal the signing secret. |
| `POST` | `/v1/endpoints/{id}/test` | Ingest a synthetic `hookrelay.test` event targeting only this endpoint. |

### Deliveries
| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/deliveries` | List. Filters: `status`, `endpoint_id`, `event_id`. |
| `GET` | `/v1/deliveries/{id}` | Delivery plus all its attempts. |
| `POST` | `/v1/deliveries/{id}/retry` | Only from `dead`, `cancelled` or `succeeded` (redelivery). Resets to `pending` with `next_attempt_at=now()` and sets `max_attempts = attempt_count + 1`, which allows exactly one manual attempt. If that attempt fails, the delivery goes back to `dead` with no automatic retries. Returns `202`. |

### Stats and ops
| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/stats?window=24h` | Counts by status, success rate, p50/p95 latency, volume time series (hourly buckets). |
| `GET` | `/healthz` | Liveness, with no dependency checks. |
| `GET` | `/readyz` | Pings Postgres and Redis. |

**Admin (CLI, not HTTP).** Creating a project and its API keys is done with
`hookrelay admin create-project --name X` and `hookrelay admin create-key --project <id>`.
This avoids building a user and auth system for the MVP.

---

## 6. Concurrency and Correctness

### 6.1 Concurrency model
- **API:** Chi on `net/http`, one goroutine per request, and a shared `pgxpool` sized around 10–20 connections.
- **Worker:** Asynq server `Concurrency: N` (configurable, default 50). Delivery work is mostly I/O-bound, so N can be well above the CPU count. The DB pool must allow for `N` brief claim/record transactions, plus the dispatcher and reaper. Because transactions are short (D4), a pool of about 20 is enough for N = 50.
- **Dispatcher:** one loop per worker process. Multiple replicas cooperate through `SKIP LOCKED`.
- **Shared HTTP client:** one `http.Client` with a tuned `Transport` (`MaxIdleConnsPerHost`, `IdleConnTimeout`) and the SSRF-checking dialer. Never create a client per request.

### 6.2 Guarantees and how each is enforced
| Guarantee | Mechanism |
|---|---|
| No event loss after `202` | Event and deliveries are committed in a single transaction (outbox). |
| No stuck deliveries | Reaper and `locked_until` lease. |
| One worker writes the outcome of an attempt | Claim plus fenced result update (D3). |
| Idempotent fan-out | `UNIQUE (event_id, endpoint_id)`. |
| Idempotent ingest | `UNIQUE (project_id, idempotency_key)`. |
| Bounded duplicate sends | Duplicates only happen after a crash or a lease expiry. Receivers dedupe on `webhook-id`. |

### 6.3 Lease timing invariant
`locked_until` lease (60 s) **>** Asynq task timeout (30 s) **>** HTTP timeout (10 s) + DB time.
If the lease is shorter than the real work, the reaper will re-dispatch deliveries that
are still running, which produces duplicate sends. This invariant must be checked in config validation at startup.

### 6.4 Idempotency details
- Concurrent requests with the same key: both try the `INSERT ... ON CONFLICT DO NOTHING`. One wins. The loser selects the existing row and returns it with `200` (instead of `202`) and the header `Idempotent-Replayed: true`.
- Same key with a different payload: compare `payload_sha256` and `event_type`. On mismatch, return `409 idempotency_key_reuse`.
- Keys are kept indefinitely in the MVP. Expiring them after 24 h is post-MVP.

### 6.5 Graceful shutdown
On SIGTERM: the API stops accepting requests and drains (`http.Server.Shutdown`). The
worker stops the dispatcher and reaper loops, then `asynq.Server.Shutdown()` waits for
in-flight handlers up to the timeout. Anything cut off is left `in_flight` and is
recovered by the reaper after its lease expires. That is safe, just slower.

---

## 7. Security

- **API keys:** `hr_` plus 43 base62 characters (~256 bits). Only the SHA-256 is stored, and lookup is by hash. Since the attacker never sees stored hashes and the key space is 2^256, timing on the index lookup reveals nothing useful. Malformed keys are rejected before any DB call and must be answered with the same 401 as unknown keys. See §4.1.
- **Endpoint secrets:** `whsec_` plus base64 of 32 random bytes. Stored encrypted with AES-256-GCM using `HOOKRELAY_ENCRYPTION_KEY` from env. They must be reversible because HookRelay signs with them. See §4.1.
- **SSRF:** the delivery dialer resolves the host and **rejects private, loopback, link-local, CGNAT and metadata IPs** (`10/8`, `172.16/12`, `192.168/16`, `127/8`, `169.254/16`, `100.64/10`, `::1`, `fc00::/7`, `fe80::/10`). The check runs in `net.Dialer.Control`, so it applies to the IP actually connected to, which defeats DNS rebinding. Redirects are disabled. `HOOKRELAY_ALLOW_PRIVATE_TARGETS=true` exists for local development only.
- **Endpoint URLs:** `https` is required unless dev mode is on. Length limits are enforced.
- **Timestamp in signature:** lets receivers reject replays older than 5 minutes.

---

## 8. Architectural Pitfalls (and how this design avoids them)

1. **Dual write to DB and queue.** Avoided by the outbox (D1). Ingest never touches Redis.
2. **Two sources of truth for retries.** Avoided by D2: Asynq `MaxRetry(0)`, Postgres schedules retries.
3. **Signing re-serialized JSON.** Avoided by D5: raw bytes are stored, signed and sent.
4. **HTTP calls inside DB transactions.** Avoided by D4: claim, then send, then record.
5. **Lease shorter than work.** This causes duplicate sends. Enforce the §6.3 invariant at startup.
6. **Unconditional status updates.** These cause lost updates and resurrected deliveries. Every transition includes `WHERE status = ...`.
7. **Slow endpoints starving the worker pool.** One receiver that always times out can hold many worker slots for 10 s each. The MVP mitigates this with a short timeout and jittered backoff. Post-MVP: per-endpoint concurrency caps and automatic disabling.
8. **SSRF.** A webhook platform is effectively a "make HTTP requests to arbitrary URLs" service. See §7.
9. **Unbounded table growth.** `delivery_attempts` grows fastest. Response bodies are truncated, and retention or partitioning is planned post-MVP. Do not store full response headers or bodies.
10. **Neon connection pooling.** Neon provides two URLs. `DATABASE_URL` is pooled: a `-pooler` host running PgBouncer in *transaction mode*. `DATABASE_URL_UNPOOLED` is direct. The API, admin CLI and (later) worker queries use the pooled URL. Short transactions, including `FOR UPDATE SKIP LOCKED`, work there, as do pgx's protocol-level prepared statements (verified against Neon). Anything that needs **session state** must use the direct URL. That covers `hookrelay migrate`, because goose holds a session advisory lock, plus future `LISTEN/NOTIFY` wake-ups and any `SET`. Failures from using the pooled URL for these never mention pooling. Config exposes this as `Config.DatabaseURLUnpooled`, which falls back to `DATABASE_URL` for local Postgres. Neon computes also scale to zero, so expect a few hundred ms of extra latency on the first query after idle.
11. **Large fan-out in the ingest transaction.** An event matching thousands of endpoints creates thousands of rows in a single transaction. This is acceptable for the MVP. Cap the number of endpoints per project (e.g. 100) for now.
12. **Implicit ordering expectations.** Retries reorder events. Document that there is no ordering guarantee. Include the event `created_at` in the payload envelope or a header (`webhook-event-created-at`) so receivers can order events themselves.
13. **Dispatcher polling cost.** The partial index `deliveries_due_idx` keeps each poll cheap no matter how big the history grows. Measure it under k6 before optimizing (for example with LISTEN/NOTIFY).
14. **Thundering herd on recovery.** When a receiver comes back after an outage, all its scheduled retries fire together. Full jitter spreads them out.
15. **Overbuilding.** No Kafka, no microservices, no Kubernetes, no ORM, no event sourcing. Every component here has a concrete job.

---

## 9. Repository Layout

```
hookrelay/
├── CLAUDE.md
├── docs/
├── backend/
│   ├── cmd/
│   │   ├── hookrelay/          # main: subcommands api | worker | migrate | admin
│   │   └── mockreceiver/       # test receiver with configurable latency/failure rate
│   ├── internal/
│   │   ├── config/             # env parsing + invariant validation
│   │   ├── httpapi/            # chi router, handlers, middleware (auth, request id, logging)
│   │   ├── store/              # sqlc-generated code + tx helpers
│   │   ├── ingest/             # event ingest + fan-out service
│   │   ├── dispatch/           # dispatcher + reaper loops
│   │   ├── delivery/           # asynq handler, sender, ssrf dialer, attempt recording
│   │   ├── signing/            # Standard Webhooks HMAC sign/verify
│   │   ├── backoff/            # retry schedule
│   │   └── secrets/            # AES-GCM encrypt/decrypt, key generation
│   ├── db/
│   │   ├── migrations/         # goose SQL migrations
│   │   └── queries/            # sqlc .sql query files
│   ├── sqlc.yaml
│   └── Dockerfile
├── web/                        # Next.js + TS + Tailwind + shadcn/ui
├── loadtest/                   # k6 scripts
├── docker-compose.yml          # postgres, redis, mockreceiver (local dev)
└── .github/workflows/
```

Local development and CI use a plain `postgres:18` container (matching Neon's major version). Neon is used only for
deployed environments. The schema is plain Postgres, so this needs no special handling.
