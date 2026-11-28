# HookRelay REST API (v1)

This reference matches the implementation in `backend/internal/httpapi`. Implemented: **event
ingestion** and **endpoint management** (Phase 2), and the **read APIs** for events, deliveries and
attempts, **manual retry**, and **statistics** (Phase 5). Delivery itself (Phases 3–4) happens in the
worker; this API only reads its results.

- Base URL: `http://localhost:8080` (local), path prefix `/v1`
- JSON in and out. Requests with a body must send `Content-Type: application/json`.
- Timestamps are RFC 3339 in UTC, e.g. `2026-11-27T05:41:57.763864Z`.
- IDs are UUIDs.
- Every response has an `X-Request-Id` header. Send your own (1–64 chars of `A-Za-z0-9._-`) to trace a request across systems.
- **Lists** return `{"data": [...], "next_cursor": "…" | null}`. Pass `next_cursor` back as `?cursor=` for the next page;
  it is `null` on the last page. Cursors are opaque and stable: rows created after the first page do not shift later
  pages. Listings are newest first, ordered by `(created_at, id)` so items with equal timestamps never repeat or vanish
  between pages. Page size is `limit` (1–100, default 50).
- **Query parameters are strict.** An unknown parameter (e.g. a typo like `statsu=dead`), a repeated one, or a malformed
  query string is `400 invalid_parameter`, so a broken filter never silently returns unfiltered data.

## Authentication

Every `/v1` route requires a project API key:

```
Authorization: Bearer hr_<43 characters>
```

- Keys are created with the admin CLI: `hookrelay admin create-project --name "Acme"` (or `make create-project NAME="Acme"`), and `hookrelay admin create-key --project <id>`. The plaintext key is printed **once**; HookRelay stores only its SHA-256 hash.
- The key determines the project. Every operation is scoped to that project; there is no way to address another project's data. Endpoints of other projects behave exactly like missing ones (`404`).
- Missing, malformed, unknown and revoked keys all receive the same response:

```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer realm="hookrelay"

{"error":{"code":"unauthorized","message":"missing, invalid or revoked API key"}}
```

Keys are never logged or echoed. Access logs record `project_id` and `api_key_id`.

## Errors

All errors use one envelope. `details` appears only on validation errors and maps field names to problems.

```json
{"error": {"code": "validation_failed", "message": "request has invalid fields",
           "details": {"url": "url points to a private or reserved address"}}}
```

| Status | `code` | When |
|---|---|---|
| 400 | `invalid_json` | Body is empty, not valid JSON, not a single JSON object, or has an unknown field |
| 400 | `invalid_parameter` | Malformed query string; unknown or repeated query parameter; bad `limit`, `cursor`, filter or time value |
| 400 | `invalid_header` | More than one `Idempotency-Key` header |
| 401 | `unauthorized` | Missing, malformed, unknown or revoked API key |
| 404 | `not_found` | Unknown route, or the endpoint, event or delivery does not exist in your project |
| 405 | `method_not_allowed` | Wrong HTTP method for the route |
| 409 | `idempotency_key_reuse` | `Idempotency-Key` already used with a different `event_type` or payload |
| 409 | `endpoint_limit_reached` | Project already has 100 endpoints |
| 409 | `delivery_not_retryable` | Manual retry of a delivery that is not `dead` |
| 409 | `endpoint_inactive` | Manual retry of a dead delivery whose endpoint is disabled or deleted |
| 409 | `conflict` | The delivery changed state while a retry was evaluated; send the request again |
| 413 | `payload_too_large` | Event payload over 256 KB, or request body over the route limit |
| 415 | `unsupported_media_type` | `Content-Type` is not `application/json` |
| 422 | `validation_failed` | Field values are invalid; see `details` |
| 500 | `internal_error` | Unexpected server error (details are logged server-side, never returned) |

---

## Events

### `POST /v1/events`: ingest an event

Stores the event and creates one **pending delivery** per matching endpoint, in a single PostgreSQL
transaction. The response is sent only after that transaction commits, so a `202` means the event is durable.

**Request**

| Field | Type | Rules |
|---|---|---|
| `event_type` | string | Required. 1–100 characters of `a-z A-Z 0-9 . _ -` |
| `payload` | object | Required. A JSON **object**, at most 256 KB (262,144 bytes) as sent |

| Header | Rules |
|---|---|
| `Idempotency-Key` | Optional. 1–255 printable ASCII characters, no spaces. Scoped to your project. |

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Authorization: Bearer $HOOKRELAY_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-1001" \
  -d '{"event_type":"order.paid","payload":{"order_id":1001,"amount":4999}}'
```

**Response: `202 Accepted`** (new event)

```json
{
  "id": "912c8f2f-d90c-41b7-87df-7e80f463c95a",
  "event_type": "order.paid",
  "idempotency_key": "order-1001",
  "created_at": "2026-11-27T05:41:57.763864Z",
  "deliveries_created": 1
}
```

`idempotency_key` is `null` when none was sent. `deliveries_created` is the number of endpoints that will receive the event (it may be 0).

**Payload handling.** The payload bytes are stored **exactly as sent**, including whitespace and key
order. The same bytes will later be signed and delivered, so receivers can verify the signature over the raw body.

**Endpoint matching.** An endpoint receives the event when all of these hold:
- it belongs to the same project,
- it is enabled and not deleted,
- its `event_types` is empty (meaning all events) or contains `event_type` exactly (case-sensitive).

#### Idempotency

Clients should send an `Idempotency-Key` and retry with the **same key** after timeouts or network errors.

| Situation | Result |
|---|---|
| First request with a key | `202`; event and deliveries are created |
| Same key, same `event_type` and byte-identical `payload` | `200 OK` with header `Idempotent-Replayed: true` and the **original** event; nothing new is written |
| Same key, different `event_type` or `payload` | `409 idempotency_key_reuse`; nothing is written |
| Concurrent requests with the same key | Exactly one creates the event; the others get the replay response |
| No key | Every request creates a new event |
| Same key in a different project | Independent; keys are per project |
| The original request failed (e.g. `500`) | Nothing was stored, so a retry with the same key is processed as new |

Keys are currently kept indefinitely.

#### Ingestion workflow

```
POST /v1/events
  1. Authenticate the API key, which resolves the project.
  2. Decode the body (max 272 KB); check that payload ≤ 256 KB, event_type format, and Idempotency-Key format.
  3. BEGIN
       INSERT event ... ON CONFLICT (project_id, idempotency_key) DO NOTHING
         - conflict: load the existing event; 409 if it differs, otherwise replay (200).
       INSERT INTO deliveries SELECT matching endpoints     (status = pending, next_attempt_at = now())
     COMMIT
  4. Respond 202.
```

Ingestion writes **only to PostgreSQL** and never touches Redis. It keeps working while Redis is down
(`/readyz` then reports `redis: unavailable`, but `POST /v1/events` still returns `202`). The
dispatcher moves pending deliveries from PostgreSQL to the Redis queue (transactional outbox,
`ARCHITECTURE.md` D1).

### `GET /v1/events`: list events

Newest first (`created_at DESC, id DESC`), cursor-paginated. Payloads are not listed (only their size); fetch one event to see it.

| Query | Rules |
|---|---|
| `limit` | 1–100, default 50 |
| `cursor` | The `next_cursor` from the previous page |
| `event_type` | Exact event type (same format as on ingest) |
| `created_after` | RFC 3339 timestamp, **inclusive** |
| `created_before` | RFC 3339 timestamp, **exclusive**; must be later than `created_after` |

```bash
curl -s "http://localhost:8080/v1/events?event_type=order.paid&limit=2" -H "Authorization: Bearer $HOOKRELAY_API_KEY"
```

**`200 OK`**

```json
{
  "data": [
    {"id": "912c8f2f-d90c-41b7-87df-7e80f463c95a", "event_type": "order.paid",
     "idempotency_key": "order-1001", "payload_size": 37, "created_at": "2026-11-27T05:41:57.763864Z"}
  ],
  "next_cursor": "MjAyNi0xMS0yN1Qw…"
}
```

### `GET /v1/events/{id}`: retrieve an event

**`200 OK`** with the payload and the status of each of its deliveries:

```json
{
  "id": "912c8f2f-d90c-41b7-87df-7e80f463c95a",
  "event_type": "order.paid",
  "idempotency_key": "order-1001",
  "created_at": "2026-11-27T05:41:57.763864Z",
  "deliveries": [
    {"id": "5f1ef97d-8709-4b68-b219-f34ae35de20a", "endpoint_id": "03b5e05c-f9f7-4ecf-a61c-65130ab8dfe9",
     "endpoint_url": "https://example.com/webhooks", "status": "succeeded", "attempt_count": 1,
     "last_status_code": 200, "created_at": "2026-11-27T05:41:57.763864Z",
     "updated_at": "2026-11-27T05:41:58.104511Z", "completed_at": "2026-11-27T05:41:58.104511Z"}
  ],
  "deliveries_truncated": false,
  "payload": {"order_id":1001,"amount":4999}
}
```

- `payload` is the **exact stored bytes**, byte for byte as ingested (whitespace, key order and characters such as `<` and `&` are not re-encoded). These are the bytes that were signed and sent.
- `deliveries` lists at most 100 deliveries, oldest first. If there are more, `deliveries_truncated` is `true`; list them all with `GET /v1/deliveries?event_id={id}`.
- Unknown IDs, malformed IDs and other projects' events return `404 not_found`.

---

## Endpoints

Endpoint object (responses):

```json
{
  "id": "03b5e05c-f9f7-4ecf-a61c-65130ab8dfe9",
  "url": "https://example.com/webhooks",
  "description": "",
  "event_types": ["order.paid"],
  "enabled": true,
  "created_at": "2026-11-27T05:41:57.29281Z",
  "updated_at": "2026-11-27T05:41:57.29281Z"
}
```

The signing secret is never part of the endpoint object. It is returned once by `POST /v1/endpoints`
and afterwards only by `GET /v1/endpoints/{id}/secret`.

### URL rules and SSRF protection

URLs are validated on create and on every URL change:

- The scheme must be `https` (in production) or `http`/`https` (development and test).
- The URL must be absolute, at most 2,048 characters, with no credentials (`user:pass@`), no fragment, no whitespace, and a valid port.
- These hosts are rejected: `localhost`, single-label names (`intranet`), and private suffixes (`.local`, `.localhost`, `.internal`, `.lan`, `.home.arpa`, `.intranet`, `.corp`).
- IP literals and **every** address the hostname resolves to (DNS lookup at registration) must be public. These are rejected:
  - loopback, private (RFC 1918), and carrier-grade NAT
  - link-local, including the cloud metadata address `169.254.169.254`
  - multicast, reserved, and documentation ranges
  - IPv6 unique-local and link-local addresses, including `fd00:ec2::254`
  - NAT64 and 6to4 prefixes
  - IPv4-mapped IPv6 forms of all of the above
- Hosts that do not resolve are rejected.

Registration checks alone cannot stop DNS rebinding. Phase 3's delivery client re-checks the IP it
actually connects to and does not follow redirects. `HOOKRELAY_ALLOW_PRIVATE_TARGETS=true` turns off the
address checks for local development; the server refuses to start with it in production.

### `POST /v1/endpoints`: create

| Field | Type | Rules |
|---|---|---|
| `url` | string | Required. See URL rules |
| `description` | string | Optional, ≤ 1,024 characters |
| `event_types` | string[] | Optional, ≤ 50 entries, each in event-type format; duplicates removed. Empty or omitted = all events |
| `enabled` | boolean | Optional, default `true` |

```bash
curl -X POST http://localhost:8080/v1/endpoints \
  -H "Authorization: Bearer $HOOKRELAY_API_KEY" -H "Content-Type: application/json" \
  -d '{"url":"https://example.com/webhooks","event_types":["order.paid"]}'
```

**`201 Created`**, with a `Location: /v1/endpoints/{id}` header and `Cache-Control: no-store`. The body is the endpoint object plus `"secret": "whsec_…"`.

HookRelay generates the signing secret; clients cannot choose one. It is stored encrypted with
AES-256-GCM, and the ciphertext is bound to this project and endpoint ID, so a ciphertext copied onto
another endpoint row fails to decrypt.

### `GET /v1/endpoints`: list

Newest first, cursor-paginated. Deleted endpoints are excluded.

| Query | Rules |
|---|---|
| `limit` | 1–100, default 50 |
| `cursor` | The `next_cursor` from the previous page |

**`200 OK`**:

```json
{"data": [ { …endpoint… } ], "next_cursor": "MjAyNi0xMS0yN1Qw…"}
```

`next_cursor` is `null` on the last page.

### `GET /v1/endpoints/{id}`: retrieve

**`200 OK`** with the endpoint object, or `404`.

### `PATCH /v1/endpoints/{id}`: update

Send only the fields to change: `url`, `description`, `event_types`, `enabled`.
- Omitted or `null` fields are unchanged.
- `"event_types": []` subscribes the endpoint to all events.
- A new `url` is fully re-validated, including SSRF checks.

```bash
curl -X PATCH http://localhost:8080/v1/endpoints/$ENDPOINT_ID \
  -H "Authorization: Bearer $HOOKRELAY_API_KEY" -H "Content-Type: application/json" \
  -d '{"enabled":false}'
```

**`200 OK`** with the updated endpoint. Setting `"enabled": false` also **cancels** the endpoint's
deliveries that are still `pending` or `scheduled`, in the same transaction. Deliveries already handed
to a worker (`queued`/`in_flight`) are left to the worker, which re-checks the endpoint before sending
(Phase 3). Re-enabling does not resurrect cancelled deliveries; only new events are delivered.

### `DELETE /v1/endpoints/{id}`: delete

**`204 No Content`.** This is a soft delete: the endpoint disappears from the API, but the row and its
delivery history are kept. Its `pending`/`scheduled` deliveries are cancelled in the same transaction.
A second delete returns `404`.

### `GET /v1/endpoints/{id}/secret`: reveal signing secret

**`200 OK`** with `Cache-Control: no-store`:

```json
{"secret": "whsec_GSvB8m3Q0…"}
```

Returns `500 internal_error` if the stored ciphertext fails authentication (wrong encryption key, tampering, or a ciphertext moved from another endpoint).

---

## Deliveries

A delivery is one event going to one endpoint. Delivery object:

```json
{
  "id": "5f1ef97d-8709-4b68-b219-f34ae35de20a",
  "event_id": "912c8f2f-d90c-41b7-87df-7e80f463c95a",
  "event_type": "order.paid",
  "endpoint_id": "03b5e05c-f9f7-4ecf-a61c-65130ab8dfe9",
  "status": "scheduled",
  "attempt_count": 2,
  "max_attempts": 10,
  "next_attempt_at": "2026-11-27T05:52:13.004211Z",
  "last_status_code": 503,
  "last_error": "HTTP 503",
  "created_at": "2026-11-27T05:41:57.763864Z",
  "updated_at": "2026-11-27T05:44:01.912402Z",
  "completed_at": null
}
```

| Field | Meaning |
|---|---|
| `status` | `pending` (waiting for the dispatcher), `queued` (handed to the queue), `in_flight` (a worker is sending it), `scheduled` (waiting for a retry), `succeeded`, `dead` (gave up), `cancelled` (endpoint disabled or deleted) |
| `attempt_count` | Attempts made so far |
| `max_attempts` | Attempts allowed before the delivery goes `dead` (10 by default; each manual retry raises it by one) |
| `next_attempt_at` | When the next attempt becomes due. Only set while `pending` or `scheduled`, otherwise `null` |
| `last_status_code`, `last_error` | Outcome of the most recent attempt (`null` if none, or no HTTP response) |
| `completed_at` | Set when the delivery reached `succeeded`, `dead` or `cancelled` |

### `GET /v1/deliveries`: list deliveries

Newest first (`created_at DESC, id DESC`), cursor-paginated. All filters combine with AND.

| Query | Rules |
|---|---|
| `limit` | 1–100, default 50 |
| `cursor` | The `next_cursor` from the previous page |
| `status` | One status, or a comma-separated list (`status=dead,scheduled`) |
| `endpoint_id` | UUID of an endpoint (deleted endpoints included) |
| `event_id` | UUID of an event |
| `event_type` | Exact event type of the delivery's event |
| `created_after` | RFC 3339, **inclusive** |
| `created_before` | RFC 3339, **exclusive** |

```bash
curl -s "http://localhost:8080/v1/deliveries?status=dead&limit=20" -H "Authorization: Bearer $HOOKRELAY_API_KEY"
```

**`200 OK`**: `{"data": [ { …delivery… } ], "next_cursor": "…"}`. IDs of other projects' endpoints or events simply match nothing.

### `GET /v1/deliveries/{id}`: retrieve a delivery

**`200 OK`** with the delivery object plus:

```json
{
  "…": "all delivery fields",
  "endpoint": {"id": "03b5e05c-…", "url": "https://example.com/webhooks", "enabled": true, "deleted": false},
  "retryable": false
}
```

`retryable` is `true` when `POST /v1/deliveries/{id}/retry` would currently be accepted. The endpoint is shown even if it
was deleted later (`"deleted": true`). Its signing secret is never included.

### `GET /v1/deliveries/{id}/attempts`: attempt history

Oldest first (by `attempt_number`), cursor-paginated (`limit` 1–100, default 50; `cursor`). `404` if the delivery does not exist in your project.

```json
{
  "data": [
    {"attempt_number": 1, "started_at": "2026-11-27T05:41:58.004Z", "finished_at": "2026-11-27T05:41:58.130Z",
     "duration_ms": 126, "succeeded": false, "response_status": 503, "response_body": "upstream unavailable",
     "error_kind": "http_status", "error_message": "HTTP 503"},
    {"attempt_number": 2, "started_at": "2026-11-27T05:44:01.800Z", "finished_at": "2026-11-27T05:44:11.800Z",
     "duration_ms": 10000, "succeeded": false, "response_status": null, "response_body": null,
     "error_kind": "timeout", "error_message": "Client.Timeout exceeded while awaiting headers"}
  ],
  "next_cursor": null
}
```

| Field | Meaning |
|---|---|
| `succeeded` | The receiver answered 2xx |
| `duration_ms` | From sending the request to the response, timeout or error |
| `response_status`, `response_body` | The receiver's HTTP status and the first 4 KB of its body; `null` if no response arrived |
| `error_kind` | `null` on success, else `http_status`, `timeout`, `connection`, `dns`, `tls`, `ssrf_blocked`, or `worker_lost` (the worker died mid-attempt and the reaper recovered the delivery) |
| `error_message` | Human-readable detail (at most 1 KB; never contains the endpoint URL) |

Attempt numbers are never reused or reset, including across manual retries.

### `POST /v1/deliveries/{id}/retry`: manual retry

Gives a **dead** delivery exactly one more attempt. No request body.

```bash
curl -s -X POST http://localhost:8080/v1/deliveries/$DELIVERY_ID/retry -H "Authorization: Bearer $HOOKRELAY_API_KEY"
```

**`202 Accepted`** with the delivery detail in its new state:

```json
{"id": "5f1ef97d-…", "status": "pending", "attempt_count": 10, "max_attempts": 11,
 "next_attempt_at": "2026-11-27T06:10:00.100Z", "completed_at": null, "retryable": false, "…": "…"}
```

**Eligibility.** Only `dead` deliveries whose endpoint is still enabled and not deleted can be retried.

| Current state | Response |
|---|---|
| `dead`, endpoint enabled | `202`; the delivery becomes `pending` and is due immediately |
| `pending`, `queued`, `in_flight`, `scheduled` | `409 delivery_not_retryable` (it is still being worked on) |
| `succeeded` | `409 delivery_not_retryable` (it was delivered; HookRelay does not resend successes) |
| `cancelled` | `409 delivery_not_retryable` (it was cancelled because its endpoint was disabled or deleted) |
| `dead`, endpoint disabled or deleted | `409 endpoint_inactive`; re-enable the endpoint first |
| Unknown ID, malformed ID, other project | `404 not_found` |

**What a retry does**, in one conditional `UPDATE … WHERE status = 'dead'`:
- `status = 'pending'`, `next_attempt_at = now()`, `completed_at = null`.
- `max_attempts = attempt_count + 1`: exactly **one** more attempt. If it fails, the delivery goes straight back to `dead`
  with no automatic retries. Retry again to try once more.
- `attempt_count` is **not** reset and no attempt rows are touched, so the history is preserved and the next attempt gets the
  next attempt number.
- Nothing is sent to Redis by the API. The dispatcher picks the delivery up within its polling interval, and it goes through
  the normal worker pipeline (signed with the endpoint's current secret, same `webhook-id`).

**Concurrency.** Of several simultaneous retries of the same delivery, exactly one gets `202`; the others get `409`
because the delivery is no longer `dead`. A rare `409 conflict` means the delivery changed state while the request was
being evaluated; send it again.

---

## Statistics

### `GET /v1/stats`: project statistics

Aggregates over a time window `[from, to)` (from inclusive, to exclusive).

| Query | Rules |
|---|---|
| `from` | RFC 3339. Default: 24 hours before `to` |
| `to` | RFC 3339. Default: now |

`from` must be earlier than `to`, and the window may be at most 90 days.

```bash
curl -s "http://localhost:8080/v1/stats?from=2026-11-26T00:00:00Z&to=2026-11-27T00:00:00Z" \
  -H "Authorization: Bearer $HOOKRELAY_API_KEY"
```

**`200 OK`**

```json
{
  "from": "2026-11-26T00:00:00Z",
  "to": "2026-11-27T00:00:00Z",
  "events": {"total": 1200},
  "deliveries": {
    "total": 2400, "pending": 0, "queued": 2, "in_flight": 3, "scheduled": 15,
    "succeeded": 2350, "dead": 25, "cancelled": 5,
    "success_rate": 0.9894736842105263, "failure_rate": 0.010526315789473684
  },
  "attempts": {"total": 2510, "succeeded": 2350, "failed": 160, "success_rate": 0.9362549800796812},
  "latency_ms": {
    "successful_attempts": {"count": 2350, "avg": 84.2, "p50": 61, "p95": 240.5},
    "failed_attempts": {"count": 158, "avg": 3120.7, "p50": 450, "p95": 10000}
  }
}
```

**Definitions**

| Metric | Definition |
|---|---|
| `events.total` | Events whose `created_at` is in the window |
| `deliveries.*` | Deliveries whose `created_at` is in the window, counted by their **current** status. `total` is the sum of the seven statuses. A delivery created in the window but still retrying shows as `scheduled`, even if its later attempts fall outside the window. |
| `deliveries.success_rate` | `succeeded / (succeeded + dead)`: the outcome of deliveries that have **finished**. In-progress and `cancelled` deliveries are excluded. `null` when no delivery in the window has finished. |
| `deliveries.failure_rate` | `dead / (succeeded + dead)`. `success_rate + failure_rate = 1` when both are non-null. |
| `attempts.*` | Attempts whose `finished_at` is in the window. `succeeded` = the receiver answered 2xx; `failed` = everything else, including `worker_lost`. `success_rate = succeeded / total`, `null` when there are no attempts. |
| `latency_ms.successful_attempts` | `duration_ms` of successful attempts in the window: `count`, `avg`, `p50`, `p95`. This is the receiver's response time as seen by HookRelay. |
| `latency_ms.failed_attempts` | The same for failed attempts, **excluding `worker_lost`** (their duration is how long the lease ran, not an HTTP round trip). Timeouts are included, so this is typically dominated by the 10 s timeout. |

- Successful and failed latencies are never mixed: a fast `500` and a slow success are reported in different groups.
- Percentiles use PostgreSQL `percentile_cont`, which interpolates between neighbouring values (the p50 of 10 and 20 is 15).
- `avg`, `p50` and `p95` are `null` when their `count` is 0. Rates are fractions between 0 and 1, not percentages.
- All three groups are computed in one read-only `REPEATABLE READ` transaction, so they describe one consistent snapshot.
- End-to-end latency (ingest to successful delivery) and time-series buckets are not reported yet.

---

## Health

| Route | Auth | Response |
|---|---|---|
| `GET /healthz` | none | `200 {"status":"ok"}` when the process is serving |
| `GET /readyz` | none | `200` if PostgreSQL and Redis respond, else `503` with per-check status, e.g. `{"status":"unavailable","checks":{"postgres":"ok","redis":"unavailable"}}` |

## Try it locally

```bash
make migrate
make create-project NAME="Demo"            # copy the printed hr_… key
export HOOKRELAY_API_KEY=hr_...
make run-api

curl -s -X POST localhost:8080/v1/endpoints -H "Authorization: Bearer $HOOKRELAY_API_KEY" \
  -H "Content-Type: application/json" -d '{"url":"https://example.com/webhooks"}'
curl -s -X POST localhost:8080/v1/events -H "Authorization: Bearer $HOOKRELAY_API_KEY" \
  -H "Content-Type: application/json" -H "Idempotency-Key: demo-1" \
  -d '{"event_type":"user.created","payload":{"id":1}}'
```

During local development against a local receiver (e.g. `http://localhost:9000/hook`), set
`HOOKRELAY_ALLOW_PRIVATE_TARGETS=true` in `.env`.
