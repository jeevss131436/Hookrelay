# HookRelay REST API (v1)

This reference matches the implementation in `backend/internal/httpapi`. Implemented so far: **event
ingestion** and **endpoint management** (Phase 2). Webhook delivery, retries, and the read APIs for
events and deliveries arrive in Phases 3–5 (see `IMPLEMENTATION_PLAN.md`).

- Base URL: `http://localhost:8080` (local), path prefix `/v1`
- JSON in and out. Requests with a body must send `Content-Type: application/json`.
- Timestamps are RFC 3339 in UTC, e.g. `2026-11-27T05:41:57.763864Z`.
- IDs are UUIDs.
- Every response has an `X-Request-Id` header. Send your own (1–64 chars of `A-Za-z0-9._-`) to trace a request across systems.

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
| 400 | `invalid_parameter` | Bad `limit` or `cursor` query parameter |
| 400 | `invalid_header` | More than one `Idempotency-Key` header |
| 401 | `unauthorized` | Missing, malformed, unknown or revoked API key |
| 404 | `not_found` | Unknown route, or endpoint not found in your project |
| 405 | `method_not_allowed` | Wrong HTTP method for the route |
| 409 | `idempotency_key_reuse` | `Idempotency-Key` already used with a different `event_type` or payload |
| 409 | `endpoint_limit_reached` | Project already has 100 endpoints |
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
(`/readyz` then reports `redis: unavailable`, but `POST /v1/events` still returns `202`). In Phase 4 a
dispatcher will move pending deliveries from PostgreSQL to the Redis queue (transactional outbox,
`ARCHITECTURE.md` D1).

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
