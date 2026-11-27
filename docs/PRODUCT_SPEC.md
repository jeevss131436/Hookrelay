# HookRelay — Product Specification

## 1. Summary

HookRelay is developer-first webhook infrastructure. A developer's application sends
events to HookRelay over a REST API. HookRelay stores each event, works out which of the
developer's registered endpoints should receive it, and delivers it to them over HTTP.
Delivery is asynchronous, signed, retried automatically, and fully auditable.

The value proposition: *"Send us an event once. We guarantee it reaches every subscribed
endpoint at least once, and we show you exactly what happened on every attempt."*

## 2. Users

| User | Need |
|---|---|
| **Sender** (developer integrating HookRelay) | Fire-and-forget event submission with no lost events, without building retry and queue infrastructure themselves. |
| **Receiver** (owner of a webhook endpoint, often the same developer) | Verify that requests really come from HookRelay, and safely deduplicate retried deliveries. |
| **Operator** (the developer looking at the dashboard) | See what was sent, what failed, why it failed, and retry it by hand. |

## 3. Core Concepts

| Concept | Definition |
|---|---|
| **Project** | A tenant boundary. API keys, endpoints, events and deliveries all belong to one project. |
| **API key** | A secret bearer token scoped to a project. Stored only as a hash. |
| **Endpoint** | A destination URL plus a signing secret and an optional event-type filter. |
| **Event** | An immutable message submitted by the sender: `event_type` + JSON `payload`. |
| **Delivery** | One event going to one endpoint. An event matching N endpoints produces N deliveries. A delivery has a status and a retry schedule. |
| **Attempt** | One HTTP request made for a delivery: request time, response status, latency, error. |

## 4. MVP Functional Requirements

### 4.1 Event ingestion
- `POST /v1/events` accepts `event_type` (string) and `payload` (JSON object, ≤ 256 KB).
- The request is authenticated with a project API key.
- The response is `202 Accepted` once the event and its deliveries are durably committed to PostgreSQL.
- The request may carry an `Idempotency-Key` header. A repeat with the same key returns the original event and does not create new deliveries.
- Fan-out happens at ingest: one delivery is created for each enabled endpoint whose filter matches `event_type`.

### 4.2 Endpoint management
- Create, list, get, update and delete endpoints.
- Each endpoint has: `url` (HTTPS required in production), `description`, `event_types` (an empty list means all), and `enabled`.
- HookRelay generates the signing secret (`whsec_...`) when the endpoint is created. The secret can be retrieved later.
- Deleting an endpoint is a soft delete. Its delivery history stays, and its pending deliveries are cancelled.
- Endpoints can be disabled and re-enabled.

### 4.3 Delivery
- Each delivery is an HTTP `POST` of the exact payload bytes, with `Content-Type: application/json`.
- Delivery semantics are **at-least-once**. Receivers must deduplicate using `webhook-id`.
- Signature headers follow the Standard Webhooks convention:
  - `webhook-id`: the delivery ID. It stays the same across retries of the same delivery.
  - `webhook-timestamp`: Unix seconds at the moment of sending.
  - `webhook-signature`: `v1,<base64(HMAC-SHA256(secret, "{id}.{timestamp}.{body}"))>`
- A **2xx** response counts as success. Anything else, including timeouts, connection errors and redirects, is a failure.
- Request timeout: 10 seconds. Redirects are not followed.
- Failed deliveries are retried with exponential backoff and jitter, up to 10 attempts over roughly 24 hours. After that the delivery is marked `dead`.
- No ordering guarantee: events may arrive out of order. This is documented for receivers.

### 4.4 Observability and history
- Every attempt is recorded with: attempt number, timestamps, latency, response status code, a truncated response body (first 4 KB), and an error classification.
- Users can list events, deliveries and attempts, with filters for status, endpoint and event type.
- Users can manually retry a `dead` or `failed` delivery.
- A stats endpoint reports, for a time window: delivery counts by status, success rate, and p50/p95 latency.

### 4.5 Dashboard (Next.js)
- **Overview:** success rate, volume and latency for the last 24 hours, plus recent failures.
- **Endpoints:** list, create, edit, disable, reveal secret, and per-endpoint delivery health.
- **Events:** list, then a detail view with the payload and per-endpoint delivery status.
- **Deliveries:** list with filters, then a detail view with an attempt timeline and a manual retry button.

## 5. Non-Functional Requirements

| Area | Requirement |
|---|---|
| Durability | Once a `202` has been returned, the event is never lost. This must hold even if Redis is wiped or a worker crashes. |
| Delivery semantics | At-least-once. Duplicates are possible and are kept to a minimum. |
| Ingest latency | p95 < 50 ms for `POST /v1/events` (single DB transaction). |
| Dispatch latency | p95 < 2 s from ingest to the first attempt starting, under normal load. |
| Throughput target (MVP) | Sustain at least 200 events/s ingest and at least 500 deliveries/s on a single modest worker instance. This will be measured with k6. |
| Security | API keys and endpoint secrets are never stored in plaintext. SSRF protection stops deliveries to private or internal networks. |
| Operability | Health and readiness endpoints, structured JSON logs, and graceful shutdown with no lost in-flight work. |

## 6. Out of Scope for MVP

These are deliberately deferred. Each one is listed in `IMPLEMENTATION_PLAN.md` under post-MVP.

- Multi-user dashboard accounts, orgs and RBAC. The MVP dashboard is single-operator.
- Billing, quotas and per-project rate limits.
- Ordered delivery and FIFO per endpoint.
- Payload transformation and filtering by payload content.
- Signing-secret rotation with an overlap window.
- Automatic disabling of endpoints that keep failing.
- Per-endpoint concurrency and rate limits.
- Data retention and purge jobs.
- An event type registry and schemas.
- Inbound webhooks (receiving webhooks *from* third parties).
- SDKs. The MVP has a documented REST API and signature-verification examples only.

## 7. Success Criteria for MVP

1. In an end-to-end test, an event submitted while Redis is down is delivered after Redis comes back.
2. Killing a worker in the middle of a delivery leads to that delivery being retried and eventually succeeding. No delivery stays stuck forever.
3. A receiver that returns 500 three times and then 200 shows four attempts in the dashboard, with the backoff gaps between them visible.
4. Signature verification works using the documented algorithm and a small reference verifier.
5. Published k6 results show throughput and latency against the targets in §5.
