-- Read API and manual retry (Phase 5; docs/API.md).
--
-- Every query is scoped by project_id. Optional filters are NULL when unset.
-- Listings use keyset pagination on (created_at DESC, id DESC) and a caller-
-- supplied row_limit (page size + 1), so no query is unbounded. No query here
-- selects endpoints.secret_ciphertext or api_keys.key_hash.

-- ---- Events ---------------------------------------------------------------

-- The payload is not listed (it may be 256 KB), only its size.
-- name: ListEvents :many
SELECT id, event_type, idempotency_key, octet_length(payload)::integer AS payload_size, created_at
FROM events
WHERE project_id = @project_id
  AND (sqlc.narg(event_type)::text IS NULL OR event_type = sqlc.narg(event_type)::text)
  AND (sqlc.narg(created_after)::timestamptz IS NULL OR created_at >= sqlc.narg(created_after)::timestamptz)
  AND (sqlc.narg(created_before)::timestamptz IS NULL OR created_at < sqlc.narg(created_before)::timestamptz)
  AND (sqlc.narg(cursor_created_at)::timestamptz IS NULL
       OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT @row_limit;

-- name: GetEvent :one
SELECT id, event_type, payload, idempotency_key, created_at
FROM events
WHERE project_id = @project_id
  AND id = @id;

-- Per-endpoint delivery summary for GET /v1/events/{id}. Bounded by
-- row_limit; the complete list is GET /v1/deliveries?event_id=.
-- name: ListEventDeliverySummaries :many
SELECT d.id, d.endpoint_id, en.url AS endpoint_url, d.status, d.attempt_count,
       d.last_status_code, d.created_at, d.updated_at, d.completed_at
FROM deliveries d
JOIN endpoints en ON en.project_id = d.project_id AND en.id = d.endpoint_id
WHERE d.project_id = @project_id
  AND d.event_id = @event_id
ORDER BY d.created_at, d.id
LIMIT @row_limit;

-- ---- Deliveries -----------------------------------------------------------

-- statuses is a text[] cast to the enum array, so the status index stays usable.
-- name: ListDeliveries :many
SELECT d.id, d.event_id, ev.event_type, d.endpoint_id, d.status, d.attempt_count,
       d.max_attempts, d.next_attempt_at, d.last_status_code, d.last_error,
       d.created_at, d.updated_at, d.completed_at
FROM deliveries d
JOIN events ev ON ev.project_id = d.project_id AND ev.id = d.event_id
WHERE d.project_id = @project_id
  AND (sqlc.narg(statuses)::text[] IS NULL OR d.status = ANY (sqlc.narg(statuses)::text[]::delivery_status[]))
  AND (sqlc.narg(endpoint_id)::uuid IS NULL OR d.endpoint_id = sqlc.narg(endpoint_id)::uuid)
  AND (sqlc.narg(event_id)::uuid IS NULL OR d.event_id = sqlc.narg(event_id)::uuid)
  AND (sqlc.narg(event_type)::text IS NULL OR ev.event_type = sqlc.narg(event_type)::text)
  AND (sqlc.narg(created_after)::timestamptz IS NULL OR d.created_at >= sqlc.narg(created_after)::timestamptz)
  AND (sqlc.narg(created_before)::timestamptz IS NULL OR d.created_at < sqlc.narg(created_before)::timestamptz)
  AND (sqlc.narg(cursor_created_at)::timestamptz IS NULL
       OR (d.created_at, d.id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::uuid))
ORDER BY d.created_at DESC, d.id DESC
LIMIT @row_limit;

-- Delivery detail with its event type and endpoint (which may be soft-deleted).
-- name: GetDeliveryDetail :one
SELECT d.id, d.event_id, ev.event_type, d.endpoint_id, en.url AS endpoint_url,
       en.enabled AS endpoint_enabled, (en.deleted_at IS NOT NULL)::boolean AS endpoint_deleted,
       d.status, d.attempt_count, d.max_attempts, d.next_attempt_at, d.last_status_code,
       d.last_error, d.created_at, d.updated_at, d.completed_at
FROM deliveries d
JOIN events ev ON ev.project_id = d.project_id AND ev.id = d.event_id
JOIN endpoints en ON en.project_id = d.project_id AND en.id = d.endpoint_id
WHERE d.project_id = @project_id
  AND d.id = @id;

-- Attempt history, oldest first, keyset on attempt_number.
-- name: ListDeliveryAttemptsPage :many
SELECT attempt_number, started_at, finished_at, duration_ms, response_status,
       response_body, error_kind, error_message
FROM delivery_attempts
WHERE project_id = @project_id
  AND delivery_id = @delivery_id
  AND attempt_number > @after_attempt::integer
ORDER BY attempt_number
LIMIT @row_limit;

-- Manual retry (POST /v1/deliveries/{id}/retry). Only a 'dead' delivery whose
-- endpoint is still enabled and not deleted can be retried. The conditional
-- UPDATE is the whole concurrency story: of N concurrent retries exactly one
-- matches status = 'dead'; the rest match nothing.
--
-- attempt_count is NOT reset. Attempt numbers keep growing, so history rows
-- (UNIQUE (delivery_id, attempt_number)) are preserved, and every fence that
-- compares attempt_count keeps rejecting workers of earlier attempts.
-- max_attempts = attempt_count + 1 grants exactly one more attempt; if it
-- fails, MarkDeliveryFailed sends the delivery straight back to 'dead'.
-- next_attempt_at = now() makes it due for the dispatcher at once (the API
-- never enqueues to Redis). queued_at/locked_until are cleared; the next
-- dispatch sets a fresh queued_at, so tasks of older dispatches cannot claim.
-- name: RetryDeadDelivery :one
UPDATE deliveries d
SET status          = 'pending',
    max_attempts    = d.attempt_count + 1,
    next_attempt_at = now(),
    completed_at    = NULL,
    queued_at       = NULL,
    locked_until    = NULL
FROM endpoints en
WHERE d.project_id = @project_id
  AND d.id = @id
  AND d.status = 'dead'
  AND en.project_id = d.project_id
  AND en.id = d.endpoint_id
  AND en.enabled
  AND en.deleted_at IS NULL
RETURNING d.id;
