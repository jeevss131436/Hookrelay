-- Ingest (transactional outbox): InsertEvent + FanOutDeliveries run in ONE
-- transaction. Nothing here touches Redis; the dispatcher (Phase 4) picks up
-- pending deliveries from Postgres.

-- Returns no row (pgx.ErrNoRows) when the idempotency key was already used in
-- this project; the caller then loads the existing event.
-- name: InsertEvent :one
INSERT INTO events (project_id, event_type, payload, payload_sha256, idempotency_key)
VALUES (@project_id, @event_type, @payload, @payload_sha256, sqlc.narg(idempotency_key))
ON CONFLICT ON CONSTRAINT events_project_idempotency_key DO NOTHING
RETURNING id, event_type, payload_sha256, idempotency_key, created_at;

-- name: GetEventByIdempotencyKey :one
SELECT id, event_type, payload_sha256, idempotency_key, created_at
FROM events
WHERE project_id = @project_id
  AND idempotency_key = @idempotency_key;

-- One pending delivery per enabled, non-deleted endpoint of the same project
-- whose filter matches (an empty filter matches every event type).
-- name: FanOutDeliveries :execrows
INSERT INTO deliveries (project_id, event_id, endpoint_id)
SELECT e.project_id, @event_id::uuid, e.id
FROM endpoints e
WHERE e.project_id = @project_id
  AND e.enabled
  AND e.deleted_at IS NULL
  AND (cardinality(e.event_types) = 0 OR @event_type::text = ANY (e.event_types))
ON CONFLICT ON CONSTRAINT deliveries_event_endpoint_key DO NOTHING;

-- name: CountDeliveriesForEvent :one
SELECT count(*) FROM deliveries
WHERE project_id = @project_id
  AND event_id = @event_id;
