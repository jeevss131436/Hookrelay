-- Delivery state transitions (docs/ARCHITECTURE.md section 3, D3).
--
-- Every transition is a conditional UPDATE on the expected status, and every
-- write after the claim is also fenced on attempt_count, so a worker that
-- lost its lease can never overwrite newer state. All times come from now()
-- (database clock, D8). Every query is scoped by project_id.
--
-- Durations are written "interval '1 millisecond' * @x::bigint": sqlc v1.31
-- mis-rewrites the operand order "@x::bigint * interval '...'".

-- Manual hand-off to the queue (admin enqueue-delivery). The Phase 4
-- dispatcher performs the same transition in batches.
-- name: MarkDeliveryQueued :one
UPDATE deliveries
SET status    = 'queued',
    queued_at = now()
WHERE project_id = @project_id
  AND id = @id
  AND status IN ('pending', 'scheduled')
RETURNING attempt_count;

-- A queued delivery whose endpoint was disabled or deleted after dispatch is
-- cancelled instead of sent. Runs in the claim transaction, before the claim.
-- name: CancelQueuedDeliveryForInactiveEndpoint :execrows
UPDATE deliveries d
SET status       = 'cancelled',
    completed_at = now()
FROM endpoints e
WHERE d.project_id = @project_id
  AND d.id = @id
  AND d.status = 'queued'
  AND d.attempt_count = @expected_attempt_count
  AND e.project_id = d.project_id
  AND e.id = d.endpoint_id
  AND (NOT e.enabled OR e.deleted_at IS NOT NULL);

-- The claim (D3): only the worker whose UPDATE matches 'queued' with the
-- attempt_count the task was enqueued with may send. Duplicate or stale tasks
-- match no row. The lease bounds how long the claim is honoured before the
-- reaper (Phase 4) may hand the delivery to another worker.
-- name: ClaimDelivery :one
UPDATE deliveries
SET status        = 'in_flight',
    attempt_count = attempt_count + 1,
    locked_until  = now() + interval '1 millisecond' * @lease_ms::bigint
WHERE project_id = @project_id
  AND id = @id
  AND status = 'queued'
  AND attempt_count = @expected_attempt_count
RETURNING attempt_count, max_attempts, event_id, endpoint_id;

-- What to send and where: the exact stored payload bytes (D5) and the
-- endpoint's URL and encrypted signing secret.
-- name: GetDeliveryTarget :one
SELECT ev.event_type, ev.payload, en.url, en.secret_ciphertext
FROM deliveries d
JOIN events ev ON ev.project_id = d.project_id AND ev.id = d.event_id
JOIN endpoints en ON en.project_id = d.project_id AND en.id = d.endpoint_id
WHERE d.project_id = @project_id
  AND d.id = @id;

-- Idempotent: a second insert for the same attempt (e.g. after a reaper
-- recorded worker_lost, or a fenced-out worker) is a harmless no-op.
-- name: InsertDeliveryAttempt :execrows
INSERT INTO delivery_attempts (
  project_id, delivery_id, attempt_number, started_at, finished_at, duration_ms,
  response_status, response_body, error_kind, error_message
) VALUES (
  @project_id, @delivery_id, @attempt_number, @started_at, @finished_at, @duration_ms,
  sqlc.narg(response_status), sqlc.narg(response_body), sqlc.narg(error_kind), sqlc.narg(error_message)
)
ON CONFLICT ON CONSTRAINT delivery_attempts_delivery_attempt_key DO NOTHING;

-- Fenced on status AND attempt_count: zero rows means this worker was fenced out.
-- name: MarkDeliverySucceeded :execrows
UPDATE deliveries
SET status           = 'succeeded',
    completed_at     = now(),
    locked_until     = NULL,
    last_status_code = @last_status_code,
    last_error       = NULL
WHERE project_id = @project_id
  AND id = @id
  AND status = 'in_flight'
  AND attempt_count = @attempt_count;

-- Failure: schedule a retry at now() + retry_delay_ms, or give up ('dead')
-- when the failure is permanent or no attempts are left. The attempts-left
-- decision uses the row's own max_attempts, so Postgres alone owns the retry
-- state (D2). Fenced like MarkDeliverySucceeded; no row means fenced out.
-- name: MarkDeliveryFailed :one
UPDATE deliveries
SET status = CASE WHEN @permanent::boolean OR attempt_count >= max_attempts
                  THEN 'dead'::delivery_status ELSE 'scheduled'::delivery_status END,
    next_attempt_at = CASE WHEN @permanent::boolean OR attempt_count >= max_attempts
                           THEN next_attempt_at
                           ELSE now() + interval '1 millisecond' * @retry_delay_ms::bigint END,
    completed_at = CASE WHEN @permanent::boolean OR attempt_count >= max_attempts THEN now() END,
    locked_until     = NULL,
    last_status_code = sqlc.narg(last_status_code),
    last_error       = @last_error
WHERE project_id = @project_id
  AND id = @id
  AND status = 'in_flight'
  AND attempt_count = @attempt_count
RETURNING status, next_attempt_at;

-- name: GetDelivery :one
SELECT * FROM deliveries
WHERE project_id = @project_id
  AND id = @id;

-- name: ListDeliveryAttempts :many
SELECT * FROM delivery_attempts
WHERE project_id = @project_id
  AND delivery_id = @delivery_id
ORDER BY attempt_number;
