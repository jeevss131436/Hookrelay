-- Delivery state transitions (docs/ARCHITECTURE.md section 3, D3).
--
-- Every transition is a conditional UPDATE on the expected status, and every
-- write after the claim is also fenced on attempt_count, so a worker that
-- lost its lease can never overwrite newer state. All times come from now()
-- (database clock, D8). Every query is scoped by project_id.
--
-- Durations are written "interval '1 millisecond' * @x::bigint": sqlc v1.31
-- mis-rewrites the operand order "@x::bigint * interval '...'".

-- Manual hand-off to the queue (admin enqueue-delivery), regardless of
-- next_attempt_at. The dispatcher does the same for due rows in batches
-- (ClaimDueDeliveriesForDispatch). queued_at identifies this dispatch: it is
-- part of the task ID and fences the worker's claim.
-- name: MarkDeliveryQueued :one
UPDATE deliveries
SET status    = 'queued',
    queued_at = now()
WHERE project_id = @project_id
  AND id = @id
  AND status IN ('pending', 'scheduled')
RETURNING attempt_count, queued_at::timestamptz AS queued_at;

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
  AND d.queued_at = @queued_at::timestamptz
  AND e.project_id = d.project_id
  AND e.id = d.endpoint_id
  AND (NOT e.enabled OR e.deleted_at IS NOT NULL);

-- The claim (D3): only the worker whose UPDATE matches 'queued' with the
-- attempt_count AND queued_at of the dispatch that produced its task may send.
-- Each dispatch therefore authorizes exactly one claim; duplicate or stale
-- tasks (from an earlier dispatch of the same attempt) match no row. The lease
-- bounds how long the claim is honoured before the reaper may recover it.
-- name: ClaimDelivery :one
UPDATE deliveries
SET status        = 'in_flight',
    attempt_count = attempt_count + 1,
    locked_until  = now() + interval '1 millisecond' * @lease_ms::bigint
WHERE project_id = @project_id
  AND id = @id
  AND status = 'queued'
  AND attempt_count = @expected_attempt_count
  AND queued_at = @queued_at::timestamptz
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

-- ---------------------------------------------------------------------------
-- Dispatcher and reaper (docs/ARCHITECTURE.md section 3.6).
--
-- These are system-wide maintenance queries and are deliberately NOT scoped by
-- project_id: they move rows between internal states and never return tenant
-- data to a caller. Every write is still conditional on the expected status.
-- ---------------------------------------------------------------------------

-- Dispatcher step 1 (its own short transaction): move up to batch_size due
-- deliveries to 'queued'. SKIP LOCKED lets several dispatchers run at once
-- without picking the same rows. All rows of one batch share queued_at (now()
-- is the transaction start time). The caller enqueues the tasks only AFTER
-- this commits: Redis is never touched inside a transaction.
-- name: ClaimDueDeliveriesForDispatch :many
WITH due AS (
  SELECT id
  FROM deliveries
  WHERE status IN ('pending', 'scheduled')
    AND next_attempt_at <= now()
  ORDER BY next_attempt_at
  LIMIT @batch_size
  FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d
SET status    = 'queued',
    queued_at = now()
FROM due
WHERE d.id = due.id
  AND d.status IN ('pending', 'scheduled')
RETURNING d.project_id, d.id, d.attempt_count, d.queued_at::timestamptz AS queued_at;

-- Dispatcher step 2 failed (Redis unavailable): give rows of this dispatch
-- back, to be retried after retry_delay_ms. Fenced on queued_at so a row that
-- was re-dispatched or claimed meanwhile is untouched.
-- name: ReleaseQueuedDeliveries :execrows
UPDATE deliveries
SET status          = 'scheduled',
    next_attempt_at = now() + interval '1 millisecond' * @retry_delay_ms::bigint
WHERE id = ANY (@ids::uuid[])
  AND status = 'queued'
  AND queued_at = @queued_at::timestamptz;

-- Reaper A: rows queued longer than the dispatch lease whose task never got
-- claimed (dispatcher crashed before enqueueing, release failed, or Redis lost
-- the task). They become due again immediately. No attempt was made, so
-- attempt_count is unchanged. A task that does still exist becomes stale:
-- the next dispatch gets a new queued_at, so the old task cannot claim.
-- (While queued, updated_at equals queued_at, which lets this use
-- deliveries_stuck_idx.)
-- name: RequeueStaleQueuedDeliveries :execrows
WITH stale AS (
  SELECT id
  FROM deliveries
  WHERE status = 'queued'
    AND updated_at < now() - interval '1 millisecond' * @stale_after_ms::bigint
  ORDER BY updated_at
  LIMIT @batch_size
  FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d
SET status          = 'scheduled',
    next_attempt_at = now()
FROM stale
WHERE d.id = stale.id
  AND d.status = 'queued';

-- Reaper B candidates: in-flight rows whose lease has expired. Read without
-- locks; each is then recovered in its own fenced transaction.
-- name: ListExpiredInFlightDeliveries :many
SELECT project_id, id, attempt_count
FROM deliveries
WHERE status = 'in_flight'
  AND locked_until < now()
ORDER BY locked_until
LIMIT @batch_size;

-- Reaper B, first statement of the recovery tx: record the abandoned attempt
-- as 'worker_lost'. started_at is the claim time (updated_at is set by the
-- claim and unchanged while in flight). Inserts nothing unless the row is
-- still in flight on that attempt with an expired lease, or if the attempt
-- was already recorded (by the worker, or by another reaper).
-- name: InsertWorkerLostAttempt :execrows
INSERT INTO delivery_attempts (
  project_id, delivery_id, attempt_number, started_at, finished_at, duration_ms,
  error_kind, error_message
)
SELECT d.project_id, d.id, d.attempt_count, d.updated_at, greatest(now(), d.updated_at),
       least(greatest(extract(epoch FROM now() - d.updated_at) * 1000, 0), 2147483647)::integer,
       'worker_lost', @error_message
FROM deliveries d
WHERE d.project_id = @project_id
  AND d.id = @id
  AND d.status = 'in_flight'
  AND d.attempt_count = @attempt_count
  AND d.locked_until < now()
ON CONFLICT ON CONSTRAINT delivery_attempts_delivery_attempt_key DO NOTHING;
