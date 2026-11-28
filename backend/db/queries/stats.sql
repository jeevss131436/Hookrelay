-- Project statistics (GET /v1/stats; definitions in docs/API.md "Statistics").
--
-- Everything is aggregated in SQL; no row-level data is returned. Windows are
-- half-open [window_start, window_end). Events and deliveries are counted by
-- created_at (the cohort created in the window, each delivery in its CURRENT
-- status). Attempts are counted by finished_at.

-- name: GetEventStats :one
SELECT count(*) AS total
FROM events
WHERE project_id = @project_id
  AND created_at >= @window_start
  AND created_at < @window_end;

-- name: GetDeliveryStats :one
SELECT count(*)                                     AS total,
       count(*) FILTER (WHERE status = 'pending')   AS pending,
       count(*) FILTER (WHERE status = 'queued')    AS queued,
       count(*) FILTER (WHERE status = 'in_flight') AS in_flight,
       count(*) FILTER (WHERE status = 'scheduled') AS scheduled,
       count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE status = 'dead')      AS dead,
       count(*) FILTER (WHERE status = 'cancelled') AS cancelled
FROM deliveries
WHERE project_id = @project_id
  AND created_at >= @window_start
  AND created_at < @window_end;

-- A successful attempt has no error_kind: the worker records an error_kind for
-- every failure, including non-2xx responses. Latency is the attempt's
-- duration_ms (request start to response, timeout or error). worker_lost
-- attempts count as failed attempts but are excluded from failed-attempt
-- latency: their duration is the lease wait, not an HTTP round trip.
-- percentile_cont interpolates between neighbouring values. Latencies are 0
-- when their group is empty; the caller reports null based on the counts.
-- name: GetAttemptStats :one
SELECT count(*)                                          AS total,
       count(*) FILTER (WHERE error_kind IS NULL)        AS succeeded,
       count(*) FILTER (WHERE error_kind IS NOT NULL)    AS failed,
       coalesce((avg(duration_ms) FILTER (WHERE error_kind IS NULL))::float8, 0)::float8 AS success_avg_ms,
       coalesce((percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)
          FILTER (WHERE error_kind IS NULL))::float8, 0)::float8 AS success_p50_ms,
       coalesce((percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
          FILTER (WHERE error_kind IS NULL))::float8, 0)::float8 AS success_p95_ms,
       count(*) FILTER (WHERE error_kind <> 'worker_lost') AS failed_measured,
       coalesce((avg(duration_ms) FILTER (WHERE error_kind <> 'worker_lost'))::float8, 0)::float8 AS failed_avg_ms,
       coalesce((percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)
          FILTER (WHERE error_kind <> 'worker_lost'))::float8, 0)::float8 AS failed_p50_ms,
       coalesce((percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
          FILTER (WHERE error_kind <> 'worker_lost'))::float8, 0)::float8 AS failed_p95_ms
FROM delivery_attempts
WHERE project_id = @project_id
  AND finished_at >= @window_start
  AND finished_at < @window_end;
