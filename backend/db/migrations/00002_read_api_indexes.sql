-- Indexes for the Phase 5 read APIs (docs/ARCHITECTURE.md §4).
--
-- deliveries_project_status_created_idx: GET /v1/deliveries?status=dead lists
--   the newest deliveries in one state. Without it, a rare status (dead) is
--   found by walking the project's whole (created_at, id) index.
-- events_project_type_created_idx: GET /v1/events?event_type=X, same reason.
--
-- Both follow the (created_at DESC, id DESC) keyset order of the listings.
-- Plain CREATE INDEX (not CONCURRENTLY): goose runs this in a transaction and
-- the tables are small when this ships. A large production table would need a
-- NO TRANSACTION migration with CONCURRENTLY instead.

-- +goose Up
CREATE INDEX deliveries_project_status_created_idx
  ON deliveries (project_id, status, created_at DESC, id DESC);
CREATE INDEX events_project_type_created_idx
  ON events (project_id, event_type, created_at DESC, id DESC);

-- +goose Down
DROP INDEX events_project_type_created_idx;
DROP INDEX deliveries_project_status_created_idx;
