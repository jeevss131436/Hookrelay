-- Initial HookRelay schema. See docs/ARCHITECTURE.md §4 for the design.
--
-- Tenant isolation: every tenant-owned table carries project_id. Child rows
-- reference their parents through composite (project_id, id) foreign keys,
-- so the database itself rejects, say, a delivery whose event belongs to a
-- different project than its endpoint.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TABLE projects (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 100),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id   uuid NOT NULL REFERENCES projects (id),
  name         text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 100),
  -- Non-secret leading characters of the key, for display ("hr_AbCd1234").
  prefix       text NOT NULL CHECK (char_length(prefix) BETWEEN 4 AND 16),
  -- SHA-256 of the full key. The plaintext key is never stored.
  key_hash     bytea NOT NULL CHECK (octet_length(key_hash) = 32),
  created_at   timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz,
  revoked_at   timestamptz,
  CONSTRAINT api_keys_key_hash_key UNIQUE (key_hash)
);
CREATE INDEX api_keys_project_idx ON api_keys (project_id, created_at DESC);

CREATE TABLE endpoints (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id        uuid NOT NULL REFERENCES projects (id),
  url               text NOT NULL CHECK (char_length(url) BETWEEN 1 AND 2048),
  description       text NOT NULL DEFAULT '' CHECK (char_length(description) <= 1024),
  -- Empty array = subscribed to all event types.
  event_types       text[] NOT NULL DEFAULT '{}',
  -- AES-256-GCM ciphertext of the signing secret (internal/secrets).
  secret_ciphertext bytea NOT NULL,
  enabled           boolean NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  deleted_at        timestamptz,
  -- Target for composite foreign keys from deliveries.
  CONSTRAINT endpoints_project_id_id_key UNIQUE (project_id, id)
);
CREATE INDEX endpoints_project_active_idx ON endpoints (project_id, created_at DESC)
  WHERE deleted_at IS NULL;
CREATE TRIGGER endpoints_set_updated_at BEFORE UPDATE ON endpoints
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE events (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id      uuid NOT NULL REFERENCES projects (id),
  event_type      text NOT NULL CHECK (event_type ~ '^[a-zA-Z0-9._-]{1,100}$'),
  -- Exact JSON bytes as received; signed and sent verbatim (ARCHITECTURE D5).
  payload         bytea NOT NULL CHECK (octet_length(payload) <= 262144),
  payload_sha256  bytea NOT NULL CHECK (octet_length(payload_sha256) = 32),
  idempotency_key text CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
  created_at      timestamptz NOT NULL DEFAULT now(),
  -- NULL keys never conflict, so events without a key are unaffected.
  CONSTRAINT events_project_idempotency_key UNIQUE (project_id, idempotency_key),
  CONSTRAINT events_project_id_id_key UNIQUE (project_id, id)
);
CREATE INDEX events_project_created_idx ON events (project_id, created_at DESC, id DESC);

CREATE TYPE delivery_status AS ENUM (
  'pending', 'queued', 'in_flight', 'scheduled', 'succeeded', 'dead', 'cancelled'
);

CREATE TABLE deliveries (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id       uuid NOT NULL REFERENCES projects (id),
  event_id         uuid NOT NULL,
  endpoint_id      uuid NOT NULL,
  status           delivery_status NOT NULL DEFAULT 'pending',
  attempt_count    integer NOT NULL DEFAULT 0,
  max_attempts     integer NOT NULL DEFAULT 10,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  queued_at        timestamptz,
  locked_until     timestamptz,
  last_status_code integer,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  completed_at     timestamptz,
  CONSTRAINT deliveries_event_fk FOREIGN KEY (project_id, event_id)
    REFERENCES events (project_id, id),
  CONSTRAINT deliveries_endpoint_fk FOREIGN KEY (project_id, endpoint_id)
    REFERENCES endpoints (project_id, id),
  -- Fan-out is idempotent: one delivery per (event, endpoint).
  CONSTRAINT deliveries_event_endpoint_key UNIQUE (event_id, endpoint_id),
  CONSTRAINT deliveries_project_id_id_key UNIQUE (project_id, id),
  CONSTRAINT deliveries_attempts_check
    CHECK (max_attempts > 0 AND attempt_count >= 0 AND attempt_count <= max_attempts),
  -- Terminal states, and only terminal states, carry completed_at.
  CONSTRAINT deliveries_completed_at_check
    CHECK ((status IN ('succeeded', 'dead', 'cancelled')) = (completed_at IS NOT NULL))
);
-- Dispatcher hot path: only rows that can become due.
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at)
  WHERE status IN ('pending', 'scheduled');
-- Reaper: stale queued / expired in-flight rows.
CREATE INDEX deliveries_stuck_idx ON deliveries (status, updated_at)
  WHERE status IN ('queued', 'in_flight');
-- Dashboard listings (cursor pagination on created_at, id).
CREATE INDEX deliveries_project_created_idx ON deliveries (project_id, created_at DESC, id DESC);
CREATE INDEX deliveries_endpoint_created_idx ON deliveries (endpoint_id, created_at DESC, id DESC);
-- (event_id lookups use the deliveries_event_endpoint_key index.)
CREATE TRIGGER deliveries_set_updated_at BEFORE UPDATE ON deliveries
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE delivery_attempts (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id      uuid NOT NULL,
  delivery_id     uuid NOT NULL,
  attempt_number  integer NOT NULL CHECK (attempt_number > 0),
  started_at      timestamptz NOT NULL,
  finished_at     timestamptz NOT NULL,
  duration_ms     integer NOT NULL CHECK (duration_ms >= 0),
  -- NULL when no HTTP response was received (timeout, connection error...).
  response_status integer CHECK (response_status BETWEEN 100 AND 599),
  -- Truncated to 4 KB by the worker.
  response_body   text CHECK (octet_length(response_body) <= 4096),
  error_kind      text CHECK (error_kind IN (
    'timeout', 'connection', 'dns', 'tls', 'http_status', 'ssrf_blocked', 'worker_lost'
  )),
  error_message   text,
  CONSTRAINT delivery_attempts_delivery_fk FOREIGN KEY (project_id, delivery_id)
    REFERENCES deliveries (project_id, id),
  -- Recording an attempt is idempotent.
  CONSTRAINT delivery_attempts_delivery_attempt_key UNIQUE (delivery_id, attempt_number),
  CONSTRAINT delivery_attempts_finished_check CHECK (finished_at >= started_at)
);
-- Per-project stats over a time window (GET /v1/stats).
CREATE INDEX delivery_attempts_project_finished_idx ON delivery_attempts (project_id, finished_at);

-- +goose Down
DROP TABLE delivery_attempts;
DROP TABLE deliveries;
DROP TYPE delivery_status;
DROP TABLE events;
DROP TABLE endpoints;
DROP TABLE api_keys;
DROP TABLE projects;
DROP FUNCTION set_updated_at();
