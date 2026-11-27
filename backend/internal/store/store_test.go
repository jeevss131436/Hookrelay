package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
	"github.com/jeevss131436/Hookrelay/backend/internal/testutil/pgtest"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	return store.New(pgtest.NewMigrated(t).Pool)
}

func mustProject(t *testing.T, st *store.Store, name string) sqlcgen.Project {
	t.Helper()
	p, err := st.CreateProject(context.Background(), name)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	return p
}

func mustKey(t *testing.T, st *store.Store, projectID pgtype.UUID) (sqlcgen.CreateAPIKeyRow, secrets.APIKey) {
	t.Helper()
	k := secrets.GenerateAPIKey()
	row, err := st.CreateAPIKey(context.Background(), sqlcgen.CreateAPIKeyParams{
		ProjectID: projectID, Name: "test", Prefix: k.Prefix, KeyHash: k.Hash,
	})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	return row, k
}

// mustEndpoint and mustEvent insert rows with raw SQL: their sqlc queries
// arrive in Phase 2, but constraint tests need the rows now.
func mustEndpoint(t *testing.T, st *store.Store, projectID pgtype.UUID) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	err := st.Pool().QueryRow(context.Background(),
		`INSERT INTO endpoints (project_id, url, secret_ciphertext) VALUES ($1, 'https://example.com/hook', '\x01') RETURNING id`,
		projectID).Scan(&id)
	if err != nil {
		t.Fatalf("insert endpoint: %v", err)
	}
	return id
}

func mustEvent(t *testing.T, st *store.Store, projectID pgtype.UUID, idemKey *string) pgtype.UUID {
	t.Helper()
	id, err := insertEvent(st, projectID, "user.created", idemKey)
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}
	return id
}

func insertEvent(st *store.Store, projectID pgtype.UUID, eventType string, idemKey *string) (pgtype.UUID, error) {
	payload := []byte(`{"id":1}`)
	sum := sha256.Sum256(payload)
	var id pgtype.UUID
	err := st.Pool().QueryRow(context.Background(),
		`INSERT INTO events (project_id, event_type, payload, payload_sha256, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		projectID, eventType, payload, sum[:], idemKey).Scan(&id)
	return id, err
}

func insertDelivery(st *store.Store, projectID, eventID, endpointID pgtype.UUID) (pgtype.UUID, error) {
	var id pgtype.UUID
	err := st.Pool().QueryRow(context.Background(),
		`INSERT INTO deliveries (project_id, event_id, endpoint_id) VALUES ($1, $2, $3) RETURNING id`,
		projectID, eventID, endpointID).Scan(&id)
	return id, err
}

func countRows(t *testing.T, st *store.Store, table string) int {
	t.Helper()
	var n int
	if err := st.Pool().QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// ---- Transactions ----

func TestInTxCommits(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	err := st.InTx(ctx, func(q *sqlcgen.Queries) error {
		p, err := q.CreateProject(ctx, "a")
		if err != nil {
			return err
		}
		k := secrets.GenerateAPIKey()
		_, err = q.CreateAPIKey(ctx, sqlcgen.CreateAPIKeyParams{ProjectID: p.ID, Name: "k", Prefix: k.Prefix, KeyHash: k.Hash})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRows(t, st, "projects") != 1 || countRows(t, st, "api_keys") != 1 {
		t.Error("committed rows missing")
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	sentinel := errors.New("boom")
	err := st.InTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.CreateProject(ctx, "rolled-back"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if n := countRows(t, st, "projects"); n != 0 {
		t.Errorf("projects = %d after rollback, want 0", n)
	}
}

func TestInTxRollsBackOnDatabaseError(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	err := st.InTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.CreateProject(ctx, "partial"); err != nil {
			return err
		}
		// Violates the projects.name CHECK constraint.
		_, err := q.CreateProject(ctx, "")
		return err
	})
	if !store.IsCheckViolation(err, "") {
		t.Fatalf("err = %v, want check violation", err)
	}
	if n := countRows(t, st, "projects"); n != 0 {
		t.Errorf("projects = %d, want 0 (first insert must roll back too)", n)
	}
}

func TestInTxRollsBackOnPanic(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic was swallowed")
			}
		}()
		_ = st.InTx(ctx, func(q *sqlcgen.Queries) error {
			if _, err := q.CreateProject(ctx, "panicky"); err != nil {
				return err
			}
			panic("boom")
		})
	}()
	if n := countRows(t, st, "projects"); n != 0 {
		t.Errorf("projects = %d after panic, want 0", n)
	}
}

// ---- Projects and API keys ----

func TestProjectCreateAndGet(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	p := mustProject(t, st, "Acme")
	if !p.ID.Valid || p.Name != "Acme" || p.CreatedAt.IsZero() {
		t.Fatalf("unexpected project: %+v", p)
	}
	got, err := st.GetProject(ctx, p.ID)
	if err != nil || got.ID != p.ID || got.Name != "Acme" {
		t.Fatalf("GetProject = %+v, %v", got, err)
	}
	missing, _ := store.ParseUUID("00000000-0000-4000-8000-000000000000")
	if _, err := st.GetProject(ctx, missing); !store.IsNotFound(err) {
		t.Errorf("missing project err = %v, want not found", err)
	}
}

func TestAPIKeyLookupByHash(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	p := mustProject(t, st, "Acme")
	row, key := mustKey(t, st, p.ID)

	hash, err := secrets.HashAPIKey(key.Plaintext)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.GetActiveAPIKeyByHash(ctx, hash)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.ID != row.ID || got.ProjectID != p.ID || got.Prefix != key.Prefix {
		t.Errorf("lookup returned %+v", got)
	}

	other := secrets.GenerateAPIKey()
	if _, err := st.GetActiveAPIKeyByHash(ctx, other.Hash); !store.IsNotFound(err) {
		t.Errorf("unknown key err = %v, want not found", err)
	}
}

func TestAPIKeyPlaintextNeverStored(t *testing.T) {
	st := newStore(t)
	p := mustProject(t, st, "Acme")
	_, key := mustKey(t, st, p.ID)

	var n int
	err := st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM api_keys
		 WHERE name = $1 OR prefix = $1 OR key_hash = convert_to($1, 'UTF8')
		    OR position(convert_to($1, 'UTF8') IN key_hash) > 0`,
		key.Plaintext).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("plaintext API key found in api_keys")
	}
	var stored []byte
	if err := st.Pool().QueryRow(context.Background(), `SELECT key_hash FROM api_keys`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, key.Hash) {
		t.Error("stored hash does not match SHA-256 of the key")
	}
}

func TestAPIKeyHashIsUnique(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	a, b := mustProject(t, st, "a"), mustProject(t, st, "b")
	_, key := mustKey(t, st, a.ID)
	_, err := st.CreateAPIKey(ctx, sqlcgen.CreateAPIKeyParams{ProjectID: b.ID, Name: "dup", Prefix: key.Prefix, KeyHash: key.Hash})
	if !store.IsUniqueViolation(err, "api_keys_key_hash_key") {
		t.Errorf("err = %v, want api_keys_key_hash_key violation", err)
	}
}

func TestAPIKeyRequiresExistingProject(t *testing.T) {
	st := newStore(t)
	missing, _ := store.ParseUUID("00000000-0000-4000-8000-000000000000")
	k := secrets.GenerateAPIKey()
	_, err := st.CreateAPIKey(context.Background(), sqlcgen.CreateAPIKeyParams{ProjectID: missing, Name: "x", Prefix: k.Prefix, KeyHash: k.Hash})
	if !store.IsForeignKeyViolation(err, "") {
		t.Errorf("err = %v, want foreign key violation", err)
	}
}

// ---- Tenant scoping ----

func TestAPIKeyQueriesAreTenantScoped(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	a, b := mustProject(t, st, "a"), mustProject(t, st, "b")
	keyA, _ := mustKey(t, st, a.ID)
	mustKey(t, st, a.ID)
	keyB, keyBSecret := mustKey(t, st, b.ID)

	listA, err := st.ListAPIKeys(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(listA) != 2 {
		t.Fatalf("project A sees %d keys, want 2", len(listA))
	}
	for _, k := range listA {
		if k.ProjectID != a.ID || k.ID == keyB.ID {
			t.Errorf("project A listing leaked key %+v", k)
		}
	}

	// Project A cannot revoke project B's key...
	n, err := st.RevokeAPIKey(ctx, sqlcgen.RevokeAPIKeyParams{ProjectID: a.ID, ID: keyB.ID})
	if err != nil || n != 0 {
		t.Fatalf("cross-tenant revoke affected %d rows (err %v), want 0", n, err)
	}
	if _, lookupErr := st.GetActiveAPIKeyByHash(ctx, keyBSecret.Hash); lookupErr != nil {
		t.Errorf("project B key no longer active after cross-tenant revoke attempt: %v", lookupErr)
	}

	// ...but can revoke its own, and revoked keys stop authenticating.
	n, err = st.RevokeAPIKey(ctx, sqlcgen.RevokeAPIKeyParams{ProjectID: a.ID, ID: keyA.ID})
	if err != nil || n != 1 {
		t.Fatalf("own revoke affected %d rows (err %v), want 1", n, err)
	}
	if n, _ := st.RevokeAPIKey(ctx, sqlcgen.RevokeAPIKeyParams{ProjectID: a.ID, ID: keyA.ID}); n != 0 {
		t.Errorf("second revoke affected %d rows, want 0", n)
	}
	listA, _ = st.ListAPIKeys(ctx, a.ID)
	for _, k := range listA {
		if k.ID == keyA.ID && k.RevokedAt == nil {
			t.Error("revoked key has no revoked_at")
		}
	}
}

func TestRevokedKeyDoesNotAuthenticate(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	p := mustProject(t, st, "a")
	row, key := mustKey(t, st, p.ID)
	if _, err := st.RevokeAPIKey(ctx, sqlcgen.RevokeAPIKeyParams{ProjectID: p.ID, ID: row.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetActiveAPIKeyByHash(ctx, key.Hash); !store.IsNotFound(err) {
		t.Errorf("revoked key lookup err = %v, want not found", err)
	}
}

func TestDeliveryCannotCrossTenants(t *testing.T) {
	st := newStore(t)
	a, b := mustProject(t, st, "a"), mustProject(t, st, "b")
	eventA := mustEvent(t, st, a.ID, nil)
	endpointA := mustEndpoint(t, st, a.ID)
	endpointB := mustEndpoint(t, st, b.ID)

	if _, err := insertDelivery(st, a.ID, eventA, endpointA); err != nil {
		t.Fatalf("same-tenant delivery rejected: %v", err)
	}
	// Event from A, endpoint from B.
	if _, err := insertDelivery(st, a.ID, eventA, endpointB); !store.IsForeignKeyViolation(err, "deliveries_endpoint_fk") {
		t.Errorf("A-event/B-endpoint err = %v, want deliveries_endpoint_fk violation", err)
	}
	// Claimed as B's delivery, but the event is A's.
	if _, err := insertDelivery(st, b.ID, eventA, endpointB); !store.IsForeignKeyViolation(err, "deliveries_event_fk") {
		t.Errorf("B-delivery/A-event err = %v, want deliveries_event_fk violation", err)
	}
}

func TestDeliveryAttemptCannotCrossTenants(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	a, b := mustProject(t, st, "a"), mustProject(t, st, "b")
	deliveryA, err := insertDelivery(st, a.ID, mustEvent(t, st, a.ID, nil), mustEndpoint(t, st, a.ID))
	if err != nil {
		t.Fatal(err)
	}
	insertAttempt := func(projectID pgtype.UUID, n int) error {
		_, err := st.Pool().Exec(ctx,
			`INSERT INTO delivery_attempts (project_id, delivery_id, attempt_number, started_at, finished_at, duration_ms, response_status)
			 VALUES ($1, $2, $3, now(), now(), 5, 200)`, projectID, deliveryA, n)
		return err
	}
	if err := insertAttempt(a.ID, 1); err != nil {
		t.Fatalf("same-tenant attempt rejected: %v", err)
	}
	if err := insertAttempt(b.ID, 2); !store.IsForeignKeyViolation(err, "delivery_attempts_delivery_fk") {
		t.Errorf("cross-tenant attempt err = %v, want FK violation", err)
	}
	if err := insertAttempt(a.ID, 1); !store.IsUniqueViolation(err, "delivery_attempts_delivery_attempt_key") {
		t.Errorf("duplicate attempt number err = %v, want unique violation", err)
	}
}

// ---- Other constraints ----

func TestIdempotencyKeyUniquePerProject(t *testing.T) {
	st := newStore(t)
	a, b := mustProject(t, st, "a"), mustProject(t, st, "b")
	key := "order-123"
	mustEvent(t, st, a.ID, &key)

	if _, err := insertEvent(st, a.ID, "user.created", &key); !store.IsUniqueViolation(err, "events_project_idempotency_key") {
		t.Errorf("same project duplicate key err = %v, want unique violation", err)
	}
	if _, err := insertEvent(st, b.ID, "user.created", &key); err != nil {
		t.Errorf("same key in another project rejected: %v", err)
	}
	// Events without a key never conflict.
	mustEvent(t, st, a.ID, nil)
	mustEvent(t, st, a.ID, nil)
}

func TestEventTypeValidated(t *testing.T) {
	st := newStore(t)
	p := mustProject(t, st, "a")
	for _, bad := range []string{"", "has space", "emoji-🙂", strings.Repeat("a", 101)} {
		if _, err := insertEvent(st, p.ID, bad, nil); !store.IsCheckViolation(err, "") {
			t.Errorf("event_type %q err = %v, want check violation", bad, err)
		}
	}
}

func TestFanOutIsIdempotent(t *testing.T) {
	st := newStore(t)
	p := mustProject(t, st, "a")
	ev, ep := mustEvent(t, st, p.ID, nil), mustEndpoint(t, st, p.ID)
	if _, err := insertDelivery(st, p.ID, ev, ep); err != nil {
		t.Fatal(err)
	}
	if _, err := insertDelivery(st, p.ID, ev, ep); !store.IsUniqueViolation(err, "deliveries_event_endpoint_key") {
		t.Errorf("duplicate delivery err = %v, want unique violation", err)
	}
}

func TestDeliveryStateConstraints(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	p := mustProject(t, st, "a")
	id, err := insertDelivery(st, p.ID, mustEvent(t, st, p.ID, nil), mustEndpoint(t, st, p.ID))
	if err != nil {
		t.Fatal(err)
	}

	var d sqlcgen.Delivery
	err = st.Pool().QueryRow(ctx, `SELECT status, attempt_count, max_attempts, created_at, updated_at FROM deliveries WHERE id = $1`, id).
		Scan(&d.Status, &d.AttemptCount, &d.MaxAttempts, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != sqlcgen.DeliveryStatusPending || d.AttemptCount != 0 || d.MaxAttempts != 10 {
		t.Errorf("unexpected defaults: %+v", d)
	}

	exec := func(sql string) error {
		_, err := st.Pool().Exec(ctx, sql, id)
		return err
	}
	if err := exec(`UPDATE deliveries SET status = 'bogus' WHERE id = $1`); err == nil {
		t.Error("invalid status accepted")
	}
	if err := exec(`UPDATE deliveries SET status = 'succeeded' WHERE id = $1`); !store.IsCheckViolation(err, "deliveries_completed_at_check") {
		t.Errorf("terminal status without completed_at err = %v", err)
	}
	if err := exec(`UPDATE deliveries SET completed_at = now() WHERE id = $1`); !store.IsCheckViolation(err, "deliveries_completed_at_check") {
		t.Errorf("completed_at on pending delivery err = %v", err)
	}
	if err := exec(`UPDATE deliveries SET attempt_count = 11 WHERE id = $1`); !store.IsCheckViolation(err, "deliveries_attempts_check") {
		t.Errorf("attempt_count > max_attempts err = %v", err)
	}
	if err := exec(`UPDATE deliveries SET status = 'succeeded', completed_at = now() WHERE id = $1`); err != nil {
		t.Errorf("valid terminal transition rejected: %v", err)
	}
}

func TestUpdatedAtTrigger(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	p := mustProject(t, st, "a")
	ep := mustEndpoint(t, st, p.ID)

	// Even an explicit attempt to set an old updated_at is overridden by the
	// trigger, which proves it fires on every UPDATE.
	var got time.Time
	err := st.Pool().QueryRow(ctx,
		`UPDATE endpoints SET enabled = false, updated_at = '2000-01-01' WHERE id = $1 RETURNING updated_at`, ep).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	if got.Year() == 2000 || time.Since(got) > time.Hour {
		t.Errorf("updated_at = %v, want ≈ now()", got)
	}
}
