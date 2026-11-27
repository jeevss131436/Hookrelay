package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/ingest"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
	"github.com/jeevss131436/Hookrelay/backend/internal/testutil/pgtest"
)

// ---- harness ----

type fakeResolver map[string]string

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	ip, ok := f[host]
	if !ok {
		return nil, errors.New("no such host")
	}
	return []netip.Addr{netip.MustParseAddr(ip)}, nil
}

var testResolver = fakeResolver{
	"hooks.example.com":    "93.184.215.14",
	"a.example.com":        "93.184.215.14",
	"b.example.com":        "93.184.215.14",
	"internal.example.com": "10.0.0.7",
}

type apiEnv struct {
	t      *testing.T
	db     pgtest.DB
	st     *store.Store
	h      http.Handler
	logBuf *syncBuffer
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newAPIEnv wires the real router, services and a fresh migrated database.
// Readiness checks include a Redis client pointed at a closed port, so every
// test also runs with Redis unavailable.
func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
	db := pgtest.NewMigrated(t)
	st := store.New(db.Pool)
	cipher, err := secrets.NewCipher(secrets.GenerateKey())
	if err != nil {
		t.Fatal(err)
	}
	logBuf := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	api := NewAPI(log, st,
		endpoints.NewService(st, cipher, ssrf.Policy{RequireHTTPS: true, Resolver: testResolver}),
		ingest.NewService(st))

	deadRedis := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = deadRedis.Close() })
	h := NewRouter(log, []Check{PostgresCheck(db.Pool), RedisCheck(deadRedis)}, api.Routes())
	return &apiEnv{t: t, db: db, st: st, h: h, logBuf: logBuf}
}

// project creates a project with one API key and returns both.
func (e *apiEnv) project(name string) (pgtype.UUID, string) {
	e.t.Helper()
	ctx := context.Background()
	p, err := e.st.CreateProject(ctx, name)
	if err != nil {
		e.t.Fatal(err)
	}
	k := secrets.GenerateAPIKey()
	if _, err := e.st.CreateAPIKey(ctx, sqlcgen.CreateAPIKeyParams{ProjectID: p.ID, Name: "test", Prefix: k.Prefix, KeyHash: k.Hash}); err != nil {
		e.t.Fatal(err)
	}
	return p.ID, k.Plaintext
}

type response struct {
	*httptest.ResponseRecorder
}

func (r response) json(t *testing.T, dst any) {
	t.Helper()
	if err := json.Unmarshal(r.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode %q: %v", r.Body.String(), err)
	}
}

func (r response) errCode(t *testing.T) string {
	t.Helper()
	var b errorBody
	r.json(t, &b)
	return b.Error.Code
}

// call sends a request. body may be nil, a string (sent verbatim) or a value
// (JSON-encoded). Extra headers are given as key, value pairs.
func (e *apiEnv) call(method, path, key string, body any, headers ...string) response {
	e.t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rdr = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			e.t.Fatal(err)
		}
		rdr = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return response{rec}
}

type createdEndpoint struct {
	endpointResponse
	Secret string `json:"secret"`
}

func (e *apiEnv) createEndpoint(key string, body map[string]any) createdEndpoint {
	e.t.Helper()
	res := e.call(http.MethodPost, "/v1/endpoints", key, body)
	if res.Code != http.StatusCreated {
		e.t.Fatalf("create endpoint: %d %s", res.Code, res.Body.String())
	}
	var ep createdEndpoint
	res.json(e.t, &ep)
	return ep
}

func (e *apiEnv) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.db.Pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *apiEnv) deliveryEndpoints(eventID string) []string {
	e.t.Helper()
	rows, err := e.db.Pool.Query(context.Background(),
		`SELECT endpoint_id::text FROM deliveries WHERE event_id = $1 AND status = 'pending'`, eventID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// ---- authentication ----

func TestAuthentication(t *testing.T) {
	e := newAPIEnv(t)
	projectID, key := e.project("acme")

	for name, header := range map[string]string{
		"missing":       "",
		"wrong scheme":  "Basic " + key,
		"no token":      "Bearer",
		"malformed":     "Bearer not-a-key",
		"unknown key":   "Bearer " + secrets.GenerateAPIKey().Plaintext,
		"key with junk": "Bearer " + key + "x",
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/endpoints", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			rec := httptest.NewRecorder()
			e.h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("missing WWW-Authenticate header")
			}
			if strings.Contains(rec.Body.String(), "hr_") {
				t.Error("response echoes key material")
			}
		})
	}

	// Valid key works, case-insensitive scheme, and last_used_at is recorded.
	res := e.call(http.MethodGet, "/v1/endpoints", key, nil)
	if res.Code != http.StatusOK {
		t.Fatalf("valid key: %d %s", res.Code, res.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/endpoints", nil)
	req.Header.Set("Authorization", "bearer "+key)
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("lowercase bearer: %d", rec.Code)
	}
	if n := e.count(`SELECT count(*) FROM api_keys WHERE project_id = $1 AND last_used_at IS NOT NULL`, projectID); n != 1 {
		t.Errorf("last_used_at not recorded")
	}

	// The plaintext key never appears in logs; the project does.
	logs := e.logBuf.String()
	if strings.Contains(logs, key) || strings.Contains(logs, key[len(key)-20:]) {
		t.Error("API key leaked into logs")
	}
	if !strings.Contains(logs, `"project_id":"`+projectID.String()+`"`) {
		t.Error("access log lacks project_id for authenticated requests")
	}
}

func TestRevokedKeyIsRejected(t *testing.T) {
	e := newAPIEnv(t)
	projectID, key := e.project("acme")
	if res := e.call(http.MethodGet, "/v1/endpoints", key, nil); res.Code != http.StatusOK {
		t.Fatalf("before revoke: %d", res.Code)
	}
	hash, _ := secrets.HashAPIKey(key)
	row, err := e.st.GetActiveAPIKeyByHash(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.st.RevokeAPIKey(context.Background(), sqlcgen.RevokeAPIKeyParams{ProjectID: projectID, ID: row.ID}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/endpoints"} {
		if res := e.call(http.MethodGet, path, key, nil); res.Code != http.StatusUnauthorized {
			t.Errorf("GET %s with revoked key: %d, want 401", path, res.Code)
		}
	}
	if res := e.call(http.MethodPost, "/v1/events", key, map[string]any{"event_type": "a", "payload": map[string]any{}}); res.Code != http.StatusUnauthorized {
		t.Errorf("ingest with revoked key: %d, want 401", res.Code)
	}
	if n := e.count(`SELECT count(*) FROM events`); n != 0 {
		t.Errorf("revoked key created %d events", n)
	}
}

// ---- endpoint CRUD ----

func TestEndpointCRUD(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")

	ep := e.createEndpoint(key, map[string]any{
		"url": "https://hooks.example.com/hr", "description": "orders", "event_types": []string{"order.paid", "order.paid", "order.refunded"},
	})
	if ep.ID == "" || ep.URL != "https://hooks.example.com/hr" || !ep.Enabled || ep.Description != "orders" {
		t.Fatalf("unexpected endpoint: %+v", ep)
	}
	if len(ep.EventTypes) != 2 {
		t.Errorf("event types not de-duplicated: %v", ep.EventTypes)
	}
	if !strings.HasPrefix(ep.Secret, "whsec_") {
		t.Errorf("secret = %q", ep.Secret)
	}

	// Get.
	res := e.call(http.MethodGet, "/v1/endpoints/"+ep.ID, key, nil)
	if res.Code != http.StatusOK || strings.Contains(res.Body.String(), "secret") {
		t.Fatalf("get: %d %s", res.Code, res.Body.String())
	}

	// Secret reveal returns the creation secret.
	var sec map[string]string
	res = e.call(http.MethodGet, "/v1/endpoints/"+ep.ID+"/secret", key, nil)
	res.json(t, &sec)
	if res.Code != http.StatusOK || sec["secret"] != ep.Secret {
		t.Fatalf("secret reveal: %d %v", res.Code, sec)
	}
	if res.Header().Get("Cache-Control") != "no-store" {
		t.Error("secret response must be Cache-Control: no-store")
	}

	// The secret is stored encrypted, never in plaintext.
	var ct []byte
	if err := e.db.Pool.QueryRow(context.Background(), `SELECT secret_ciphertext FROM endpoints WHERE id = $1`, ep.ID).Scan(&ct); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ct, []byte(ep.Secret)) || bytes.Contains(ct, []byte(ep.Secret[6:])) {
		t.Fatal("signing secret stored in plaintext")
	}
	if strings.Contains(e.logBuf.String(), ep.Secret[6:]) {
		t.Fatal("signing secret leaked into logs")
	}

	// Update: partial fields; empty event_types means "all".
	res = e.call(http.MethodPatch, "/v1/endpoints/"+ep.ID, key, map[string]any{"description": "all events", "event_types": []string{}})
	var upd endpointResponse
	res.json(t, &upd)
	if res.Code != http.StatusOK || upd.Description != "all events" || len(upd.EventTypes) != 0 || upd.URL != ep.URL || !upd.Enabled {
		t.Fatalf("update: %d %+v", res.Code, upd)
	}
	res = e.call(http.MethodPatch, "/v1/endpoints/"+ep.ID, key, map[string]any{"url": "https://a.example.com/new"})
	res.json(t, &upd)
	if res.Code != http.StatusOK || upd.URL != "https://a.example.com/new" || upd.Description != "all events" {
		t.Fatalf("update url: %d %+v", res.Code, upd)
	}

	// List.
	var list listEndpointsResponse
	res = e.call(http.MethodGet, "/v1/endpoints", key, nil)
	res.json(t, &list)
	if res.Code != http.StatusOK || len(list.Data) != 1 || list.NextCursor != nil {
		t.Fatalf("list: %d %s", res.Code, res.Body.String())
	}

	// Delete (soft): gone from the API, row kept for history.
	if res = e.call(http.MethodDelete, "/v1/endpoints/"+ep.ID, key, nil); res.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", res.Code)
	}
	for _, p := range []string{"/v1/endpoints/" + ep.ID, "/v1/endpoints/" + ep.ID + "/secret"} {
		if res = e.call(http.MethodGet, p, key, nil); res.Code != http.StatusNotFound {
			t.Errorf("GET %s after delete: %d", p, res.Code)
		}
	}
	if res = e.call(http.MethodDelete, "/v1/endpoints/"+ep.ID, key, nil); res.Code != http.StatusNotFound {
		t.Errorf("second delete: %d", res.Code)
	}
	if res = e.call(http.MethodPatch, "/v1/endpoints/"+ep.ID, key, map[string]any{"enabled": true}); res.Code != http.StatusNotFound {
		t.Errorf("patch deleted: %d", res.Code)
	}
	if n := e.count(`SELECT count(*) FROM endpoints WHERE id = $1 AND deleted_at IS NOT NULL AND NOT enabled`, ep.ID); n != 1 {
		t.Error("soft-deleted row not kept")
	}
	if res = e.call(http.MethodGet, "/v1/endpoints/not-a-uuid", key, nil); res.Code != http.StatusNotFound {
		t.Errorf("malformed id: %d", res.Code)
	}
}

func TestEndpointValidationAndSSRF(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")

	tests := map[string]map[string]any{
		"http in production": {"url": "http://hooks.example.com/"},
		"loopback ip":        {"url": "https://127.0.0.1/hook"},
		"private ip":         {"url": "https://10.1.2.3/hook"},
		"metadata ip":        {"url": "https://169.254.169.254/latest/meta-data"},
		"ipv6 loopback":      {"url": "https://[::1]/"},
		"localhost":          {"url": "https://localhost:8080/"},
		"internal suffix":    {"url": "https://db.internal/"},
		"resolves private":   {"url": "https://internal.example.com/"},
		"unresolvable":       {"url": "https://nope.example.com/"},
		"credentials":        {"url": "https://u:p@hooks.example.com/"},
		"bad scheme":         {"url": "file:///etc/passwd"},
		"missing url":        {"description": "x"},
		"bad event type":     {"url": "https://hooks.example.com/", "event_types": []string{"has space"}},
		"long description":   {"url": "https://hooks.example.com/", "description": strings.Repeat("d", 1025)},
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			res := e.call(http.MethodPost, "/v1/endpoints", key, body)
			if res.Code != http.StatusUnprocessableEntity || res.errCode(t) != "validation_failed" {
				t.Fatalf("status %d body %s, want 422 validation_failed", res.Code, res.Body.String())
			}
		})
	}
	var b errorBody
	e.call(http.MethodPost, "/v1/endpoints", key, map[string]any{"url": "https://10.0.0.1/"}).json(t, &b)
	if !strings.Contains(b.Error.Details["url"], "private") {
		t.Errorf("details = %v", b.Error.Details)
	}
	if n := e.count(`SELECT count(*) FROM endpoints`); n != 0 {
		t.Fatalf("%d endpoints created by invalid requests", n)
	}

	// PATCH re-validates the URL and leaves the endpoint unchanged on failure.
	ep := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/ok"})
	res := e.call(http.MethodPatch, "/v1/endpoints/"+ep.ID, key, map[string]any{"url": "https://192.168.1.1/"})
	if res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("patch to private url: %d", res.Code)
	}
	var got endpointResponse
	e.call(http.MethodGet, "/v1/endpoints/"+ep.ID, key, nil).json(t, &got)
	if got.URL != "https://hooks.example.com/ok" {
		t.Errorf("url changed to %q after rejected patch", got.URL)
	}

	// Malformed bodies.
	if res := e.call(http.MethodPost, "/v1/endpoints", key, `{"url":`); res.Code != http.StatusBadRequest {
		t.Errorf("invalid json: %d", res.Code)
	}
	if res := e.call(http.MethodPost, "/v1/endpoints", key, map[string]any{"url": "https://hooks.example.com/", "secret": "mine"}); res.Code != http.StatusBadRequest {
		t.Errorf("unknown field (client-chosen secret): %d", res.Code)
	}
}

func TestEndpointPagination(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	created := map[string]bool{}
	for i := range 5 {
		ep := e.createEndpoint(key, map[string]any{"url": fmt.Sprintf("https://hooks.example.com/%d", i)})
		created[ep.ID] = true
	}
	seen := map[string]bool{}
	path := "/v1/endpoints?limit=2"
	pages := 0
	for {
		var page listEndpointsResponse
		res := e.call(http.MethodGet, path, key, nil)
		res.json(t, &page)
		if res.Code != http.StatusOK {
			t.Fatalf("list: %d %s", res.Code, res.Body.String())
		}
		pages++
		for _, ep := range page.Data {
			if seen[ep.ID] {
				t.Fatalf("endpoint %s returned twice", ep.ID)
			}
			seen[ep.ID] = true
		}
		if page.NextCursor == nil {
			break
		}
		path = "/v1/endpoints?limit=2&cursor=" + *page.NextCursor
		if pages > 5 {
			t.Fatal("pagination does not terminate")
		}
	}
	if pages != 3 || len(seen) != 5 {
		t.Errorf("pages = %d, seen = %d; want 3 pages, 5 endpoints", pages, len(seen))
	}
	for _, bad := range []string{"?limit=0", "?limit=500", "?cursor=garbage"} {
		if res := e.call(http.MethodGet, "/v1/endpoints"+bad, key, nil); res.Code != http.StatusBadRequest {
			t.Errorf("GET /v1/endpoints%s: %d, want 400", bad, res.Code)
		}
	}
}

func TestEndpointLimit(t *testing.T) {
	e := newAPIEnv(t)
	projectID, key := e.project("acme")
	_, err := e.db.Pool.Exec(context.Background(),
		`INSERT INTO endpoints (project_id, url, secret_ciphertext)
		 SELECT $1, 'https://hooks.example.com/' || g, '\x01' FROM generate_series(1, $2) g`,
		projectID, endpoints.MaxEndpointsPerProject)
	if err != nil {
		t.Fatal(err)
	}
	res := e.call(http.MethodPost, "/v1/endpoints", key, map[string]any{"url": "https://hooks.example.com/one-too-many"})
	if res.Code != http.StatusConflict || res.errCode(t) != "endpoint_limit_reached" {
		t.Fatalf("status %d %s, want 409 endpoint_limit_reached", res.Code, res.Body.String())
	}
	// Deleted endpoints don't count.
	if _, err := e.db.Pool.Exec(context.Background(), `UPDATE endpoints SET deleted_at = now() WHERE url = 'https://hooks.example.com/1'`); err != nil {
		t.Fatal(err)
	}
	if res := e.call(http.MethodPost, "/v1/endpoints", key, map[string]any{"url": "https://hooks.example.com/fits"}); res.Code != http.StatusCreated {
		t.Errorf("after delete: %d", res.Code)
	}
}

func TestSecretCiphertextIsBoundToEndpoint(t *testing.T) {
	e := newAPIEnv(t)
	_, keyA := e.project("a")
	_, keyB := e.project("b")
	a1 := e.createEndpoint(keyA, map[string]any{"url": "https://a.example.com/1"})
	a2 := e.createEndpoint(keyA, map[string]any{"url": "https://a.example.com/2"})
	b1 := e.createEndpoint(keyB, map[string]any{"url": "https://b.example.com/1"})

	// An attacker with DB write access copies a1's ciphertext onto a2 and b1.
	for _, target := range []string{a2.ID, b1.ID} {
		if _, err := e.db.Pool.Exec(context.Background(),
			`UPDATE endpoints SET secret_ciphertext = (SELECT secret_ciphertext FROM endpoints WHERE id = $1) WHERE id = $2`,
			a1.ID, target); err != nil {
			t.Fatal(err)
		}
	}
	if res := e.call(http.MethodGet, "/v1/endpoints/"+a2.ID+"/secret", keyA, nil); res.Code != http.StatusInternalServerError || strings.Contains(res.Body.String(), a1.Secret) {
		t.Errorf("copied ciphertext (same project) decrypted: %d %s", res.Code, res.Body.String())
	}
	if res := e.call(http.MethodGet, "/v1/endpoints/"+b1.ID+"/secret", keyB, nil); res.Code != http.StatusInternalServerError || strings.Contains(res.Body.String(), a1.Secret) {
		t.Errorf("copied ciphertext (other project) decrypted: %d %s", res.Code, res.Body.String())
	}
	var sec map[string]string
	e.call(http.MethodGet, "/v1/endpoints/"+a1.ID+"/secret", keyA, nil).json(t, &sec)
	if sec["secret"] != a1.Secret {
		t.Error("original endpoint secret no longer decrypts")
	}
}

// ---- tenant isolation ----

func TestCrossTenantAccessIsPrevented(t *testing.T) {
	e := newAPIEnv(t)
	_, keyA := e.project("a")
	_, keyB := e.project("b")
	epA := e.createEndpoint(keyA, map[string]any{"url": "https://a.example.com/hook"})
	e.createEndpoint(keyB, map[string]any{"url": "https://b.example.com/hook"})

	// B cannot see, read the secret of, change or delete A's endpoint.
	checks := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/endpoints/" + epA.ID, nil},
		{http.MethodGet, "/v1/endpoints/" + epA.ID + "/secret", nil},
		{http.MethodPatch, "/v1/endpoints/" + epA.ID, map[string]any{"enabled": false}},
		{http.MethodDelete, "/v1/endpoints/" + epA.ID, nil},
	}
	for _, c := range checks {
		res := e.call(c.method, c.path, keyB, c.body)
		if res.Code != http.StatusNotFound {
			t.Errorf("B %s %s: %d, want 404", c.method, c.path, res.Code)
		}
		if strings.Contains(res.Body.String(), epA.Secret) || strings.Contains(res.Body.String(), "a.example.com") {
			t.Errorf("B %s %s leaked A's data", c.method, c.path)
		}
	}
	var got endpointResponse
	e.call(http.MethodGet, "/v1/endpoints/"+epA.ID, keyA, nil).json(t, &got)
	if !got.Enabled {
		t.Error("B's PATCH modified A's endpoint")
	}

	// Listings are scoped.
	var list listEndpointsResponse
	e.call(http.MethodGet, "/v1/endpoints", keyB, nil).json(t, &list)
	if len(list.Data) != 1 || list.Data[0].ID == epA.ID {
		t.Errorf("B's listing: %+v", list.Data)
	}

	// B's events never fan out to A's endpoints, even for matching types.
	var ev eventResponse
	e.call(http.MethodPost, "/v1/events", keyB, map[string]any{"event_type": "user.created", "payload": map[string]any{}}).json(t, &ev)
	if ev.DeliveriesCreated != 1 {
		t.Errorf("B's event created %d deliveries, want 1 (its own endpoint only)", ev.DeliveriesCreated)
	}
	if n := e.count(`SELECT count(*) FROM deliveries WHERE endpoint_id = $1`, epA.ID); n != 0 {
		t.Errorf("A's endpoint received %d deliveries from B's event", n)
	}

	// Idempotency keys are per project: the same key in A creates a new event.
	e.call(http.MethodPost, "/v1/events", keyB, map[string]any{"event_type": "x", "payload": map[string]any{}}, IdempotencyKeyHeader, "shared-key")
	res := e.call(http.MethodPost, "/v1/events", keyA, map[string]any{"event_type": "x", "payload": map[string]any{}}, IdempotencyKeyHeader, "shared-key")
	if res.Code != http.StatusAccepted {
		t.Errorf("same idempotency key in another project: %d, want 202", res.Code)
	}
}

// ---- ingestion ----

func TestIngestFanOutMatching(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	_, otherKey := e.project("other")

	all := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/all"})
	users := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/users", "event_types": []string{"user.created", "user.deleted"}})
	orders := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/orders", "event_types": []string{"order.paid"}})
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/disabled", "event_types": []string{"user.created"}, "enabled": false})
	deleted := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/deleted", "event_types": []string{"user.created"}})
	e.call(http.MethodDelete, "/v1/endpoints/"+deleted.ID, key, nil)
	e.createEndpoint(otherKey, map[string]any{"url": "https://b.example.com/all"})

	tests := []struct {
		eventType string
		want      []string
	}{
		{"user.created", []string{all.ID, users.ID}},
		{"order.paid", []string{all.ID, orders.ID}},
		{"something.else", []string{all.ID}},
		{"User.Created", []string{all.ID}}, // filters are case-sensitive
	}
	for _, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			res := e.call(http.MethodPost, "/v1/events", key, map[string]any{"event_type": tt.eventType, "payload": map[string]any{"n": 1}})
			if res.Code != http.StatusAccepted {
				t.Fatalf("ingest: %d %s", res.Code, res.Body.String())
			}
			var ev eventResponse
			res.json(t, &ev)
			if ev.DeliveriesCreated != int64(len(tt.want)) {
				t.Errorf("deliveries_created = %d, want %d", ev.DeliveriesCreated, len(tt.want))
			}
			want := append([]string(nil), tt.want...)
			sort.Strings(want)
			if got := e.deliveryEndpoints(ev.ID); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("deliveries to %v, want %v", got, want)
			}
		})
	}

	// A project with no endpoints still accepts events.
	_, emptyKey := e.project("empty")
	res := e.call(http.MethodPost, "/v1/events", emptyKey, map[string]any{"event_type": "a", "payload": map[string]any{}})
	var ev eventResponse
	res.json(t, &ev)
	if res.Code != http.StatusAccepted || ev.DeliveriesCreated != 0 {
		t.Errorf("no endpoints: %d %+v", res.Code, ev)
	}
}

func TestIngestStoresExactPayloadAndPendingDeliveries(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	ep := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/"})

	payload := "{ \"b\" : 1,\n  \"a\": [1, 2.50, \"\\u00e9\"] }"
	res := e.call(http.MethodPost, "/v1/events", key, `{"event_type":"order.paid","payload":`+payload+`}`)
	if res.Code != http.StatusAccepted {
		t.Fatalf("ingest: %d %s", res.Code, res.Body.String())
	}
	var ev eventResponse
	res.json(t, &ev)
	if ev.ID == "" || ev.EventType != "order.paid" || ev.IdempotencyKey != nil || ev.CreatedAt == "" {
		t.Errorf("response: %+v", ev)
	}

	var stored []byte
	if err := e.db.Pool.QueryRow(context.Background(), `SELECT payload FROM events WHERE id = $1`, ev.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) != payload {
		t.Errorf("payload not stored verbatim:\n got %q\nwant %q", stored, payload)
	}

	var status string
	var attempts int
	var dueNow bool
	err := e.db.Pool.QueryRow(context.Background(),
		`SELECT status::text, attempt_count, next_attempt_at <= now() FROM deliveries WHERE event_id = $1 AND endpoint_id = $2`,
		ev.ID, ep.ID).Scan(&status, &attempts, &dueNow)
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 0 || !dueNow {
		t.Errorf("delivery = %s attempts=%d due=%v; want pending, 0, due now", status, attempts, dueNow)
	}
}

func TestIngestValidation(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")

	exact := `{"x":"` + strings.Repeat("a", ingest.MaxPayloadBytes-8) + `"}`
	tests := []struct {
		name    string
		body    string
		headers []string
		ct      string
		status  int
		code    string
	}{
		{"exactly 256 KB payload", `{"event_type":"big","payload":` + exact + `}`, nil, "", 202, ""},
		{"payload over 256 KB", `{"event_type":"big","payload":{"x":"` + strings.Repeat("a", ingest.MaxPayloadBytes) + `"}}`, nil, "", 413, "payload_too_large"},
		{"body over limit", `{"event_type":"big","payload":{"x":"` + strings.Repeat("a", 300<<10) + `"}}`, nil, "", 413, "payload_too_large"},
		{"missing event_type", `{"payload":{}}`, nil, "", 422, "validation_failed"},
		{"invalid event_type", `{"event_type":"has space","payload":{}}`, nil, "", 422, "validation_failed"},
		{"missing payload", `{"event_type":"a"}`, nil, "", 422, "validation_failed"},
		{"null payload", `{"event_type":"a","payload":null}`, nil, "", 422, "validation_failed"},
		{"array payload", `{"event_type":"a","payload":[1,2]}`, nil, "", 422, "validation_failed"},
		{"string payload", `{"event_type":"a","payload":"hi"}`, nil, "", 422, "validation_failed"},
		{"event_type wrong type", `{"event_type":5,"payload":{}}`, nil, "", 422, "validation_failed"},
		{"invalid json", `{"event_type":"a","payload":{`, nil, "", 400, "invalid_json"},
		{"unknown field", `{"event_type":"a","payload":{},"type":"x"}`, nil, "", 400, "invalid_json"},
		{"wrong content type", `{"event_type":"a","payload":{}}`, nil, "text/plain", 415, "unsupported_media_type"},
		{"bad idempotency key", `{"event_type":"a","payload":{}}`, []string{IdempotencyKeyHeader, "has space"}, "", 422, "validation_failed"},
		{"two idempotency keys", `{"event_type":"a","payload":{}}`, []string{IdempotencyKeyHeader, "k1", IdempotencyKeyHeader, "k2"}, "", 400, "invalid_header"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(tt.body))
			ct := tt.ct
			if ct == "" {
				ct = "application/json"
			}
			req.Header.Set("Content-Type", ct)
			req.Header.Set("Authorization", "Bearer "+key)
			for i := 0; i+1 < len(tt.headers); i += 2 {
				req.Header.Add(tt.headers[i], tt.headers[i+1])
			}
			rec := httptest.NewRecorder()
			e.h.ServeHTTP(rec, req)
			res := response{rec}
			if res.Code != tt.status {
				t.Fatalf("status = %d, want %d (%s)", res.Code, tt.status, truncate(res.Body.String()))
			}
			if tt.code != "" && res.errCode(t) != tt.code {
				t.Errorf("code = %s, want %s", res.errCode(t), tt.code)
			}
		})
	}
	if n := e.count(`SELECT count(*) FROM events`); n != 1 {
		t.Errorf("events stored = %d, want 1 (only the valid 256 KB one)", n)
	}
}

func truncate(s string) string {
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}

func TestIngestIdempotency(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/1"})
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/2"})
	body := map[string]any{"event_type": "order.paid", "payload": map[string]any{"order": 42}}

	first := e.call(http.MethodPost, "/v1/events", key, body, IdempotencyKeyHeader, "order-42")
	if first.Code != http.StatusAccepted || first.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("first: %d %v", first.Code, first.Header())
	}
	var ev1 eventResponse
	first.json(t, &ev1)
	if ev1.IdempotencyKey == nil || *ev1.IdempotencyKey != "order-42" || ev1.DeliveriesCreated != 2 {
		t.Fatalf("first response: %+v", ev1)
	}

	// Identical retry: same event, 200, header, no new rows.
	retry := e.call(http.MethodPost, "/v1/events", key, body, IdempotencyKeyHeader, "order-42")
	var ev2 eventResponse
	retry.json(t, &ev2)
	if retry.Code != http.StatusOK || retry.Header().Get("Idempotent-Replayed") != "true" || ev2.ID != ev1.ID || ev2.DeliveriesCreated != 2 {
		t.Fatalf("retry: %d %+v", retry.Code, ev2)
	}

	// Same key, different payload or type: 409.
	for name, b := range map[string]map[string]any{
		"different payload": {"event_type": "order.paid", "payload": map[string]any{"order": 43}},
		"different type":    {"event_type": "order.refunded", "payload": map[string]any{"order": 42}},
	} {
		res := e.call(http.MethodPost, "/v1/events", key, b, IdempotencyKeyHeader, "order-42")
		if res.Code != http.StatusConflict || res.errCode(t) != "idempotency_key_reuse" {
			t.Errorf("%s: %d %s", name, res.Code, res.Body.String())
		}
	}

	// Without a key, identical requests are distinct events.
	e.call(http.MethodPost, "/v1/events", key, body)
	e.call(http.MethodPost, "/v1/events", key, body)
	if n := e.count(`SELECT count(*) FROM events`); n != 3 {
		t.Errorf("events = %d, want 3", n)
	}
	if n := e.count(`SELECT count(*) FROM deliveries`); n != 6 {
		t.Errorf("deliveries = %d, want 6", n)
	}
}

func TestIngestIdempotencyUnderConcurrency(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	for i := range 3 {
		e.createEndpoint(key, map[string]any{"url": fmt.Sprintf("https://hooks.example.com/%d", i)})
	}
	const n = 12
	var wg sync.WaitGroup
	results := make([]response, n)
	for i := range n {
		wg.Go(func() {
			results[i] = e.call(http.MethodPost, "/v1/events", key,
				map[string]any{"event_type": "user.created", "payload": map[string]any{"id": 7}},
				IdempotencyKeyHeader, "user-7")
		})
	}
	wg.Wait()

	accepted, replayed := 0, 0
	ids := map[string]bool{}
	for _, r := range results {
		var ev eventResponse
		r.json(t, &ev)
		ids[ev.ID] = true
		switch r.Code {
		case http.StatusAccepted:
			accepted++
		case http.StatusOK:
			replayed++
		default:
			t.Errorf("unexpected status %d: %s", r.Code, r.Body.String())
		}
	}
	if accepted != 1 || replayed != n-1 || len(ids) != 1 {
		t.Errorf("accepted=%d replayed=%d distinct ids=%d; want 1, %d, 1", accepted, replayed, len(ids), n-1)
	}
	if c := e.count(`SELECT count(*) FROM events`); c != 1 {
		t.Errorf("events = %d, want 1", c)
	}
	if c := e.count(`SELECT count(*) FROM deliveries`); c != 3 {
		t.Errorf("deliveries = %d, want 3", c)
	}
}

func TestIngestRollsBackOnFailure(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/"})

	// Make the fan-out INSERT fail after the event INSERT has succeeded.
	ctx := context.Background()
	_, err := e.db.Pool.Exec(ctx, `
		CREATE FUNCTION fail_deliveries() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected failure'; END; $$;
		CREATE TRIGGER fail_deliveries BEFORE INSERT ON deliveries FOR EACH ROW EXECUTE FUNCTION fail_deliveries();`)
	if err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"event_type": "a", "payload": map[string]any{}}
	res := e.call(http.MethodPost, "/v1/events", key, body, IdempotencyKeyHeader, "k1")
	if res.Code != http.StatusInternalServerError || strings.Contains(res.Body.String(), "injected") {
		t.Fatalf("status %d body %s; want generic 500", res.Code, res.Body.String())
	}
	if n := e.count(`SELECT count(*) FROM events`); n != 0 {
		t.Fatalf("event persisted despite failed transaction (%d rows)", n)
	}

	// Nothing was left behind: the same idempotency key now succeeds as new.
	if _, err := e.db.Pool.Exec(ctx, `DROP TRIGGER fail_deliveries ON deliveries`); err != nil {
		t.Fatal(err)
	}
	res = e.call(http.MethodPost, "/v1/events", key, body, IdempotencyKeyHeader, "k1")
	if res.Code != http.StatusAccepted {
		t.Fatalf("after recovery: %d, want 202 (not a replay)", res.Code)
	}
}

func TestDisableAndDeleteCancelPendingDeliveries(t *testing.T) {
	e := newAPIEnv(t)
	_, key := e.project("acme")
	ep := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/"})
	ingestOne := func() {
		t.Helper()
		if res := e.call(http.MethodPost, "/v1/events", key, map[string]any{"event_type": "a", "payload": map[string]any{}}); res.Code != http.StatusAccepted {
			t.Fatalf("ingest: %d", res.Code)
		}
	}
	pending := func() int {
		return e.count(`SELECT count(*) FROM deliveries WHERE endpoint_id = $1 AND status = 'pending'`, ep.ID)
	}

	ingestOne()
	if pending() != 1 {
		t.Fatal("expected one pending delivery")
	}
	// A delivery already handed to a worker is not touched.
	if _, err := e.db.Pool.Exec(context.Background(),
		`UPDATE deliveries SET status = 'queued', queued_at = now() WHERE endpoint_id = $1`, ep.ID); err != nil {
		t.Fatal(err)
	}
	ingestOne()

	e.call(http.MethodPatch, "/v1/endpoints/"+ep.ID, key, map[string]any{"enabled": false})
	if pending() != 0 || e.count(`SELECT count(*) FROM deliveries WHERE endpoint_id = $1 AND status = 'cancelled' AND completed_at IS NOT NULL`, ep.ID) != 1 {
		t.Error("disable did not cancel the pending delivery")
	}
	if e.count(`SELECT count(*) FROM deliveries WHERE endpoint_id = $1 AND status = 'queued'`, ep.ID) != 1 {
		t.Error("disable must not touch queued deliveries")
	}
	ingestOne() // disabled endpoint receives nothing
	if pending() != 0 {
		t.Error("disabled endpoint received a delivery")
	}

	e.call(http.MethodPatch, "/v1/endpoints/"+ep.ID, key, map[string]any{"enabled": true})
	ingestOne()
	if pending() != 1 {
		t.Fatal("re-enabled endpoint did not receive a delivery")
	}
	e.call(http.MethodDelete, "/v1/endpoints/"+ep.ID, key, nil)
	if pending() != 0 || e.count(`SELECT count(*) FROM deliveries WHERE endpoint_id = $1 AND status = 'cancelled'`, ep.ID) != 2 {
		t.Error("delete did not cancel the pending delivery")
	}
}

func TestIngestWorksWithRedisUnavailable(t *testing.T) {
	e := newAPIEnv(t) // readiness includes a Redis client on a closed port
	_, key := e.project("acme")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/"})

	ready := e.call(http.MethodGet, "/readyz", "", nil)
	var rr readyResponse
	ready.json(t, &rr)
	if ready.Code != http.StatusServiceUnavailable || rr.Checks["redis"] != "unavailable" || rr.Checks["postgres"] != "ok" {
		t.Fatalf("precondition: readyz = %d %+v; want Redis down, Postgres up", ready.Code, rr)
	}

	res := e.call(http.MethodPost, "/v1/events", key, map[string]any{"event_type": "order.paid", "payload": map[string]any{"id": 1}})
	if res.Code != http.StatusAccepted {
		t.Fatalf("ingest with Redis down: %d %s", res.Code, res.Body.String())
	}
	var ev eventResponse
	res.json(t, &ev)
	if ev.DeliveriesCreated != 1 || e.count(`SELECT count(*) FROM deliveries WHERE event_id = $1 AND status = 'pending'`, ev.ID) != 1 {
		t.Error("delivery not recorded in Postgres while Redis is down")
	}
}
