package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// Integration tests for the Phase 5 read APIs, manual retry and stats.
// Delivery states and attempts are arranged with SQL (the worker is tested
// in internal/delivery and internal/dispatch); everything is read back
// through the real HTTP API.

func (e *apiEnv) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Pool.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

// ingestEvent ingests one event and returns its ID and its delivery IDs.
func (e *apiEnv) ingestEvent(key, eventType, payload string) (string, []string) {
	e.t.Helper()
	res := e.call(http.MethodPost, "/v1/events", key, fmt.Sprintf(`{"event_type":%q,"payload":%s}`, eventType, payload))
	if res.Code != http.StatusAccepted {
		e.t.Fatalf("ingest: %d %s", res.Code, res.Body.String())
	}
	var ev eventResponse
	res.json(e.t, &ev)
	rows, err := e.db.Pool.Query(context.Background(),
		`SELECT id::text FROM deliveries WHERE event_id = $1 ORDER BY created_at, id`, ev.ID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			e.t.Fatal(err)
		}
		ids = append(ids, s)
	}
	return ev.ID, ids
}

// setStatus puts a delivery into status with the given attempt count,
// respecting the schema's constraints.
func (e *apiEnv) setStatus(id, status string, attempts int) {
	e.t.Helper()
	e.exec(`UPDATE deliveries
		SET status = $2::delivery_status, attempt_count = $3,
		    completed_at = CASE WHEN $2 IN ('succeeded', 'dead', 'cancelled') THEN now() END,
		    queued_at    = CASE WHEN $2 IN ('queued', 'in_flight') THEN now() END,
		    locked_until = CASE WHEN $2 = 'in_flight' THEN now() + interval '1 hour' END,
		    last_status_code = CASE WHEN $2 = 'dead' THEN 500 WHEN $2 = 'succeeded' THEN 200 END,
		    last_error = CASE WHEN $2 = 'dead' THEN 'HTTP 500' END
		WHERE id = $1`, id, status, attempts)
}

// addAttempt records attempt n. errorKind "" means success (HTTP 200).
func (e *apiEnv) addAttempt(deliveryID string, n int, durationMs int, errorKind string, finishedAt time.Time) {
	e.t.Helper()
	var status *int
	var kind, msg, body *string
	switch errorKind {
	case "":
		s, b := 200, "ok"
		status, body = &s, &b
	case "http_status":
		s, b, k, m := 500, "boom", errorKind, "HTTP 500"
		status, body, kind, msg = &s, &b, &k, &m
	default:
		k, m := errorKind, errorKind+" error"
		kind, msg = &k, &m
	}
	started := finishedAt.Add(-time.Duration(durationMs) * time.Millisecond)
	e.exec(`INSERT INTO delivery_attempts (project_id, delivery_id, attempt_number, started_at, finished_at,
		duration_ms, response_status, response_body, error_kind, error_message)
		SELECT project_id, id, $2, $3, $4, $5, $6, $7, $8, $9 FROM deliveries WHERE id = $1`,
		deliveryID, n, started, finishedAt, durationMs, status, body, kind, msg)
}

type deliveryListBody struct {
	Data       []deliveryItem `json:"data"`
	NextCursor *string        `json:"next_cursor"`
}

type eventListBody struct {
	Data       []eventListItem `json:"data"`
	NextCursor *string         `json:"next_cursor"`
}

func (e *apiEnv) listDeliveries(key, query string) deliveryListBody {
	e.t.Helper()
	res := e.call(http.MethodGet, "/v1/deliveries?"+query, key, nil)
	if res.Code != http.StatusOK {
		e.t.Fatalf("GET /v1/deliveries?%s: %d %s", query, res.Code, res.Body.String())
	}
	var b deliveryListBody
	res.json(e.t, &b)
	return b
}

func (e *apiEnv) listEvents(key, query string) eventListBody {
	e.t.Helper()
	res := e.call(http.MethodGet, "/v1/events?"+query, key, nil)
	if res.Code != http.StatusOK {
		e.t.Fatalf("GET /v1/events?%s: %d %s", query, res.Code, res.Body.String())
	}
	var b eventListBody
	res.json(e.t, &b)
	return b
}

func deliveryIDs(items []deliveryItem) []string {
	out := make([]string, len(items))
	for i, d := range items {
		out[i] = d.ID
	}
	return out
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
	}
	for _, n := range m {
		if n != 0 {
			return false
		}
	}
	return true
}

// ======================= Events =======================

func TestListAndGetEvents(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	ep := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/a"})

	// Empty project: data is [] (never null), no cursor.
	if res := e.call(http.MethodGet, "/v1/events", key, nil); res.Code != 200 || res.Body.String() != "{\"data\":[],\"next_cursor\":null}\n" {
		t.Fatalf("empty list = %d %s", res.Code, res.Body.String())
	}

	// Whitespace, key order and characters encoding/json would escape.
	payload := `{"order_id": 1001,  "items":[1,2], "note":"<spaces & html kept>"}`
	ev1, dels := e.ingestEvent(key, "order.paid", payload)
	ev2, _ := e.ingestEvent(key, "order.refunded", `{"n":2}`)
	ev3, _ := e.ingestEvent(key, "order.paid", `{"n":3}`)

	all := e.listEvents(key, "")
	if len(all.Data) != 3 || all.Data[0].ID != ev3 || all.Data[1].ID != ev2 || all.Data[2].ID != ev1 || all.NextCursor != nil {
		t.Fatalf("list = %+v", all)
	}
	if all.Data[2].PayloadSize != int32(len(payload)) || all.Data[2].EventType != "order.paid" {
		t.Fatalf("list item = %+v", all.Data[2])
	}
	if res := e.call(http.MethodGet, "/v1/events", key, nil); json.Valid(res.Body.Bytes()) &&
		contains(res.Body.String(), `"payload"`) {
		t.Fatal("event list must not include payloads")
	}

	paid := e.listEvents(key, "event_type=order.paid")
	if len(paid.Data) != 2 || paid.Data[0].ID != ev3 || paid.Data[1].ID != ev1 {
		t.Fatalf("filtered = %+v", paid)
	}
	if none := e.listEvents(key, "event_type=nothing.here"); len(none.Data) != 0 {
		t.Fatalf("no-match filter = %+v", none)
	}

	// Detail: exact payload bytes, per-endpoint delivery summary.
	res := e.call(http.MethodGet, "/v1/events/"+ev1, key, nil)
	if res.Code != http.StatusOK {
		t.Fatalf("get event: %d %s", res.Code, res.Body.String())
	}
	var raw struct {
		Payload json.RawMessage `json:"payload"`
	}
	res.json(t, &raw)
	if string(raw.Payload) != payload {
		t.Fatalf("payload = %s, want exact stored bytes %s", raw.Payload, payload)
	}
	var det eventDetailResponse
	res.json(t, &det)
	if det.ID != ev1 || det.EventType != "order.paid" || det.DeliveriesTruncated || len(det.Deliveries) != 1 {
		t.Fatalf("detail = %+v", det)
	}
	d := det.Deliveries[0]
	if d.ID != dels[0] || d.EndpointID != ep.ID || d.EndpointURL != ep.URL || d.Status != "pending" || d.AttemptCount != 0 {
		t.Fatalf("delivery summary = %+v", d)
	}

	// Unknown and malformed IDs are 404.
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-a-uuid"} {
		if res := e.call(http.MethodGet, "/v1/events/"+id, key, nil); res.Code != 404 || res.errCode(t) != "not_found" {
			t.Fatalf("GET /v1/events/%s = %d %s", id, res.Code, res.Body.String())
		}
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func TestReadAPIInvalidParameters(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	cases := []string{
		"/v1/events?limit=0",
		"/v1/events?limit=101",
		"/v1/events?limit=abc",
		"/v1/events?cursor=%%%",
		"/v1/events?cursor=bm90LWEtY3Vyc29y",
		"/v1/events?event_type=bad%20type",
		"/v1/events?created_after=yesterday",
		"/v1/events?created_after=2026-01-02T00:00:00Z&created_before=2026-01-01T00:00:00Z",
		"/v1/events?evnet_type=order.paid", // typo: unknown parameter
		"/v1/events?limit=1&limit=2",
		"/v1/deliveries?status=failed",
		"/v1/deliveries?status=dead,bogus",
		"/v1/deliveries?endpoint_id=nope",
		"/v1/deliveries?event_id=123",
		"/v1/deliveries?statuss=dead",
		"/v1/deliveries/00000000-0000-0000-0000-000000000000/attempts?cursor=xyz",
		"/v1/stats?from=nope",
		"/v1/stats?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
		"/v1/stats?from=2025-01-01T00:00:00Z&to=2026-01-01T00:00:00Z", // > 90 days
		"/v1/stats?window=24h",
	}
	for _, path := range cases {
		res := e.call(http.MethodGet, path, key, nil)
		if res.Code != http.StatusBadRequest || res.errCode(t) != "invalid_parameter" {
			t.Errorf("GET %s = %d %s, want 400 invalid_parameter", path, res.Code, res.Body.String())
		}
	}
}

// ======================= Pagination and filters =======================

func TestEventAndDeliveryPagination(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/a"})
	var wantEvents, wantDeliveries []string
	for i := range 7 {
		ev, ds := e.ingestEvent(key, "page.test", fmt.Sprintf(`{"i":%d}`, i))
		wantEvents = append([]string{ev}, wantEvents...) // newest first
		wantDeliveries = append(append([]string{}, ds...), wantDeliveries...)
	}
	// Force created_at ties: the (created_at, id) keyset must still be exact.
	e.exec(`UPDATE events SET created_at = '2026-06-01T00:00:00Z'`)
	e.exec(`UPDATE deliveries SET created_at = '2026-06-01T00:00:00Z'`)

	pageAll := func(path string) []string {
		var got []string
		cursor := ""
		for pages := 0; ; pages++ {
			if pages > 10 {
				t.Fatalf("%s: too many pages", path)
			}
			q := "limit=3"
			if cursor != "" {
				q += "&cursor=" + url.QueryEscape(cursor)
			}
			res := e.call(http.MethodGet, path+"?"+q, key, nil)
			if res.Code != 200 {
				t.Fatalf("%s: %d %s", path, res.Code, res.Body.String())
			}
			var b struct {
				Data       []struct{ ID string } `json:"data"`
				NextCursor *string               `json:"next_cursor"`
			}
			res.json(t, &b)
			if len(b.Data) > 3 {
				t.Fatalf("page of %d > limit", len(b.Data))
			}
			for _, d := range b.Data {
				got = append(got, d.ID)
			}
			if b.NextCursor == nil {
				return got
			}
			cursor = *b.NextCursor
		}
	}
	gotEvents := pageAll("/v1/events")
	gotDeliveries := pageAll("/v1/deliveries")
	if len(gotEvents) != 7 || !sameSet(gotEvents, wantEvents) {
		t.Fatalf("paged events = %v", gotEvents)
	}
	if len(gotDeliveries) != 7 || !sameSet(gotDeliveries, wantDeliveries) {
		t.Fatalf("paged deliveries = %v", gotDeliveries)
	}
	// With equal created_at the order is id DESC, strictly.
	for i := 1; i < len(gotEvents); i++ {
		if gotEvents[i-1] <= gotEvents[i] {
			t.Fatalf("events not in (created_at, id) DESC order: %v", gotEvents)
		}
	}
	// An exact multiple of the page size ends with next_cursor null, not an empty page.
	if b := e.listEvents(key, "limit=7"); len(b.Data) != 7 || b.NextCursor != nil {
		t.Fatalf("limit=7: %d items, cursor %v", len(b.Data), b.NextCursor)
	}
}

func TestListDeliveriesFilters(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	epA := e.createEndpoint(key, map[string]any{"url": "https://a.example.com/h"})
	epB := e.createEndpoint(key, map[string]any{"url": "https://b.example.com/h", "event_types": []string{"user.created"}})

	ev1, d1 := e.ingestEvent(key, "order.paid", `{"n":1}`)   // A only
	ev2, d2 := e.ingestEvent(key, "user.created", `{"n":2}`) // A and B
	if len(d1) != 1 || len(d2) != 2 {
		t.Fatalf("fan-out: %v %v", d1, d2)
	}
	var d2A, d2B string
	for _, id := range d2 {
		var ep string
		if err := e.db.Pool.QueryRow(context.Background(), `SELECT endpoint_id::text FROM deliveries WHERE id = $1`, id).Scan(&ep); err != nil {
			t.Fatal(err)
		}
		if ep == epA.ID {
			d2A = id
		} else {
			d2B = id
		}
	}
	e.setStatus(d1[0], "dead", 10)
	e.setStatus(d2A, "succeeded", 1)
	e.setStatus(d2B, "scheduled", 2)
	e.exec(`UPDATE deliveries SET created_at = '2026-05-01T12:00:00Z' WHERE id = $1`, d1[0])
	e.exec(`UPDATE deliveries SET created_at = '2026-05-02T12:00:00Z' WHERE event_id = $1`, ev2)

	cases := []struct {
		query string
		want  []string
	}{
		{"", []string{d1[0], d2A, d2B}},
		{"status=dead", []string{d1[0]}},
		{"status=dead,succeeded", []string{d1[0], d2A}},
		{"status=pending", nil},
		{"endpoint_id=" + epB.ID, []string{d2B}},
		{"endpoint_id=" + epA.ID, []string{d1[0], d2A}},
		{"event_id=" + ev1, []string{d1[0]}},
		{"event_type=user.created", []string{d2A, d2B}},
		{"event_type=user.created&status=scheduled", []string{d2B}},
		{"created_after=2026-05-02T00:00:00Z", []string{d2A, d2B}},
		{"created_before=2026-05-02T00:00:00Z", []string{d1[0]}},
		{"created_after=2026-05-01T12:00:00Z&created_before=2026-05-01T12:00:00.000001Z", []string{d1[0]}}, // inclusive / exclusive
		{"endpoint_id=00000000-0000-0000-0000-000000000000", nil},
	}
	for _, c := range cases {
		got := e.listDeliveries(key, c.query)
		if got.Data == nil || !sameSet(deliveryIDs(got.Data), c.want) {
			t.Errorf("?%s = %v, want %v", c.query, deliveryIDs(got.Data), c.want)
		}
	}

	// Scheduled deliveries expose their next attempt; others do not.
	for _, d := range e.listDeliveries(key, "").Data {
		if (d.Status == "scheduled") != (d.NextAttemptAt != nil) {
			t.Errorf("delivery %s status %s next_attempt_at %v", d.ID, d.Status, d.NextAttemptAt)
		}
		if d.Status == "dead" && (d.LastStatusCode == nil || *d.LastStatusCode != 500 || d.LastError == nil || d.CompletedAt == nil) {
			t.Errorf("dead delivery fields = %+v", d)
		}
		if d.EventType == "" {
			t.Errorf("event_type missing: %+v", d)
		}
	}

	// Events: date range.
	e.exec(`UPDATE events SET created_at = '2026-05-01T12:00:00Z' WHERE id = $1`, ev1)
	e.exec(`UPDATE events SET created_at = '2026-05-02T12:00:00Z' WHERE id = $1`, ev2)
	if b := e.listEvents(key, "created_after=2026-05-02T00:00:00Z"); len(b.Data) != 1 || b.Data[0].ID != ev2 {
		t.Errorf("events created_after = %+v", b.Data)
	}
	if b := e.listEvents(key, "created_before=2026-05-02T00:00:00Z"); len(b.Data) != 1 || b.Data[0].ID != ev1 {
		t.Errorf("events created_before = %+v", b.Data)
	}
}

// ======================= Delivery detail and attempts =======================

func TestGetDeliveryAndAttempts(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	ep := e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/a"})
	ev, ds := e.ingestEvent(key, "order.paid", `{"n":1}`)
	id := ds[0]

	// Pending, no attempts yet.
	res := e.call(http.MethodGet, "/v1/deliveries/"+id, key, nil)
	if res.Code != 200 {
		t.Fatalf("get: %d %s", res.Code, res.Body.String())
	}
	var det deliveryDetailResponse
	res.json(t, &det)
	if det.ID != id || det.EventID != ev || det.EventType != "order.paid" || det.Status != "pending" ||
		det.AttemptCount != 0 || det.MaxAttempts != 10 || det.NextAttemptAt == nil || det.CompletedAt != nil ||
		det.Retryable || det.Endpoint.ID != ep.ID || det.Endpoint.URL != ep.URL || !det.Endpoint.Enabled || det.Endpoint.Deleted {
		t.Fatalf("pending detail = %+v", det)
	}
	if contains(res.Body.String(), "secret") || contains(res.Body.String(), "locked_until") || contains(res.Body.String(), "queued_at") {
		t.Fatalf("detail exposes internal fields: %s", res.Body.String())
	}
	if ar := e.call(http.MethodGet, "/v1/deliveries/"+id+"/attempts", key, nil); ar.Code != 200 ||
		ar.Body.String() != "{\"data\":[],\"next_cursor\":null}\n" {
		t.Fatalf("no attempts = %d %s", ar.Code, ar.Body.String())
	}

	// Three failed attempts then dead.
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	e.setStatus(id, "dead", 3)
	e.addAttempt(id, 1, 120, "http_status", base)
	e.addAttempt(id, 2, 10000, "timeout", base.Add(time.Minute))
	e.addAttempt(id, 3, 15, "connection", base.Add(2*time.Minute))

	res = e.call(http.MethodGet, "/v1/deliveries/"+id, key, nil)
	res.json(t, &det)
	if det.Status != "dead" || det.AttemptCount != 3 || det.NextAttemptAt != nil || det.CompletedAt == nil || !det.Retryable {
		t.Fatalf("dead detail = %+v", det)
	}

	type attemptsBody struct {
		Data       []attemptItem `json:"data"`
		NextCursor *string       `json:"next_cursor"`
	}
	var p1, p2 attemptsBody
	e.call(http.MethodGet, "/v1/deliveries/"+id+"/attempts?limit=2", key, nil).json(t, &p1)
	if len(p1.Data) != 2 || p1.NextCursor == nil || p1.Data[0].AttemptNumber != 1 || p1.Data[1].AttemptNumber != 2 {
		t.Fatalf("attempts page 1 = %+v", p1)
	}
	a1 := p1.Data[0]
	if a1.Succeeded || a1.ResponseStatus == nil || *a1.ResponseStatus != 500 || a1.ResponseBody == nil || *a1.ResponseBody != "boom" ||
		a1.ErrorKind == nil || *a1.ErrorKind != "http_status" || a1.DurationMs != 120 || a1.FinishedAt != formatTime(base) {
		t.Fatalf("attempt 1 = %+v", a1)
	}
	if a2 := p1.Data[1]; a2.ResponseStatus != nil || *a2.ErrorKind != "timeout" || a2.ErrorMessage == nil {
		t.Fatalf("attempt 2 = %+v", a2)
	}
	e.call(http.MethodGet, "/v1/deliveries/"+id+"/attempts?limit=2&cursor="+*p1.NextCursor, key, nil).json(t, &p2)
	if len(p2.Data) != 1 || p2.NextCursor != nil || p2.Data[0].AttemptNumber != 3 {
		t.Fatalf("attempts page 2 = %+v", p2)
	}

	for _, path := range []string{
		"/v1/deliveries/00000000-0000-0000-0000-000000000000",
		"/v1/deliveries/nope",
		"/v1/deliveries/00000000-0000-0000-0000-000000000000/attempts",
		"/v1/deliveries/nope/attempts",
	} {
		if res := e.call(http.MethodGet, path, key, nil); res.Code != 404 || res.errCode(t) != "not_found" {
			t.Errorf("GET %s = %d %s", path, res.Code, res.Body.String())
		}
	}

	// A deleted endpoint's deliveries stay readable, flagged as deleted.
	if res := e.call(http.MethodDelete, "/v1/endpoints/"+ep.ID, key, nil); res.Code != 204 {
		t.Fatalf("delete endpoint: %d", res.Code)
	}
	e.call(http.MethodGet, "/v1/deliveries/"+id, key, nil).json(t, &det)
	if !det.Endpoint.Deleted || det.Retryable {
		t.Fatalf("after endpoint delete = %+v", det)
	}
}

// ======================= Tenant isolation =======================

func TestReadAPIsAreTenantScoped(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, keyA := e.project("a")
	_, keyB := e.project("b")
	epA := e.createEndpoint(keyA, map[string]any{"url": "https://a.example.com/h"})
	evA, dsA := e.ingestEvent(keyA, "order.paid", `{"secret_of_a":true}`)
	e.setStatus(dsA[0], "dead", 10)
	e.addAttempt(dsA[0], 1, 50, "http_status", time.Now())

	// B sees nothing of A's, by ID, by listing, or through filters naming A's IDs.
	for _, path := range []string{
		"/v1/events/" + evA,
		"/v1/deliveries/" + dsA[0],
		"/v1/deliveries/" + dsA[0] + "/attempts",
	} {
		if res := e.call(http.MethodGet, path, keyB, nil); res.Code != 404 {
			t.Errorf("B: GET %s = %d %s", path, res.Code, res.Body.String())
		}
	}
	if res := e.call(http.MethodPost, "/v1/deliveries/"+dsA[0]+"/retry", keyB, nil); res.Code != 404 {
		t.Errorf("B retried A's delivery: %d %s", res.Code, res.Body.String())
	}
	if got := e.count(`SELECT count(*) FROM deliveries WHERE id = $1 AND status = 'dead'`, dsA[0]); got != 1 {
		t.Fatal("B's retry attempt changed A's delivery")
	}
	if b := e.listEvents(keyB, ""); len(b.Data) != 0 {
		t.Errorf("B lists events: %+v", b)
	}
	for _, q := range []string{"", "endpoint_id=" + epA.ID, "event_id=" + evA, "status=dead"} {
		if b := e.listDeliveries(keyB, q); len(b.Data) != 0 {
			t.Errorf("B lists deliveries ?%s: %+v", q, b)
		}
	}
	var s statsResponse
	e.call(http.MethodGet, "/v1/stats", keyB, nil).json(t, &s)
	if s.Events.Total != 0 || s.Deliveries.Total != 0 || s.Attempts.Total != 0 {
		t.Errorf("B stats include A's data: %+v", s)
	}

	// A still sees its own.
	if b := e.listDeliveries(keyA, "status=dead"); len(b.Data) != 1 {
		t.Errorf("A lists = %+v", b)
	}
}

// ======================= Manual retry =======================

func TestRetryEligibility(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/a"})

	for _, st := range []string{"pending", "queued", "in_flight", "scheduled", "succeeded", "cancelled"} {
		_, ds := e.ingestEvent(key, "order.paid", `{}`)
		id := ds[0]
		if st != "pending" {
			e.setStatus(id, st, 1)
		}
		before := e.call(http.MethodGet, "/v1/deliveries/"+id, key, nil).Body.String()

		res := e.call(http.MethodPost, "/v1/deliveries/"+id+"/retry", key, nil)
		if res.Code != http.StatusConflict || res.errCode(t) != "delivery_not_retryable" {
			t.Errorf("retry of %s delivery = %d %s, want 409 delivery_not_retryable", st, res.Code, res.Body.String())
		}
		if after := e.call(http.MethodGet, "/v1/deliveries/"+id, key, nil).Body.String(); after != before {
			t.Errorf("refused retry of %s changed the delivery:\n%s\n%s", st, before, after)
		}
	}

	// Eligible: dead.
	_, ds := e.ingestEvent(key, "order.paid", `{}`)
	id := ds[0]
	e.setStatus(id, "dead", 10)
	for n := 1; n <= 10; n++ {
		e.addAttempt(id, n, 100, "http_status", time.Now())
	}
	res := e.call(http.MethodPost, "/v1/deliveries/"+id+"/retry", key, nil)
	if res.Code != http.StatusAccepted {
		t.Fatalf("retry dead = %d %s", res.Code, res.Body.String())
	}
	var r deliveryDetailResponse
	res.json(t, &r)
	if r.Status != "pending" || r.AttemptCount != 10 || r.MaxAttempts != 11 || r.CompletedAt != nil ||
		r.NextAttemptAt == nil || r.Retryable || r.LastStatusCode == nil || *r.LastStatusCode != 500 {
		t.Fatalf("retried delivery = %+v", r)
	}
	// Immediately due for the dispatcher (DB clock), history untouched.
	if got := e.count(`SELECT count(*) FROM deliveries WHERE id = $1 AND status = 'pending'
		AND next_attempt_at <= now() AND queued_at IS NULL AND locked_until IS NULL`, id); got != 1 {
		t.Fatal("retried delivery is not due for dispatch")
	}
	if got := e.count(`SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1`, id); got != 10 {
		t.Fatalf("attempt history = %d rows, want 10", got)
	}
	// A second retry while pending is refused.
	if res := e.call(http.MethodPost, "/v1/deliveries/"+id+"/retry", key, nil); res.Code != http.StatusConflict {
		t.Fatalf("second retry = %d", res.Code)
	}
	// The access log carries the delivery ID.
	if !contains(e.logBuf.String(), `"delivery_id":"`+id+`"`) {
		t.Error("manual retry not logged with delivery_id")
	}

	// Dead, but the endpoint was disabled: refused (the worker would cancel it).
	ep2 := e.createEndpoint(key, map[string]any{"url": "https://b.example.com/h", "event_types": []string{"only.b"}})
	evB, _ := e.ingestEvent(key, "only.b", `{}`) // also fans out to the catch-all endpoint
	var deadID string
	if err := e.db.Pool.QueryRow(context.Background(),
		`SELECT id::text FROM deliveries WHERE event_id = $1 AND endpoint_id = $2`, evB, ep2.ID).Scan(&deadID); err != nil {
		t.Fatal(err)
	}
	e.setStatus(deadID, "dead", 10)
	if res := e.call(http.MethodPatch, "/v1/endpoints/"+ep2.ID, key, map[string]any{"enabled": false}); res.Code != 200 {
		t.Fatalf("disable: %d", res.Code)
	}
	if res := e.call(http.MethodPost, "/v1/deliveries/"+deadID+"/retry", key, nil); res.Code != http.StatusConflict || res.errCode(t) != "endpoint_inactive" {
		t.Fatalf("retry with disabled endpoint = %d %s", res.Code, res.Body.String())
	}
	if res := e.call(http.MethodPatch, "/v1/endpoints/"+ep2.ID, key, map[string]any{"enabled": true}); res.Code != 200 {
		t.Fatalf("enable: %d", res.Code)
	}
	if res := e.call(http.MethodPost, "/v1/deliveries/"+deadID+"/retry", key, nil); res.Code != http.StatusAccepted {
		t.Fatalf("retry after re-enable = %d %s", res.Code, res.Body.String())
	}

	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "nope"} {
		if res := e.call(http.MethodPost, "/v1/deliveries/"+id+"/retry", key, nil); res.Code != 404 {
			t.Errorf("retry %s = %d", id, res.Code)
		}
	}
}

func TestConcurrentRetriesApplyOnce(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/a"})
	_, ds := e.ingestEvent(key, "order.paid", `{}`)
	id := ds[0]
	e.setStatus(id, "dead", 10)

	const n = 20
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			codes[i] = e.call(http.MethodPost, "/v1/deliveries/"+id+"/retry", key, nil).Code
		}()
	}
	close(start)
	wg.Wait()

	accepted, conflicts := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		default:
			t.Errorf("unexpected status %d", c)
		}
	}
	if accepted != 1 || conflicts != n-1 {
		t.Fatalf("accepted = %d, conflicts = %d; want exactly one retry", accepted, conflicts)
	}
	// One retry = one extra attempt allowed, not n.
	if got := e.count(`SELECT count(*) FROM deliveries WHERE id = $1 AND status = 'pending'
		AND attempt_count = 10 AND max_attempts = 11`, id); got != 1 {
		t.Fatal("concurrent retries corrupted the delivery state")
	}
}

// ======================= Stats =======================

func approx(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > 1e-9 {
		t.Errorf("%s = %v, want %v", name, deref(got), want)
	}
}

func deref(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}

func TestStats(t *testing.T) {
	t.Parallel()
	e := newAPIEnv(t)
	_, key := e.project("acme")
	_, keyOther := e.project("other")
	e.createEndpoint(key, map[string]any{"url": "https://hooks.example.com/a"})
	e.createEndpoint(keyOther, map[string]any{"url": "https://hooks.example.com/b"})

	const window = "from=2026-03-01T00:00:00Z&to=2026-03-02T00:00:00Z"
	in := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	out := time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC)

	// Empty project: zero counts, null rates and latencies.
	var empty statsResponse
	res := e.call(http.MethodGet, "/v1/stats?"+window, key, nil)
	if res.Code != 200 {
		t.Fatalf("stats: %d %s", res.Code, res.Body.String())
	}
	res.json(t, &empty)
	if empty.Events.Total != 0 || empty.Deliveries.Total != 0 || empty.Deliveries.SuccessRate != nil ||
		empty.Deliveries.FailureRate != nil || empty.Attempts.SuccessRate != nil ||
		empty.LatencyMs.SuccessfulAttempts.Count != 0 || empty.LatencyMs.SuccessfulAttempts.P50 != nil ||
		empty.LatencyMs.FailedAttempts.Avg != nil {
		t.Fatalf("empty stats = %+v", empty)
	}
	if empty.From != "2026-03-01T00:00:00Z" || empty.To != "2026-03-02T00:00:00Z" {
		t.Fatalf("window = %s..%s", empty.From, empty.To)
	}

	// Six deliveries in the window: 2 succeeded, 1 dead, 1 scheduled,
	// 1 in_flight, 1 cancelled. One more event + delivery outside it.
	statuses := []string{"succeeded", "succeeded", "dead", "scheduled", "in_flight", "cancelled"}
	ids := make([]string, len(statuses))
	for i, st := range statuses {
		ev, ds := e.ingestEvent(key, "order.paid", `{}`)
		ids[i] = ds[0]
		e.setStatus(ds[0], st, 1)
		e.exec(`UPDATE events SET created_at = $2 WHERE id = $1`, ev, in)
		e.exec(`UPDATE deliveries SET created_at = $2 WHERE id = $1`, ds[0], in)
	}
	evOut, dsOut := e.ingestEvent(key, "order.paid", `{}`)
	e.exec(`UPDATE events SET created_at = $2 WHERE id = $1`, evOut, out)
	e.exec(`UPDATE deliveries SET created_at = $2, status = 'succeeded', completed_at = now() WHERE id = $1`, dsOut[0], out)
	// The boundary is half-open: an event exactly at "to" is excluded.
	evEdge, _ := e.ingestEvent(key, "order.paid", `{}`)
	e.exec(`UPDATE events SET created_at = '2026-03-02T00:00:00Z' WHERE id = $1`, evEdge)
	e.exec(`UPDATE deliveries SET created_at = '2026-03-02T00:00:00Z' WHERE event_id = $1`, evEdge)

	// Attempts finished in the window:
	//   successes: 10, 20, 30, 40, 100 ms
	//   failures:  200, 400 ms (http_status), plus a worker_lost of 60 s
	//   (counted as a failed attempt, excluded from latency).
	for i, ms := range []int{10, 20, 30, 40, 100} {
		e.addAttempt(ids[i%2], i+1, ms, "", in.Add(time.Duration(i)*time.Minute))
	}
	e.addAttempt(ids[2], 1, 200, "http_status", in)
	e.addAttempt(ids[2], 2, 400, "http_status", in.Add(time.Minute))
	e.addAttempt(ids[2], 3, 60000, "worker_lost", in.Add(2*time.Minute))
	// Outside the window: must not count.
	e.addAttempt(dsOut[0], 1, 5, "", out)
	e.addAttempt(ids[3], 1, 99999, "timeout", out)

	// Noise in another project.
	_, dsOther := e.ingestEvent(keyOther, "order.paid", `{}`)
	e.exec(`UPDATE events SET created_at = $2 WHERE project_id = (SELECT project_id FROM deliveries WHERE id = $1)`, dsOther[0], in)
	e.exec(`UPDATE deliveries SET created_at = $2, status = 'dead', completed_at = now(), attempt_count = 1 WHERE id = $1`, dsOther[0], in)
	e.addAttempt(dsOther[0], 1, 7, "", in)

	var s statsResponse
	res = e.call(http.MethodGet, "/v1/stats?"+window, key, nil)
	if res.Code != 200 {
		t.Fatalf("stats: %d %s", res.Code, res.Body.String())
	}
	res.json(t, &s)

	if s.Events.Total != 6 {
		t.Errorf("events = %d, want 6", s.Events.Total)
	}
	d := s.Deliveries
	if d.Total != 6 || d.Succeeded != 2 || d.Dead != 1 || d.Scheduled != 1 || d.InFlight != 1 ||
		d.Cancelled != 1 || d.Pending != 0 || d.Queued != 0 {
		t.Errorf("deliveries = %+v", d)
	}
	approx(t, "success_rate", d.SuccessRate, 2.0/3.0)
	approx(t, "failure_rate", d.FailureRate, 1.0/3.0)

	if s.Attempts.Total != 8 || s.Attempts.Succeeded != 5 || s.Attempts.Failed != 3 {
		t.Errorf("attempts = %+v", s.Attempts)
	}
	approx(t, "attempt success_rate", s.Attempts.SuccessRate, 5.0/8.0)

	ok := s.LatencyMs.SuccessfulAttempts
	if ok.Count != 5 {
		t.Errorf("successful latency count = %d", ok.Count)
	}
	approx(t, "success avg", ok.Avg, 40)
	approx(t, "success p50", ok.P50, 30)
	approx(t, "success p95", ok.P95, 88) // 40 + 0.8 * (100 - 40), linear interpolation

	bad := s.LatencyMs.FailedAttempts
	if bad.Count != 2 {
		t.Errorf("failed latency count = %d (worker_lost must be excluded)", bad.Count)
	}
	approx(t, "failed avg", bad.Avg, 300)
	approx(t, "failed p50", bad.P50, 300)
	approx(t, "failed p95", bad.P95, 390)

	// The default window is the last 24 hours: none of the data above is in it.
	var recent statsResponse
	e.call(http.MethodGet, "/v1/stats", key, nil).json(t, &recent)
	from, err1 := time.Parse(time.RFC3339Nano, recent.From)
	to, err2 := time.Parse(time.RFC3339Nano, recent.To)
	if err1 != nil || err2 != nil || to.Sub(from) != 24*time.Hour || time.Since(to) > time.Minute {
		t.Errorf("default window = %s..%s", recent.From, recent.To)
	}
	if recent.Events.Total != 0 || recent.Deliveries.Total != 0 || recent.Attempts.Total != 0 {
		t.Errorf("default window stats = %+v", recent)
	}
}
