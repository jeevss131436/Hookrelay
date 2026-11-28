package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/history"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// Read APIs (events, deliveries, attempts), manual retry and stats. See
// docs/API.md. Query parameters are validated strictly: unknown parameters
// and malformed values are 400 invalid_parameter, so a typo in a filter
// never silently returns unfiltered data.

// ---- query parameter parsing ----

type queryParams struct {
	v   url.Values
	err error
}

// newQuery rejects a malformed query string, parameters outside allowed and
// repeated parameters. (r.URL.Query() silently drops what it cannot parse,
// which would turn a broken filter into an unfiltered listing.)
func newQuery(r *http.Request, allowed ...string) *queryParams {
	v, err := url.ParseQuery(r.URL.RawQuery)
	q := &queryParams{v: v}
	if err != nil {
		q.fail("query string is malformed")
		return q
	}
	ok := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		ok[a] = true
	}
	names := make([]string, 0, len(q.v))
	for k := range q.v {
		names = append(names, k)
	}
	sort.Strings(names) // deterministic error message
	for _, k := range names {
		switch {
		case !ok[k]:
			q.fail("unknown query parameter %q", k)
		case len(q.v[k]) > 1:
			q.fail("query parameter %q must be given at most once", k)
		}
	}
	return q
}

func (q *queryParams) fail(format string, args ...any) {
	if q.err == nil {
		q.err = fmt.Errorf(format, args...)
	}
}

func (q *queryParams) str(name string) string { return q.v.Get(name) }

func (q *queryParams) limit() int {
	n, err := parseLimit(q.str("limit"))
	if err != nil {
		q.fail("%s", err.Error())
	}
	return n
}

func (q *queryParams) cursor() *history.Cursor {
	c, err := decodeCursor(q.str("cursor"))
	if err != nil {
		q.fail("cursor is invalid")
		return nil
	}
	if c == nil {
		return nil
	}
	hc := history.Cursor(*c)
	return &hc
}

func (q *queryParams) time(name string) *time.Time {
	s := q.str(name)
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		q.fail("%s must be an RFC 3339 timestamp, e.g. 2026-11-27T00:00:00Z", name)
		return nil
	}
	return &t
}

// timeRange reads created_after (inclusive) and created_before (exclusive).
func (q *queryParams) timeRange() history.TimeRange {
	tr := history.TimeRange{After: q.time("created_after"), Before: q.time("created_before")}
	if tr.After != nil && tr.Before != nil && !tr.After.Before(*tr.Before) {
		q.fail("created_after must be earlier than created_before")
	}
	return tr
}

func (q *queryParams) uuid(name string) pgtype.UUID {
	s := q.str(name)
	if s == "" {
		return pgtype.UUID{}
	}
	id, ok := parseID(s)
	if !ok {
		q.fail("%s must be a UUID", name)
	}
	return id
}

func (q *queryParams) eventType() *string {
	s := q.str("event_type")
	if s == "" {
		return nil
	}
	if !endpoints.EventTypePattern.MatchString(s) {
		q.fail("event_type must be 1-100 characters of a-z A-Z 0-9 . _ -")
		return nil
	}
	return &s
}

// statuses parses a comma-separated list of delivery statuses.
func (q *queryParams) statuses() []sqlcgen.DeliveryStatus {
	s := q.str("status")
	if s == "" {
		return nil
	}
	var out []sqlcgen.DeliveryStatus
	seen := map[sqlcgen.DeliveryStatus]bool{}
	for _, part := range strings.Split(s, ",") {
		st := sqlcgen.DeliveryStatus(strings.TrimSpace(part))
		if !st.Valid() {
			q.fail("status must be a comma-separated list of: %s", strings.Join(deliveryStatusNames(), ", "))
			return nil
		}
		if !seen[st] {
			seen[st] = true
			out = append(out, st)
		}
	}
	return out
}

func deliveryStatusNames() []string {
	all := sqlcgen.AllDeliveryStatusValues()
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = string(s)
	}
	return out
}

func (a *API) badParam(w http.ResponseWriter, err error) {
	writeError(w, http.StatusBadRequest, "invalid_parameter", err.Error())
}

func optTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := formatTime(*t)
	return &s
}

func encodeHistoryCursor(c *history.Cursor) *string {
	if c == nil {
		return nil
	}
	ec := endpoints.Cursor(*c)
	return encodeCursor(&ec)
}

// ---- events ----

type eventListItem struct {
	ID             string  `json:"id"`
	EventType      string  `json:"event_type"`
	IdempotencyKey *string `json:"idempotency_key"`
	PayloadSize    int32   `json:"payload_size"`
	CreatedAt      string  `json:"created_at"`
}

type listEventsResponse struct {
	Data       []eventListItem `json:"data"`
	NextCursor *string         `json:"next_cursor"`
}

func (a *API) handleListEvents(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	q := newQuery(r, "limit", "cursor", "event_type", "created_after", "created_before")
	limit, cursor := q.limit(), q.cursor()
	filter := history.EventFilter{EventType: q.eventType(), Created: q.timeRange()}
	if q.err != nil {
		a.badParam(w, q.err)
		return
	}
	page, err := a.history.ListEvents(r.Context(), p.ProjectID, filter, cursor, limit)
	if err != nil {
		a.writeServiceError(w, r, "list events", err)
		return
	}
	resp := listEventsResponse{Data: make([]eventListItem, len(page.Events)), NextCursor: encodeHistoryCursor(page.NextCursor)}
	for i, e := range page.Events {
		resp.Data[i] = eventListItem{
			ID: e.ID.String(), EventType: e.EventType, IdempotencyKey: e.IdempotencyKey,
			PayloadSize: e.PayloadSize, CreatedAt: formatTime(e.CreatedAt),
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

type eventDeliveryItem struct {
	ID             string  `json:"id"`
	EndpointID     string  `json:"endpoint_id"`
	EndpointURL    string  `json:"endpoint_url"`
	Status         string  `json:"status"`
	AttemptCount   int32   `json:"attempt_count"`
	LastStatusCode *int32  `json:"last_status_code"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	CompletedAt    *string `json:"completed_at"`
}

type eventDetailResponse struct {
	ID                  string              `json:"id"`
	EventType           string              `json:"event_type"`
	IdempotencyKey      *string             `json:"idempotency_key"`
	CreatedAt           string              `json:"created_at"`
	Deliveries          []eventDeliveryItem `json:"deliveries"`
	DeliveriesTruncated bool                `json:"deliveries_truncated"`
	// Payload is written by writeEventDetail, not by encoding/json.
	Payload json.RawMessage `json:"-"`
}

// writeEventDetail writes the event with "payload" set to the exact stored
// bytes. encoding/json would compact and HTML-escape a json.RawMessage, and
// receivers verify signatures over the exact bytes (ARCHITECTURE D5), so the
// payload is spliced in verbatim. It is valid JSON: ingest only stores a
// payload that decoded as a JSON object.
func writeEventDetail(w http.ResponseWriter, resp eventDetailResponse) error {
	head, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	body := make([]byte, 0, len(head)+len(resp.Payload)+16)
	body = append(body, head[:len(head)-1]...) // drop the closing brace
	body = append(body, `,"payload":`...)
	body = append(body, resp.Payload...)
	body = append(body, "}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body) // the client may have gone away
	return nil
}

func (a *API) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "event not found")
		return
	}
	ev, err := a.history.GetEvent(r.Context(), p.ProjectID, id)
	if err != nil {
		a.writeServiceError(w, r, "get event", err)
		return
	}
	resp := eventDetailResponse{
		ID: ev.ID.String(), EventType: ev.EventType, IdempotencyKey: ev.IdempotencyKey,
		CreatedAt: formatTime(ev.CreatedAt), Payload: ev.Payload,
		Deliveries:          make([]eventDeliveryItem, len(ev.Deliveries)),
		DeliveriesTruncated: ev.DeliveriesTruncated,
	}
	for i, d := range ev.Deliveries {
		resp.Deliveries[i] = eventDeliveryItem{
			ID: d.ID.String(), EndpointID: d.EndpointID.String(), EndpointURL: d.EndpointUrl,
			Status: string(d.Status), AttemptCount: d.AttemptCount, LastStatusCode: d.LastStatusCode,
			CreatedAt: formatTime(d.CreatedAt), UpdatedAt: formatTime(d.UpdatedAt), CompletedAt: optTime(d.CompletedAt),
		}
	}
	if err := writeEventDetail(w, resp); err != nil {
		a.internalError(w, r, "get event", err)
	}
}

// ---- deliveries ----

type deliveryItem struct {
	ID           string `json:"id"`
	EventID      string `json:"event_id"`
	EventType    string `json:"event_type"`
	EndpointID   string `json:"endpoint_id"`
	Status       string `json:"status"`
	AttemptCount int32  `json:"attempt_count"`
	MaxAttempts  int32  `json:"max_attempts"`
	// When the next attempt becomes due; null unless pending or scheduled.
	NextAttemptAt  *string `json:"next_attempt_at"`
	LastStatusCode *int32  `json:"last_status_code"`
	LastError      *string `json:"last_error"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	CompletedAt    *string `json:"completed_at"`
}

func nextAttemptAt(status sqlcgen.DeliveryStatus, t time.Time) *string {
	if status != sqlcgen.DeliveryStatusPending && status != sqlcgen.DeliveryStatusScheduled {
		return nil
	}
	s := formatTime(t)
	return &s
}

type listDeliveriesResponse struct {
	Data       []deliveryItem `json:"data"`
	NextCursor *string        `json:"next_cursor"`
}

func (a *API) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	q := newQuery(r, "limit", "cursor", "status", "endpoint_id", "event_id", "event_type",
		"created_after", "created_before")
	limit, cursor := q.limit(), q.cursor()
	filter := history.DeliveryFilter{
		Statuses: q.statuses(), EndpointID: q.uuid("endpoint_id"), EventID: q.uuid("event_id"),
		EventType: q.eventType(), Created: q.timeRange(),
	}
	if q.err != nil {
		a.badParam(w, q.err)
		return
	}
	page, err := a.history.ListDeliveries(r.Context(), p.ProjectID, filter, cursor, limit)
	if err != nil {
		a.writeServiceError(w, r, "list deliveries", err)
		return
	}
	resp := listDeliveriesResponse{Data: make([]deliveryItem, len(page.Deliveries)), NextCursor: encodeHistoryCursor(page.NextCursor)}
	for i, d := range page.Deliveries {
		resp.Data[i] = deliveryItem{
			ID: d.ID.String(), EventID: d.EventID.String(), EventType: d.EventType, EndpointID: d.EndpointID.String(),
			Status: string(d.Status), AttemptCount: d.AttemptCount, MaxAttempts: d.MaxAttempts,
			NextAttemptAt: nextAttemptAt(d.Status, d.NextAttemptAt), LastStatusCode: d.LastStatusCode,
			LastError: d.LastError, CreatedAt: formatTime(d.CreatedAt), UpdatedAt: formatTime(d.UpdatedAt),
			CompletedAt: optTime(d.CompletedAt),
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

type deliveryEndpoint struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Enabled bool   `json:"enabled"`
	Deleted bool   `json:"deleted"`
}

type deliveryDetailResponse struct {
	deliveryItem
	Endpoint deliveryEndpoint `json:"endpoint"`
	// Whether POST /v1/deliveries/{id}/retry would currently be accepted.
	Retryable bool `json:"retryable"`
}

func toDeliveryDetail(d history.DeliveryDetail) deliveryDetailResponse {
	return deliveryDetailResponse{
		deliveryItem: deliveryItem{
			ID: d.ID.String(), EventID: d.EventID.String(), EventType: d.EventType, EndpointID: d.EndpointID.String(),
			Status: string(d.Status), AttemptCount: d.AttemptCount, MaxAttempts: d.MaxAttempts,
			NextAttemptAt: nextAttemptAt(d.Status, d.NextAttemptAt), LastStatusCode: d.LastStatusCode,
			LastError: d.LastError, CreatedAt: formatTime(d.CreatedAt), UpdatedAt: formatTime(d.UpdatedAt),
			CompletedAt: optTime(d.CompletedAt),
		},
		Endpoint: deliveryEndpoint{
			ID: d.EndpointID.String(), URL: d.EndpointUrl, Enabled: d.EndpointEnabled, Deleted: d.EndpointDeleted,
		},
		Retryable: d.Status == sqlcgen.DeliveryStatusDead && d.EndpointEnabled && !d.EndpointDeleted,
	}
}

func (a *API) handleGetDelivery(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	d, err := a.history.GetDelivery(r.Context(), p.ProjectID, id)
	if err != nil {
		a.writeServiceError(w, r, "get delivery", err)
		return
	}
	writeJSON(w, http.StatusOK, toDeliveryDetail(d))
}

// handleRetryDelivery makes a dead delivery due again (202). It never
// enqueues to Redis; the dispatcher picks the delivery up.
func (a *API) handleRetryDelivery(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	d, err := a.history.Retry(r.Context(), p.ProjectID, id)
	if err != nil {
		a.writeServiceError(w, r, "retry delivery", err)
		return
	}
	a.log.InfoContext(r.Context(), "delivery manually retried",
		"request_id", RequestIDFromContext(r.Context()), "delivery_id", d.ID.String(),
		"project_id", p.ProjectID.String(), "attempt_count", d.AttemptCount)
	writeJSON(w, http.StatusAccepted, toDeliveryDetail(d))
}

// ---- attempts ----

type attemptItem struct {
	AttemptNumber int32  `json:"attempt_number"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	DurationMs    int32  `json:"duration_ms"`
	// True when the receiver answered 2xx.
	Succeeded      bool    `json:"succeeded"`
	ResponseStatus *int32  `json:"response_status"`
	ResponseBody   *string `json:"response_body"`
	ErrorKind      *string `json:"error_kind"`
	ErrorMessage   *string `json:"error_message"`
}

type listAttemptsResponse struct {
	Data       []attemptItem `json:"data"`
	NextCursor *string       `json:"next_cursor"`
}

// Attempt cursors are opaque: base64url("attempt|<n>").
func encodeAttemptCursor(n *int32) *string {
	if n == nil {
		return nil
	}
	s := base64.RawURLEncoding.EncodeToString([]byte("attempt|" + strconv.Itoa(int(*n))))
	return &s
}

func decodeAttemptCursor(s string) (int32, error) {
	if s == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return 0, errors.New("cursor is invalid")
	}
	num, ok := strings.CutPrefix(string(raw), "attempt|")
	if !ok {
		return 0, errors.New("cursor is invalid")
	}
	n, err := strconv.ParseInt(num, 10, 32)
	if err != nil || n < 0 {
		return 0, errors.New("cursor is invalid")
	}
	return int32(n), nil
}

func (a *API) handleListAttempts(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	q := newQuery(r, "limit", "cursor")
	limit := q.limit()
	after, err := decodeAttemptCursor(q.str("cursor"))
	if err != nil {
		q.fail("%s", err.Error())
	}
	if q.err != nil {
		a.badParam(w, q.err)
		return
	}
	page, err := a.history.ListAttempts(r.Context(), p.ProjectID, id, after, limit)
	if err != nil {
		a.writeServiceError(w, r, "list attempts", err)
		return
	}
	resp := listAttemptsResponse{Data: make([]attemptItem, len(page.Attempts)), NextCursor: encodeAttemptCursor(page.NextAfter)}
	for i, at := range page.Attempts {
		resp.Data[i] = attemptItem{
			AttemptNumber: at.AttemptNumber, StartedAt: formatTime(at.StartedAt), FinishedAt: formatTime(at.FinishedAt),
			DurationMs: at.DurationMs, Succeeded: at.ErrorKind == nil, ResponseStatus: at.ResponseStatus,
			ResponseBody: at.ResponseBody, ErrorKind: at.ErrorKind, ErrorMessage: at.ErrorMessage,
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---- stats ----

type latencyStats struct {
	Count int64    `json:"count"`
	Avg   *float64 `json:"avg"`
	P50   *float64 `json:"p50"`
	P95   *float64 `json:"p95"`
}

type statsResponse struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Events struct {
		Total int64 `json:"total"`
	} `json:"events"`
	Deliveries struct {
		Total       int64    `json:"total"`
		Pending     int64    `json:"pending"`
		Queued      int64    `json:"queued"`
		InFlight    int64    `json:"in_flight"`
		Scheduled   int64    `json:"scheduled"`
		Succeeded   int64    `json:"succeeded"`
		Dead        int64    `json:"dead"`
		Cancelled   int64    `json:"cancelled"`
		SuccessRate *float64 `json:"success_rate"`
		FailureRate *float64 `json:"failure_rate"`
	} `json:"deliveries"`
	Attempts struct {
		Total       int64    `json:"total"`
		Succeeded   int64    `json:"succeeded"`
		Failed      int64    `json:"failed"`
		SuccessRate *float64 `json:"success_rate"`
	} `json:"attempts"`
	LatencyMs struct {
		SuccessfulAttempts latencyStats `json:"successful_attempts"`
		FailedAttempts     latencyStats `json:"failed_attempts"`
	} `json:"latency_ms"`
}

func toLatency(l history.Latency) latencyStats {
	return latencyStats{Count: l.Count, Avg: l.Avg, P50: l.P50, P95: l.P95}
}

// statsWindow resolves from/to: to defaults to now, from to 24 h before to.
func statsWindow(q *queryParams, now time.Time) (time.Time, time.Time) {
	from, to := q.time("from"), q.time("to")
	end := now
	if to != nil {
		end = *to
	}
	start := end.Add(-history.DefaultStatsWindow)
	if from != nil {
		start = *from
	}
	switch {
	case !start.Before(end):
		q.fail("from must be earlier than to")
	case end.Sub(start) > history.MaxStatsWindow:
		q.fail("the window from..to must be at most %d days", int(history.MaxStatsWindow/(24*time.Hour)))
	}
	return start, end
}

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	q := newQuery(r, "from", "to")
	from, to := statsWindow(q, time.Now())
	if q.err != nil {
		a.badParam(w, q.err)
		return
	}
	s, err := a.history.Stats(r.Context(), p.ProjectID, from, to)
	if err != nil {
		a.writeServiceError(w, r, "stats", err)
		return
	}
	var resp statsResponse
	resp.From, resp.To = formatTime(s.From), formatTime(s.To)
	resp.Events.Total = s.Events
	d := &resp.Deliveries
	d.Total, d.Pending, d.Queued, d.InFlight = s.Deliveries.Total, s.Deliveries.Pending, s.Deliveries.Queued, s.Deliveries.InFlight
	d.Scheduled, d.Succeeded, d.Dead, d.Cancelled = s.Deliveries.Scheduled, s.Deliveries.Succeeded, s.Deliveries.Dead, s.Deliveries.Cancelled
	d.SuccessRate, d.FailureRate = s.SuccessRate, s.FailureRate
	resp.Attempts.Total, resp.Attempts.Succeeded, resp.Attempts.Failed = s.Attempts, s.AttemptsSucceeded, s.AttemptsFailed
	resp.Attempts.SuccessRate = s.AttemptSuccessRate
	resp.LatencyMs.SuccessfulAttempts = toLatency(s.SuccessLatency)
	resp.LatencyMs.FailedAttempts = toLatency(s.FailureLatency)
	writeJSON(w, http.StatusOK, resp)
}
