package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func okCheck(name string) Check {
	return Check{Name: name, Ping: func(context.Context) error { return nil }}
}

func failCheck(name string) Check {
	return Check{Name: name, Ping: func(context.Context) error {
		return errors.New("dial tcp 10.0.0.5:5432: connection refused")
	}}
}

func do(t *testing.T, h http.Handler, method, path string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	h := NewRouter(discardLogger(), []Check{failCheck("postgres")})
	rec := do(t, h, http.MethodGet, "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (liveness must not depend on dependencies)", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name       string
		checks     []Check
		wantCode   int
		wantStatus string
		wantChecks map[string]string
	}{
		{
			name:       "all healthy",
			checks:     []Check{okCheck("postgres"), okCheck("redis")},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{"postgres": "ok", "redis": "ok"},
		},
		{
			name:       "one failing",
			checks:     []Check{okCheck("postgres"), failCheck("redis")},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "unavailable",
			wantChecks: map[string]string{"postgres": "ok", "redis": "unavailable"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, NewRouter(discardLogger(), tt.checks), http.MethodGet, "/readyz", nil)
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			var body readyResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Status != tt.wantStatus {
				t.Errorf("status field = %q, want %q", body.Status, tt.wantStatus)
			}
			for k, v := range tt.wantChecks {
				if body.Checks[k] != v {
					t.Errorf("checks[%s] = %q, want %q", k, body.Checks[k], v)
				}
			}
			if strings.Contains(rec.Body.String(), "10.0.0.5") {
				t.Error("readyz response leaks dependency error details")
			}
		})
	}
}

func TestReadyzHonorsCheckTimeout(t *testing.T) {
	hang := Check{Name: "postgres", Ping: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	rec := do(t, NewRouter(discardLogger(), []Check{hang}), http.MethodGet, "/readyz", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestRequestID(t *testing.T) {
	h := NewRouter(discardLogger(), nil)

	t.Run("generated when absent", func(t *testing.T) {
		rec := do(t, h, http.MethodGet, "/healthz", nil)
		id := rec.Header().Get(RequestIDHeader)
		if len(id) != 32 {
			t.Errorf("generated id %q, want 32 hex chars", id)
		}
	})

	t.Run("unique per request", func(t *testing.T) {
		a := do(t, h, http.MethodGet, "/healthz", nil).Header().Get(RequestIDHeader)
		b := do(t, h, http.MethodGet, "/healthz", nil).Header().Get(RequestIDHeader)
		if a == b {
			t.Errorf("ids not unique: %q", a)
		}
	})

	t.Run("valid incoming id is reused", func(t *testing.T) {
		rec := do(t, h, http.MethodGet, "/healthz", http.Header{RequestIDHeader: {"abc-123_X.y"}})
		if got := rec.Header().Get(RequestIDHeader); got != "abc-123_X.y" {
			t.Errorf("id = %q, want incoming id", got)
		}
	})

	for _, bad := range []string{"has space", "<script>", strings.Repeat("a", 65), "new\nline"} {
		t.Run("invalid incoming id replaced: "+bad, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, "/healthz", http.Header{RequestIDHeader: {bad}})
			if got := rec.Header().Get(RequestIDHeader); got == bad || len(got) != 32 {
				t.Errorf("id = %q, want freshly generated id", got)
			}
		})
	}
}

func TestRequestIDInContextAndLogs(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	var seen string
	h := RequestID(Logger(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusTeapot)
	})))
	rec := do(t, h, http.MethodGet, "/x", http.Header{RequestIDHeader: {"trace-1"}})

	if seen != "trace-1" {
		t.Errorf("context id = %q, want trace-1", seen)
	}
	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d", rec.Code)
	}
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line is not JSON: %v (%q)", err, buf.String())
	}
	if line["request_id"] != "trace-1" || line["status"] != float64(http.StatusTeapot) || line["path"] != "/x" {
		t.Errorf("unexpected log line: %v", line)
	}
}

func TestRecovererReturns500(t *testing.T) {
	h := RequestID(Recoverer(discardLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	rec := do(t, h, http.MethodGet, "/", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Code != "internal_error" {
		t.Errorf("unexpected body %q (err %v)", rec.Body.String(), err)
	}
}

func TestNotFoundUsesErrorEnvelope(t *testing.T) {
	rec := do(t, NewRouter(discardLogger(), nil), http.MethodGet, "/nope", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Code != "not_found" {
		t.Errorf("unexpected body %q", rec.Body.String())
	}
}

func TestMethodNotAllowed(t *testing.T) {
	rec := do(t, NewRouter(discardLogger(), nil), http.MethodPost, "/healthz", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
