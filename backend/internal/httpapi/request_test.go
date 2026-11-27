package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

func TestDecodeJSON(t *testing.T) {
	type body struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	tests := []struct {
		name       string
		ct         string
		body       string
		max        int64
		wantStatus int
		wantCode   string
	}{
		{"ok", "application/json", `{"name":"a","count":1}`, 1024, 0, ""},
		{"ok with charset", "application/json; charset=utf-8", `{"name":"a"}`, 1024, 0, ""},
		{"wrong content type", "text/plain", `{"name":"a"}`, 1024, 415, "unsupported_media_type"},
		{"missing content type", "", `{"name":"a"}`, 1024, 415, "unsupported_media_type"},
		{"empty", "application/json", ``, 1024, 400, "invalid_json"},
		{"syntax", "application/json", `{"name":`, 1024, 400, "invalid_json"},
		{"not object", "application/json", `[1,2]`, 1024, 400, "invalid_json"},
		{"unknown field", "application/json", `{"nam":"a"}`, 1024, 400, "invalid_json"},
		{"wrong type", "application/json", `{"count":"x"}`, 1024, 422, "validation_failed"},
		{"trailing data", "application/json", `{"name":"a"} {"name":"b"}`, 1024, 400, "invalid_json"},
		{"too large", "application/json", `{"name":"` + strings.Repeat("a", 100) + `"}`, 50, 413, "payload_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			if tt.ct != "" {
				r.Header.Set("Content-Type", tt.ct)
			}
			var dst body
			aerr := decodeJSON(httptest.NewRecorder(), r, tt.max, &dst)
			if tt.wantStatus == 0 {
				if aerr != nil {
					t.Fatalf("unexpected error: %v", aerr)
				}
				return
			}
			if aerr == nil || aerr.status != tt.wantStatus || aerr.code != tt.wantCode {
				t.Fatalf("got %+v, want %d %s", aerr, tt.wantStatus, tt.wantCode)
			}
		})
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := &endpoints.Cursor{CreatedAt: time.Date(2026, 11, 27, 5, 1, 2, 123456000, time.UTC), ID: store.NewUUID()}
	s := encodeCursor(c)
	if s == nil {
		t.Fatal("nil cursor string")
	}
	got, err := decodeCursor(*s)
	if err != nil || !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	if encodeCursor(nil) != nil {
		t.Error("nil cursor must encode to nil")
	}
	if got, err := decodeCursor(""); got != nil || err != nil {
		t.Error("empty cursor must decode to nil, nil")
	}
	for _, bad := range []string{"!!!", "bm90LWEtY3Vyc29y", "MjAyNi0xMS0yN3xub3QtYS11dWlk"} {
		if _, err := decodeCursor(bad); err == nil {
			t.Errorf("decodeCursor(%q) succeeded", bad)
		}
	}
}

func TestParseLimit(t *testing.T) {
	if n, err := parseLimit(""); err != nil || n != endpoints.DefaultPageSize {
		t.Errorf("default = %d, %v", n, err)
	}
	if n, err := parseLimit("100"); err != nil || n != 100 {
		t.Errorf("100 = %d, %v", n, err)
	}
	for _, bad := range []string{"0", "101", "-1", "abc", "1.5"} {
		if _, err := parseLimit(bad); err == nil {
			t.Errorf("parseLimit(%q) succeeded", bad)
		}
	}
}
