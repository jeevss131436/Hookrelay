package mockreceiver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/signing"
)

func post(t *testing.T, h http.Handler, target string, hdr http.Header, body []byte) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	for k, v := range hdr {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestReceiverVerifiesSignaturesAndCountsDuplicates(t *testing.T) {
	secret := secrets.GenerateSigningSecret()
	rc := New(secret, nil)
	body := []byte(`{"a":1}`)
	hdr, err := signing.Sign(secret, "dlv_1", time.Now(), body)
	if err != nil {
		t.Fatal(err)
	}

	if code := post(t, rc, "/hook", hdr, body); code != http.StatusOK {
		t.Fatalf("valid signature: status %d", code)
	}
	if code := post(t, rc, "/hook", hdr, body); code != http.StatusOK {
		t.Fatalf("duplicate: status %d", code)
	}
	if code := post(t, rc, "/hook", hdr, []byte(`{"a":2}`)); code != http.StatusUnauthorized {
		t.Fatalf("tampered body: status %d, want 401", code)
	}
	if rc.Count("dlv_1") != 3 || rc.Duplicates() != 2 {
		t.Fatalf("count = %d, duplicates = %d", rc.Count("dlv_1"), rc.Duplicates())
	}
	reqs := rc.Requests()
	if !reqs[0].SignatureValid || reqs[2].SignatureValid {
		t.Fatalf("SignatureValid flags wrong: %+v", reqs)
	}
}

func TestReceiverFailureKnobs(t *testing.T) {
	rc := New("", nil)
	if code := post(t, rc, "/hook?status=503", nil, []byte(`{}`)); code != 503 {
		t.Fatalf("status knob: %d", code)
	}
	if code := post(t, rc, "/hook?fail_rate=1", nil, []byte(`{}`)); code != 500 {
		t.Fatalf("fail_rate=1: %d", code)
	}
	start := time.Now()
	if code := post(t, rc, "/hook?latency_ms=50", nil, []byte(`{}`)); code != 200 {
		t.Fatalf("latency: %d", code)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("latency_ms not applied")
	}
}
