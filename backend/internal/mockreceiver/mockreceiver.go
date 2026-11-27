// Package mockreceiver is a webhook receiver for local testing and tests
// (IMPLEMENTATION_PLAN Phase 3). It verifies Standard Webhooks signatures,
// records every request, counts duplicate webhook-ids, and can be told to
// misbehave through query parameters on the endpoint URL:
//
//	?status=500       respond with this status (default 200)
//	?latency_ms=2000  wait this long before responding
//	?fail_rate=0.3    respond 500 with this probability
//
// It is a test tool: never expose it to the internet.
package mockreceiver

import (
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jeevss131436/Hookrelay/backend/internal/signing"
)

// maxBody bounds request bodies (HookRelay payloads are at most 256 KB).
const maxBody = 1 << 20

// Request is one received webhook.
type Request struct {
	WebhookID  string
	Timestamp  string
	Signature  string
	Header     http.Header
	Body       []byte
	ReceivedAt time.Time
	// SignatureValid is true when a secret is configured and the signature
	// verified. Requests with an invalid signature are answered 401.
	SignatureValid bool
	Status         int // the status the receiver answered with
}

// Receiver is an http.Handler. It is safe for concurrent use.
type Receiver struct {
	secret string
	log    *slog.Logger

	mu       sync.Mutex
	requests []Request
	seen     map[string]int
}

// New returns a Receiver. With an empty secret, signatures are not checked.
func New(secret string, log *slog.Logger) *Receiver {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Receiver{secret: secret, log: log, seen: map[string]int{}}
}

// ServeHTTP handles one webhook.
func (rc *Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	req := Request{
		WebhookID:  r.Header.Get(signing.HeaderID),
		Timestamp:  r.Header.Get(signing.HeaderTimestamp),
		Signature:  r.Header.Get(signing.HeaderSignature),
		Header:     r.Header.Clone(),
		Body:       body,
		ReceivedAt: time.Now(),
	}

	status, sigErr := rc.decide(r, &req)
	req.Status = status

	rc.mu.Lock()
	rc.seen[req.WebhookID]++
	dup := rc.seen[req.WebhookID]
	rc.requests = append(rc.requests, req)
	rc.mu.Unlock()

	attrs := []any{
		slog.String("webhook_id", req.WebhookID),
		slog.Int("times_seen", dup),
		slog.Int("status", status),
		slog.Int("bytes", len(body)),
	}
	if sigErr != nil {
		attrs = append(attrs, slog.String("signature_error", sigErr.Error()))
	}
	if dup > 1 {
		rc.log.Warn("duplicate webhook received", attrs...)
	} else {
		rc.log.Info("webhook received", attrs...)
	}

	if lat, ok := intParam(r, "latency_ms"); ok && lat > 0 {
		select {
		case <-time.After(time.Duration(lat) * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, http.StatusText(status))
}

func (rc *Receiver) decide(r *http.Request, req *Request) (int, error) {
	if rc.secret != "" {
		if err := signing.Verify(rc.secret, r.Header, req.Body, time.Now(), 0); err != nil {
			return http.StatusUnauthorized, err
		}
		req.SignatureValid = true
	}
	if rate, err := strconv.ParseFloat(r.URL.Query().Get("fail_rate"), 64); err == nil && rate > 0 {
		if rand.Float64() < rate { //nolint:gosec // test tool, not security
			return http.StatusInternalServerError, nil
		}
	}
	if s, ok := intParam(r, "status"); ok && s >= 100 && s <= 599 {
		return s, nil
	}
	return http.StatusOK, nil
}

func intParam(r *http.Request, name string) (int, bool) {
	v, err := strconv.Atoi(r.URL.Query().Get(name))
	return v, err == nil
}

// Requests returns a copy of every request received so far.
func (rc *Receiver) Requests() []Request {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]Request(nil), rc.requests...)
}

// Count returns how many times webhookID was received.
func (rc *Receiver) Count(webhookID string) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.seen[webhookID]
}

// Duplicates returns how many received requests repeated an earlier webhook-id.
func (rc *Receiver) Duplicates() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	n := 0
	for _, c := range rc.seen {
		n += c - 1
	}
	return n
}
