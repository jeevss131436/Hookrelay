package delivery

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/mockreceiver"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/signing"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

// devSender may reach httptest servers on 127.0.0.1.
func devSender(timeout time.Duration) *Sender {
	return NewSender(SenderOptions{Timeout: timeout, AllowPrivate: true})
}

// countingServer counts requests and answers with h.
func countingServer(t *testing.T, h http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

// fakeResolver answers from a script: call i gets answers[min(i, len-1)].
// It simulates DNS records changing between registration and delivery.
type fakeResolver struct {
	mu      sync.Mutex
	calls   int
	answers [][]netip.Addr
	err     error
}

func (f *fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	i := min(f.calls, len(f.answers)-1)
	f.calls++
	return f.answers[i], nil
}

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParseAddr(s)
	}
	return out
}

func port(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Port()
}

func TestSendSignedExactBytes(t *testing.T) {
	secret := secrets.GenerateSigningSecret()
	rc := mockreceiver.New(secret, nil)
	srv := httptest.NewServer(rc)
	t.Cleanup(srv.Close)

	payload := []byte("{ \"z\":1,\n\"a\" : [ true ] }") // not canonical JSON on purpose
	hdr, err := signing.Sign(secret, "dlv_abc", time.Now(), payload)
	if err != nil {
		t.Fatal(err)
	}
	res := devSender(5*time.Second).Send(context.Background(), Request{URL: srv.URL + "/hook", Headers: hdr, Body: payload})
	if !res.Success() || res.StatusCode != 200 {
		t.Fatalf("result = %+v", res)
	}
	if res.Duration <= 0 || res.StartedAt.IsZero() {
		t.Errorf("latency not recorded: %+v", res)
	}
	reqs := rc.Requests()
	if len(reqs) != 1 || !reqs[0].SignatureValid {
		t.Fatalf("receiver saw %+v", reqs)
	}
	got := reqs[0]
	if string(got.Body) != string(payload) {
		t.Errorf("body = %q, want the exact stored bytes %q", got.Body, payload)
	}
	if got.WebhookID != "dlv_abc" || got.Header.Get("Content-Type") != "application/json" ||
		got.Header.Get("User-Agent") != UserAgent {
		t.Errorf("headers = %v", got.Header)
	}
}

func TestSendHTTP500(t *testing.T) {
	srv, _ := countingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	res := devSender(5*time.Second).Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
	if res.Success() || res.StatusCode != 500 || res.ErrorKind != ErrKindHTTPStatus || res.Permanent {
		t.Fatalf("result = %+v", res)
	}
	if res.Body != "boom\n" || res.ErrorMsg != "HTTP 500" {
		t.Errorf("body %q msg %q", res.Body, res.ErrorMsg)
	}
}

func TestSendRetryAfterOn429(t *testing.T) {
	srv, _ := countingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	res := devSender(5*time.Second).Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
	if res.StatusCode != 429 || res.RetryAfter != 2*time.Minute {
		t.Fatalf("result = %+v", res)
	}
}

func TestSendTimeout(t *testing.T) {
	srv, _ := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(time.Second):
		case <-r.Context().Done():
		}
	})
	start := time.Now()
	res := devSender(300*time.Millisecond).Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
	if res.ErrorKind != ErrKindTimeout || res.StatusCode != 0 || res.Permanent {
		t.Fatalf("result = %+v", res)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("timeout not enforced: took %s", elapsed)
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	target, targetHits := countingServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	srv, _ := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
		code, _ := strconv.Atoi(r.URL.Query().Get("code"))
		http.Redirect(w, r, target.URL, code)
	})
	for _, code := range []int{301, 302, 307, 308} {
		res := devSender(5*time.Second).Send(context.Background(),
			Request{URL: srv.URL + "/?code=" + strconv.Itoa(code), Body: []byte(`{}`)})
		if res.Success() || res.StatusCode != code || res.ErrorKind != ErrKindHTTPStatus ||
			!strings.Contains(res.ErrorMsg, "redirects are not followed") {
			t.Fatalf("%d: result = %+v", code, res)
		}
	}
	if n := targetHits.Load(); n != 0 {
		t.Fatalf("redirect target was hit %d times", n)
	}
}

func TestSendTruncatesAndSanitizesResponseBody(t *testing.T) {
	big := strings.Repeat("é", 5000) // 10,000 bytes of 2-byte runes
	srv, _ := countingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "\x00"+big)
	})
	res := devSender(5*time.Second).Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
	if len(res.Body) > MaxResponseBody || len(res.Body) < MaxResponseBody-4 {
		t.Fatalf("body length = %d, want ~%d", len(res.Body), MaxResponseBody)
	}
	if !utf8.ValidString(res.Body) || strings.ContainsRune(res.Body, 0) {
		t.Fatal("body is not storable text")
	}
}

func TestSanitizeText(t *testing.T) {
	if got := sanitizeText([]byte("ok\xffbad\x00"), 100); got != "ok�bad" {
		t.Errorf("got %q", got)
	}
	if got := sanitizeText([]byte("aé"), 2); got != "a" {
		t.Errorf("rune split: got %q", got)
	}
}

func TestSendBlocksPrivateAddresses(t *testing.T) {
	srv, hits := countingServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	s := NewSender(SenderOptions{Timeout: 2 * time.Second}) // production settings
	for _, u := range []string{
		srv.URL,                              // 127.0.0.1 literal
		"http://[::1]:" + port(t, srv) + "/", // IPv6 loopback
		"http://169.254.169.254/latest/meta-data/", // cloud metadata
		"http://10.0.0.1/",
	} {
		res := s.Send(context.Background(), Request{URL: u, Body: []byte(`{}`)})
		if res.ErrorKind != ErrKindSSRFBlocked || !res.Permanent {
			t.Errorf("%s: result = %+v", u, res)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("blocked server was hit %d times", hits.Load())
	}
}

func TestSendDefeatsDNSRebinding(t *testing.T) {
	srv, hits := countingServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	host := "hooks.example.com"
	u := "http://" + host + ":" + port(t, srv) + "/hook"

	// Registration-time lookup sees a public address; by delivery time the
	// attacker's DNS answers with loopback.
	r := &fakeResolver{answers: [][]netip.Addr{addrs("93.184.215.14"), addrs("127.0.0.1")}}
	if _, err := (ssrf.Policy{Resolver: r}).ValidateURL(context.Background(), u); err != nil {
		t.Fatalf("registration should pass: %v", err)
	}
	res := NewSender(SenderOptions{Timeout: 2 * time.Second, Resolver: r}).
		Send(context.Background(), Request{URL: u, Body: []byte(`{}`)})
	if res.ErrorKind != ErrKindSSRFBlocked || !res.Permanent || !strings.Contains(res.ErrorMsg, "127.0.0.1") {
		t.Fatalf("result = %+v", res)
	}

	// A mixed answer is refused as a whole.
	mixed := &fakeResolver{answers: [][]netip.Addr{addrs("93.184.215.14", "::ffff:127.0.0.1")}}
	res = NewSender(SenderOptions{Timeout: 2 * time.Second, Resolver: mixed}).
		Send(context.Background(), Request{URL: u, Body: []byte(`{}`)})
	if res.ErrorKind != ErrKindSSRFBlocked {
		t.Fatalf("mixed answer: result = %+v", res)
	}
	if hits.Load() != 0 {
		t.Fatalf("internal server was hit %d times", hits.Load())
	}
}

// The Control hook is the last line of defence: it sees the literal IP the
// socket connects to, whatever resolution produced it.
func TestConnectControlChecksConnectedIP(t *testing.T) {
	ctl := connectControl(false)
	for _, a := range []string{"127.0.0.1:80", "10.1.2.3:443", "169.254.169.254:80", "[::1]:80",
		"[::ffff:127.0.0.1]:80", "[fd00:ec2::254]:80", "0.0.0.0:80", "100.64.0.1:80"} {
		var be *BlockedError
		if err := ctl("tcp4", a, nil); !errors.As(err, &be) {
			t.Errorf("%s: err = %v, want BlockedError", a, err)
		}
	}
	for _, a := range []string{"93.184.215.14:443", "[2606:4700::1111]:443"} {
		if err := ctl("tcp6", a, nil); err != nil {
			t.Errorf("%s: unexpected error %v", a, err)
		}
	}
	if err := ctl("udp4", "93.184.215.14:53", nil); err == nil {
		t.Error("udp must be refused")
	}
	if err := connectControl(true)("tcp4", "127.0.0.1:80", nil); err != nil {
		t.Errorf("allowPrivate: %v", err)
	}
}

func TestSendClassifiesTransportErrors(t *testing.T) {
	t.Run("dns", func(t *testing.T) {
		r := &fakeResolver{err: &net.DNSError{Err: "no such host", Name: "nope.example.com", IsNotFound: true}}
		res := NewSender(SenderOptions{Timeout: 2 * time.Second, Resolver: r}).
			Send(context.Background(), Request{URL: "https://nope.example.com/hook", Body: []byte(`{}`)})
		if res.ErrorKind != ErrKindDNS || res.Permanent {
			t.Fatalf("result = %+v", res)
		}
	})
	t.Run("connection refused", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		res := devSender(2*time.Second).Send(context.Background(), Request{URL: "http://" + addr + "/", Body: []byte(`{}`)})
		if res.ErrorKind != ErrKindConnection || res.Permanent {
			t.Fatalf("result = %+v", res)
		}
	})
	t.Run("tls", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
		t.Cleanup(srv.Close)
		res := devSender(2*time.Second).Send(context.Background(), Request{URL: srv.URL, Body: []byte(`{}`)})
		if res.ErrorKind != ErrKindTLS {
			t.Fatalf("self-signed cert: result = %+v", res)
		}
	})
	t.Run("invalid url", func(t *testing.T) {
		res := devSender(time.Second).Send(context.Background(), Request{URL: "ftp://example.com/x", Body: []byte(`{}`)})
		if res.Success() || !res.Permanent {
			t.Fatalf("result = %+v", res)
		}
	})
}

func TestTaskPayloadRoundTrip(t *testing.T) {
	p := Payload{ProjectID: store.NewUUID(), DeliveryID: store.NewUUID(), AttemptCount: 3}
	task, err := NewTask(p, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if task.Type() != TypeDeliver {
		t.Errorf("type = %q", task.Type())
	}
	got, err := ParsePayload(task.Payload())
	if err != nil || got != p {
		t.Fatalf("ParsePayload = %+v, %v; want %+v", got, err, p)
	}
	if want := p.DeliveryID.String() + ":3"; p.TaskID() != want {
		t.Errorf("TaskID = %q, want %q", p.TaskID(), want)
	}
	for _, bad := range []string{`not json`, `{}`, `{"project_id":"` + p.ProjectID.String() + `"}`} {
		if _, err := ParsePayload([]byte(bad)); err == nil {
			t.Errorf("ParsePayload(%s) succeeded", bad)
		}
	}
	if _, err := NewTask(Payload{ProjectID: p.ProjectID, DeliveryID: pgtype.UUID{}}, time.Second); err == nil {
		t.Error("NewTask accepted a payload without delivery_id")
	}
}

func TestProcessTaskRejectsMalformedPayloadWithoutRetry(t *testing.T) {
	h := &Handler{log: discardLogger()}
	err := h.ProcessTask(context.Background(), asynq.NewTask(TypeDeliver, []byte(`garbage`)))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("err = %v, want SkipRetry", err)
	}
}
