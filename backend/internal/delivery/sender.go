package delivery

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jeevss131436/Hookrelay/backend/internal/backoff"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
)

// Error kinds stored in delivery_attempts.error_kind (must match the CHECK
// constraint in the schema).
const (
	ErrKindTimeout     = "timeout"
	ErrKindConnection  = "connection"
	ErrKindDNS         = "dns"
	ErrKindTLS         = "tls"
	ErrKindHTTPStatus  = "http_status"
	ErrKindSSRFBlocked = "ssrf_blocked"
)

// Limits.
const (
	// MaxResponseBody is how much of a receiver's response body is kept
	// (delivery_attempts.response_body is at most 4096 bytes).
	MaxResponseBody = 4096
	// maxErrorMessage bounds delivery_attempts.error_message / last_error.
	maxErrorMessage = 1024
	// drainLimit is how much more of a response body is read (and discarded)
	// so the connection can be reused. Larger bodies just close the connection.
	drainLimit = 64 << 10
)

// UserAgent is sent with every delivery.
const UserAgent = "HookRelay-Webhooks/1.0"

// BlockedError reports a destination address the SSRF policy forbids. It is
// returned both by the pre-dial DNS check and by the dialer's Control hook,
// which sees the IP actually being connected to.
type BlockedError struct {
	Host string
	IP   netip.Addr
}

func (e *BlockedError) Error() string {
	if e.Host != "" && e.Host != e.IP.String() {
		return fmt.Sprintf("destination %s resolves to disallowed address %s", e.Host, e.IP)
	}
	return fmt.Sprintf("destination address %s is not allowed", e.IP)
}

// SenderOptions configures a Sender.
type SenderOptions struct {
	// Timeout bounds the whole request, from dialing to reading the
	// (truncated) response body. PRODUCT_SPEC: 10 seconds.
	Timeout time.Duration
	// AllowPrivate disables the address checks. Local development and tests
	// only; config refuses it in production.
	AllowPrivate bool
	// Resolver resolves hostnames; nil uses net.DefaultResolver. Tests inject
	// a fake one to simulate DNS rebinding.
	Resolver ssrf.Resolver
}

// Sender POSTs signed webhooks through one shared, SSRF-guarded HTTP client
// (ARCHITECTURE §6.1, §7). It is safe for concurrent use. Never use
// http.DefaultClient for deliveries (CLAUDE.md invariant 9).
type Sender struct {
	client *http.Client
}

// NewSender builds the shared client.
//
// SSRF defence in depth:
//  1. The dialer resolves the host itself and refuses the whole answer if any
//     address is disallowed (the same rule as registration-time validation).
//  2. net.Dialer.Control re-checks the exact IP:port the socket is about to
//     connect to. Because this runs at connect time, on the address actually
//     used, a DNS record that changed since registration (DNS rebinding)
//     cannot route a delivery to an internal address.
//  3. No proxy (an HTTP proxy would make the proxy, not the target, the
//     connected address) and no redirects (a redirect could point anywhere).
func NewSender(o SenderOptions) *Sender {
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Second
	}
	resolver := o.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	dialer := &net.Dialer{
		Timeout:   o.Timeout,
		KeepAlive: 30 * time.Second,
		Control:   connectControl(o.AllowPrivate),
	}
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := resolve(ctx, resolver, host)
		if err != nil {
			return nil, err
		}
		if !o.AllowPrivate {
			for _, ip := range ips {
				if ssrf.IsDisallowed(ip) {
					return nil, &BlockedError{Host: host, IP: ip.Unmap()}
				}
			}
		}
		var firstErr error
		for _, ip := range ips {
			conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
			if dialErr == nil {
				return conn, nil
			}
			if firstErr == nil {
				firstErr = dialErr
			}
			if ctx.Err() != nil {
				break
			}
		}
		return nil, firstErr
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dial,
		ForceAttemptHTTP2:     true,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   o.Timeout,
		ResponseHeaderTimeout: o.Timeout,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &Sender{client: &http.Client{
		Transport: transport,
		Timeout:   o.Timeout,
		// Never follow redirects: return the 3xx itself, which counts as a
		// failed attempt (PRODUCT_SPEC §4.3).
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// CloseIdleConnections releases pooled connections (on worker shutdown).
func (s *Sender) CloseIdleConnections() { s.client.CloseIdleConnections() }

func resolve(ctx context.Context, r ssrf.Resolver, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	ips, err := r.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
	}
	return ips, nil
}

// connectControl runs just before connect(2) with the literal IP:port.
func connectControl(allowPrivate bool) func(network, address string, _ syscall.RawConn) error {
	return func(network, address string, _ syscall.RawConn) error {
		if network != "tcp4" && network != "tcp6" {
			return fmt.Errorf("network %q is not allowed", network)
		}
		if allowPrivate {
			return nil
		}
		ap, err := netip.ParseAddrPort(address)
		if err != nil {
			return &BlockedError{}
		}
		if ssrf.IsDisallowed(ap.Addr()) {
			return &BlockedError{IP: ap.Addr().Unmap()}
		}
		return nil
	}
}

// Request is one outbound delivery.
type Request struct {
	URL     string
	Headers http.Header // signature headers
	Body    []byte      // the exact stored payload bytes
}

// Result is the outcome of one HTTP attempt. Exactly one of these holds:
// Success; StatusCode != 0 with ErrorKind "http_status"; or StatusCode == 0
// with a transport-level ErrorKind.
type Result struct {
	StartedAt  time.Time
	Duration   time.Duration
	StatusCode int    // 0 when no response was received
	Body       string // first MaxResponseBody bytes, valid UTF-8
	ErrorKind  string // "" on success
	ErrorMsg   string
	RetryAfter time.Duration // from a 429/503 Retry-After header
	// Permanent failures are not retried: retrying cannot succeed without a
	// configuration change (e.g. a destination the SSRF policy forbids).
	Permanent bool
}

// Success reports whether the receiver answered 2xx.
func (r Result) Success() bool { return r.ErrorKind == "" }

// Send POSTs req and classifies the outcome. It never returns an error:
// every failure is described in the Result, ready to be recorded.
func (s *Sender) Send(ctx context.Context, req Request) Result {
	start := time.Now()
	res := Result{StartedAt: start}

	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		res.ErrorKind, res.ErrorMsg, res.Permanent = ErrKindConnection, "invalid endpoint url", true
		return res
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(req.Body))
	if err != nil {
		res.ErrorKind, res.ErrorMsg, res.Permanent = ErrKindConnection, "invalid endpoint url", true
		return res
	}
	for k, vs := range req.Headers {
		for _, v := range vs {
			httpReq.Header.Add(k, v)
		}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", UserAgent)

	resp, err := s.client.Do(httpReq)
	if err != nil {
		res.Duration = time.Since(start)
		res.ErrorKind, res.ErrorMsg = classify(err)
		res.Permanent = res.ErrorKind == ErrKindSSRFBlocked
		return res
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBody))
	res.Duration = time.Since(start) // latency includes reading the kept body
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))

	res.StatusCode = resp.StatusCode
	res.Body = sanitizeText(body, MaxResponseBody)
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return res
	}
	res.ErrorKind = ErrKindHTTPStatus
	switch {
	case resp.StatusCode >= 300 && resp.StatusCode <= 399:
		res.ErrorMsg = fmt.Sprintf("HTTP %d (redirects are not followed)", resp.StatusCode)
	default:
		res.ErrorMsg = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	res.RetryAfter = backoff.RetryAfter(resp.StatusCode, resp.Header, time.Now())
	return res
}

// classify maps a transport error to an error kind and a message. The URL is
// stripped from the message (url.Error repeats it).
func classify(err error) (kind, msg string) {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	msg = sanitizeText([]byte(err.Error()), maxErrorMessage)

	var blocked *BlockedError
	var dnsErr *net.DNSError
	var certErr *tls.CertificateVerificationError
	var recordErr tls.RecordHeaderError
	var alertErr tls.AlertError
	var unknownAuth x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var netErr net.Error
	switch {
	case errors.As(err, &blocked):
		return ErrKindSSRFBlocked, msg
	case errors.As(err, &dnsErr):
		return ErrKindDNS, msg
	case errors.As(err, &certErr), errors.As(err, &recordErr), errors.As(err, &alertErr),
		errors.As(err, &unknownAuth), errors.As(err, &hostErr), errors.As(err, &invalidCert):
		return ErrKindTLS, msg
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return ErrKindTimeout, msg
	default:
		return ErrKindConnection, msg
	}
}

// sanitizeText makes b storable in a Postgres text column: valid UTF-8, no
// NUL bytes, and at most limit bytes (cut on a rune boundary).
func sanitizeText(b []byte, limit int) string {
	s := strings.ToValidUTF8(string(b), "�")
	s = strings.ReplaceAll(s, "\x00", "")
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
