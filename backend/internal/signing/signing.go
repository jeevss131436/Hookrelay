// Package signing implements Standard Webhooks signatures
// (https://www.standardwebhooks.com), as described in PRODUCT_SPEC §4.3.
//
// Every delivery carries three headers:
//
//	webhook-id:        the delivery ID (stable across retries; receivers dedupe on it)
//	webhook-timestamp: Unix seconds at the moment of sending
//	webhook-signature: "v1," + base64(HMAC-SHA256(key, "{id}.{timestamp}.{body}"))
//
// The key is the base64-decoded part of the endpoint's "whsec_..." secret. The
// body is signed exactly as stored and sent (ARCHITECTURE D5): callers must
// pass the stored payload bytes, never a re-marshalled copy.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names (lowercase, as in the Standard Webhooks spec; net/http
// canonicalizes them on the wire, and HTTP header names are case-insensitive).
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// SecretPrefix is the prefix of HookRelay signing secrets.
const SecretPrefix = "whsec_"

// DefaultTolerance is how far a timestamp may be from the receiver's clock
// before Verify rejects it (replay protection, ARCHITECTURE §7).
const DefaultTolerance = 5 * time.Minute

// Errors returned by DecodeSecret and Verify.
var (
	ErrInvalidSecret    = errors.New("signing: invalid secret")
	ErrMissingHeaders   = errors.New("signing: missing webhook-id, webhook-timestamp or webhook-signature header")
	ErrInvalidTimestamp = errors.New("signing: invalid webhook-timestamp")
	ErrTimestampTooOld  = errors.New("signing: timestamp too old")
	ErrTimestampTooNew  = errors.New("signing: timestamp too far in the future")
	ErrNoValidSignature = errors.New("signing: no matching signature")
	errEmptyKey         = errors.New("signing: empty key")
)

// DecodeSecret returns the HMAC key for a "whsec_<base64>" secret. The prefix
// is optional, as in the reference libraries. Errors never include the secret.
func DecodeSecret(secret string) ([]byte, error) {
	raw := strings.TrimPrefix(secret, SecretPrefix)
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) == 0 {
		return nil, ErrInvalidSecret
	}
	return key, nil
}

// Signature computes the "v1,<base64>" signature of payload for msgID at ts.
func Signature(key []byte, msgID string, ts int64, payload []byte) (string, error) {
	if len(key) == 0 {
		return "", errEmptyKey
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(msgID))
	mac.Write([]byte{'.'})
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte{'.'})
	mac.Write(payload)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Sign returns the three Standard Webhooks headers for payload. ts is the
// sending time; it is truncated to whole seconds.
func Sign(secret, msgID string, ts time.Time, payload []byte) (http.Header, error) {
	key, err := DecodeSecret(secret)
	if err != nil {
		return nil, err
	}
	unix := ts.Unix()
	sig, err := Signature(key, msgID, unix, payload)
	if err != nil {
		return nil, err
	}
	h := make(http.Header, 3)
	h.Set(HeaderID, msgID)
	h.Set(HeaderTimestamp, strconv.FormatInt(unix, 10))
	h.Set(HeaderSignature, sig)
	return h, nil
}

// Verify checks the Standard Webhooks headers on a received request. It is
// what a receiver runs (the mock receiver and tests use it). The signature
// header may hold several space-separated signatures; one valid "v1" entry is
// enough. A tolerance of zero means DefaultTolerance.
func Verify(secret string, h http.Header, payload []byte, now time.Time, tolerance time.Duration) error {
	key, err := DecodeSecret(secret)
	if err != nil {
		return err
	}
	msgID, tsStr, sigHeader := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if msgID == "" || tsStr == "" || sigHeader == "" {
		return ErrMissingHeaders
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return ErrInvalidTimestamp
	}
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	sent := time.Unix(ts, 0)
	if now.Sub(sent) > tolerance {
		return ErrTimestampTooOld
	}
	if sent.Sub(now) > tolerance {
		return ErrTimestampTooNew
	}

	want, err := Signature(key, msgID, ts, payload)
	if err != nil {
		return err
	}
	for _, candidate := range strings.Fields(sigHeader) {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(want)) == 1 {
			return nil
		}
	}
	return ErrNoValidSignature
}
