package signing

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
)

// Test vector published by the Standard Webhooks project (and used by the
// reference libraries' test suites).
const (
	vectorSecret    = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vectorMsgID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vectorTimestamp = int64(1614265330)
	vectorPayload   = `{"test": 2432232314}`
	vectorSignature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func TestSignatureMatchesPublishedVector(t *testing.T) {
	key, err := DecodeSecret(vectorSecret)
	if err != nil {
		t.Fatalf("DecodeSecret: %v", err)
	}
	got, err := Signature(key, vectorMsgID, vectorTimestamp, []byte(vectorPayload))
	if err != nil {
		t.Fatalf("Signature: %v", err)
	}
	if got != vectorSignature {
		t.Fatalf("signature = %q, want %q", got, vectorSignature)
	}

	h, err := Sign(vectorSecret, vectorMsgID, time.Unix(vectorTimestamp, 0), []byte(vectorPayload))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if h.Get(HeaderID) != vectorMsgID || h.Get(HeaderTimestamp) != "1614265330" || h.Get(HeaderSignature) != vectorSignature {
		t.Fatalf("headers = %v", h)
	}
	if err := Verify(vectorSecret, h, []byte(vectorPayload), time.Unix(vectorTimestamp+10, 0), 0); err != nil {
		t.Fatalf("Verify(vector): %v", err)
	}
}

func TestSignVerifyRoundTripWithGeneratedSecret(t *testing.T) {
	secret := secrets.GenerateSigningSecret()
	payload := []byte("{\n  \"b\": 1,   \"a\": [2, 3]\n}") // odd whitespace and key order must survive
	now := time.Now()
	h, err := Sign(secret, "dlv_123", now, payload)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := Verify(secret, h, payload, now, 0); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyRejects(t *testing.T) {
	secret := secrets.GenerateSigningSecret()
	other := secrets.GenerateSigningSecret()
	payload := []byte(`{"id":1}`)
	now := time.Unix(1_700_000_000, 0)
	signed, err := Sign(secret, "dlv_1", now, payload)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	with := func(name, value string) http.Header {
		h := signed.Clone()
		if value == "" {
			h.Del(name)
		} else {
			h.Set(name, value)
		}
		return h
	}

	tests := []struct {
		name    string
		secret  string
		h       http.Header
		payload []byte
		now     time.Time
		want    error
	}{
		{"tampered payload", secret, signed, []byte(`{"id":2}`), now, ErrNoValidSignature},
		{"re-marshalled payload", secret, signed, []byte(`{"id": 1}`), now, ErrNoValidSignature},
		{"wrong secret", other, signed, payload, now, ErrNoValidSignature},
		{"different webhook-id", secret, with(HeaderID, "dlv_2"), payload, now, ErrNoValidSignature},
		{"shifted timestamp", secret, with(HeaderTimestamp, strconv.FormatInt(now.Unix()+1, 10)), payload, now, ErrNoValidSignature},
		{"too old", secret, signed, payload, now.Add(DefaultTolerance + time.Second), ErrTimestampTooOld},
		{"too new", secret, signed, payload, now.Add(-DefaultTolerance - time.Second), ErrTimestampTooNew},
		{"bad timestamp", secret, with(HeaderTimestamp, "yesterday"), payload, now, ErrInvalidTimestamp},
		{"missing id", secret, with(HeaderID, ""), payload, now, ErrMissingHeaders},
		{"missing signature", secret, with(HeaderSignature, ""), payload, now, ErrMissingHeaders},
		{"unknown version only", secret, with(HeaderSignature, "v2,"+signed.Get(HeaderSignature)[3:]), payload, now, ErrNoValidSignature},
		{"invalid secret", "whsec_!!!", signed, payload, now, ErrInvalidSecret},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Verify(tt.secret, tt.h, tt.payload, tt.now, 0); !errors.Is(err, tt.want) {
				t.Fatalf("Verify = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestVerifyAcceptsAnyOfSeveralSignatures(t *testing.T) {
	secret := secrets.GenerateSigningSecret()
	payload := []byte(`{}`)
	now := time.Now()
	h, err := Sign(secret, "dlv_1", now, payload)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	// e.g. during a future secret rotation: an old-secret signature first.
	h.Set(HeaderSignature, "v1,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= v1a,xyz "+h.Get(HeaderSignature))
	if err := Verify(secret, h, payload, now, 0); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestDecodeSecret(t *testing.T) {
	for _, bad := range []string{"", "whsec_", "whsec_not base64!", "whsec_%%%"} {
		if _, err := DecodeSecret(bad); !errors.Is(err, ErrInvalidSecret) {
			t.Errorf("DecodeSecret(%q) err = %v, want ErrInvalidSecret", bad, err)
		}
	}
	gen := secrets.GenerateSigningSecret()
	key, err := DecodeSecret(gen)
	if err != nil || len(key) != 32 {
		t.Fatalf("DecodeSecret(generated) = %d bytes, %v; want 32 bytes", len(key), err)
	}
}
