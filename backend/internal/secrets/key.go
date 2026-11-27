// Package secrets handles HookRelay's secret material:
//
//   - Key / Cipher: AES-256-GCM encryption of endpoint signing secrets at rest.
//   - API keys: generation and SHA-256 hashing (only the hash is stored).
//   - Signing secrets: generation of per-endpoint "whsec_" secrets.
//
// Types that hold secret material redact themselves in fmt and slog output,
// so accidentally logging one does not leak it.
package secrets

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// KeySize is the AES-256 key length in bytes.
const KeySize = 32

const redacted = "[REDACTED]"

// Key is a 256-bit encryption key.
type Key [KeySize]byte

// ParseKey decodes a standard base64 (padded or unpadded) 32-byte key, as
// produced by `openssl rand -base64 32`. Error messages never include the input.
func ParseKey(s string) (Key, error) {
	var k Key
	s = strings.TrimSpace(s)
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		b, err = base64.RawStdEncoding.DecodeString(s)
	}
	if err != nil {
		return k, errors.New("encryption key is not valid base64")
	}
	if len(b) != KeySize {
		return k, fmt.Errorf("encryption key must decode to %d bytes, got %d", KeySize, len(b))
	}
	copy(k[:], b)
	if subtle.ConstantTimeCompare(k[:], make([]byte, KeySize)) == 1 {
		return Key{}, errors.New("encryption key must not be all zeros")
	}
	return k, nil
}

// GenerateKey returns a random key.
func GenerateKey() Key {
	var k Key
	_, _ = rand.Read(k[:]) // crypto/rand.Read never returns an error.
	return k
}

// Base64 encodes the key in the format ParseKey accepts. Use it only to hand
// a newly generated key to an operator.
func (k Key) Base64() string { return base64.StdEncoding.EncodeToString(k[:]) }

// String redacts the key.
func (Key) String() string { return redacted }

// GoString redacts the key for %#v.
func (Key) GoString() string { return redacted }

// LogValue redacts the key for slog.
func (Key) LogValue() slog.Value { return slog.StringValue(redacted) }
