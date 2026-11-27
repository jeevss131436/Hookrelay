package secrets

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
)

// API key format: "hr_" + 43 base62 characters (≈256 bits of entropy).
// Base62 keeps the key double-click selectable and URL/header safe.
const (
	APIKeyPrefix    = "hr_"
	apiKeyBodyLen   = 43
	APIKeyLen       = len(APIKeyPrefix) + apiKeyBodyLen
	displayPrefixLn = len(APIKeyPrefix) + 8 // "hr_" + 8 chars stored for display
	base62Alphabet  = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// ErrMalformedAPIKey means a presented key is not in HookRelay's format. Callers
// should treat it exactly like an unknown key (401) to avoid an oracle.
var ErrMalformedAPIKey = errors.New("secrets: malformed API key")

// APIKey is a newly generated API key. Plaintext must be shown to the user
// once and then discarded. Only Hash (and the non-secret Prefix) are stored.
type APIKey struct {
	Plaintext string
	Prefix    string
	Hash      []byte
}

// GenerateAPIKey creates a random API key.
func GenerateAPIKey() APIKey {
	body := randomBase62(apiKeyBodyLen)
	plaintext := APIKeyPrefix + body
	sum := sha256.Sum256([]byte(plaintext))
	return APIKey{
		Plaintext: plaintext,
		Prefix:    plaintext[:displayPrefixLn],
		Hash:      sum[:],
	}
}

// HashAPIKey validates the format of a presented key and returns its SHA-256
// hash for lookup. A fast hash is appropriate (not bcrypt/argon2) because the
// key carries ~256 bits of entropy and cannot be brute-forced.
func HashAPIKey(presented string) ([]byte, error) {
	if len(presented) != APIKeyLen || !strings.HasPrefix(presented, APIKeyPrefix) {
		return nil, ErrMalformedAPIKey
	}
	for i := len(APIKeyPrefix); i < len(presented); i++ {
		if strings.IndexByte(base62Alphabet, presented[i]) < 0 {
			return nil, ErrMalformedAPIKey
		}
	}
	sum := sha256.Sum256([]byte(presented))
	return sum[:], nil
}

// String shows only the non-secret prefix.
func (k APIKey) String() string { return k.Prefix + "…" }

// GoString shows only the non-secret prefix.
func (k APIKey) GoString() string { return "secrets.APIKey{Prefix:" + k.Prefix + "}" }

// LogValue logs only the non-secret prefix.
func (k APIKey) LogValue() slog.Value { return slog.StringValue(k.String()) }

// SigningSecretPrefix marks endpoint signing secrets (Standard Webhooks style).
const SigningSecretPrefix = "whsec_"

// GenerateSigningSecret returns a new endpoint signing secret:
// "whsec_" + base64(32 random bytes). It is stored encrypted with Cipher.
func GenerateSigningSecret() string {
	var b [32]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.
	return SigningSecretPrefix + base64.StdEncoding.EncodeToString(b[:])
}

// randomBase62 returns n uniformly random base62 characters, using rejection
// sampling to avoid modulo bias.
func randomBase62(n int) string {
	out := make([]byte, 0, n)
	buf := make([]byte, n+n/4)
	for len(out) < n {
		_, _ = rand.Read(buf) // crypto/rand.Read never returns an error.
		for _, b := range buf {
			if b < 248 { // 248 = 4*62: largest multiple of 62 below 256
				out = append(out, base62Alphabet[b%62])
				if len(out) == n {
					break
				}
			}
		}
	}
	return string(out)
}
