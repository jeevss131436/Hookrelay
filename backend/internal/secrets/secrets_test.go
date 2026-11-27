package secrets

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func newTestCipher(t *testing.T) *Cipher {
	t.Helper()
	c, err := NewCipher(GenerateKey())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCipherRoundTrip(t *testing.T) {
	c := newTestCipher(t)
	for _, pt := range [][]byte{nil, []byte("x"), []byte(GenerateSigningSecret()), bytes.Repeat([]byte("a"), 4096)} {
		ct := c.Encrypt(pt, []byte("endpoint-1"))
		if len(ct) != len(pt)+overhead {
			t.Errorf("ciphertext len = %d, want %d", len(ct), len(pt)+overhead)
		}
		got, err := c.Decrypt(ct, []byte("endpoint-1"))
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		if !bytes.Equal(got, pt) {
			t.Errorf("round trip mismatch: %q != %q", got, pt)
		}
	}
}

func TestCipherNonceIsRandom(t *testing.T) {
	c := newTestCipher(t)
	a, b := c.Encrypt([]byte("same"), nil), c.Encrypt([]byte("same"), nil)
	if bytes.Equal(a, b) {
		t.Fatal("two encryptions of the same plaintext are identical (nonce reuse)")
	}
}

func TestCipherDoesNotContainPlaintext(t *testing.T) {
	c := newTestCipher(t)
	pt := []byte("whsec_super-secret-value")
	if bytes.Contains(c.Encrypt(pt, nil), pt) {
		t.Fatal("ciphertext contains plaintext")
	}
}

func TestCipherDecryptFailures(t *testing.T) {
	c := newTestCipher(t)
	other := newTestCipher(t)
	ct := c.Encrypt([]byte("secret"), []byte("ad"))

	flip := func(i int) []byte {
		b := bytes.Clone(ct)
		b[i] ^= 0x01
		return b
	}
	tests := []struct {
		name string
		c    *Cipher
		ct   []byte
		ad   []byte
	}{
		{"wrong key", other, ct, []byte("ad")},
		{"wrong associated data", c, ct, []byte("other")},
		{"missing associated data", c, ct, nil},
		{"tampered ciphertext", c, flip(len(ct) - 1), []byte("ad")},
		{"tampered nonce", c, flip(1), []byte("ad")},
		{"unknown version", c, flip(0), []byte("ad")},
		{"truncated", c, ct[:overhead-1], []byte("ad")},
		{"empty", c, nil, []byte("ad")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt, err := tt.c.Decrypt(tt.ct, tt.ad)
			if !errors.Is(err, ErrDecrypt) {
				t.Fatalf("err = %v, want ErrDecrypt", err)
			}
			if pt != nil {
				t.Errorf("plaintext returned on failure: %q", pt)
			}
		})
	}
}

func TestParseKey(t *testing.T) {
	k := GenerateKey()
	raw := k[:]

	got, err := ParseKey(base64.StdEncoding.EncodeToString(raw))
	if err != nil || got != k {
		t.Fatalf("padded: got err %v", err)
	}
	if got, err = ParseKey(base64.RawStdEncoding.EncodeToString(raw) + "\n"); err != nil || got != k {
		t.Fatalf("unpadded with whitespace: got err %v", err)
	}
	if got, err = ParseKey(k.Base64()); err != nil || got != k {
		t.Fatalf("Base64() round trip: got err %v", err)
	}

	secretInput := "not!base64!" + strings.Repeat("Z", 20)
	invalid := map[string]string{
		"empty":      "",
		"not base64": secretInput,
		"too short":  base64.StdEncoding.EncodeToString(raw[:16]),
		"too long":   base64.StdEncoding.EncodeToString(append(bytes.Clone(raw), 0)),
		"all zeros":  base64.StdEncoding.EncodeToString(make([]byte, KeySize)),
	}
	for name, in := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := ParseKey(in)
			if err == nil {
				t.Fatal("expected error")
			}
			if in != "" && strings.Contains(err.Error(), in) {
				t.Errorf("error leaks input: %v", err)
			}
		})
	}
}

func TestKeyIsRedacted(t *testing.T) {
	k := GenerateKey()
	b64 := k.Base64()
	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("cfg", "key", k)

	for name, s := range map[string]string{
		"%v":       fmt.Sprintf("%v", k),
		"String()": k.String(),
		"%#v":      fmt.Sprintf("%#v", k),
		"%+v":      fmt.Sprintf("%+v", struct{ K Key }{k}),
		"log":      logBuf.String(),
	} {
		if strings.Contains(s, b64) || !strings.Contains(s, redacted) {
			t.Errorf("%s output not redacted: %q", name, s)
		}
	}
}

func TestGenerateAPIKey(t *testing.T) {
	k := GenerateAPIKey()
	if len(k.Plaintext) != APIKeyLen || !strings.HasPrefix(k.Plaintext, APIKeyPrefix) {
		t.Fatalf("bad format: %q", k.Plaintext)
	}
	if k.Prefix != k.Plaintext[:11] {
		t.Errorf("Prefix = %q", k.Prefix)
	}
	want := sha256.Sum256([]byte(k.Plaintext))
	if !bytes.Equal(k.Hash, want[:]) {
		t.Error("Hash is not SHA-256 of plaintext")
	}
	h, err := HashAPIKey(k.Plaintext)
	if err != nil || !bytes.Equal(h, k.Hash) {
		t.Fatalf("HashAPIKey(generated) = %x, %v", h, err)
	}
}

func TestGenerateAPIKeyUniqueAndUnbiasedAlphabet(t *testing.T) {
	seen := make(map[string]bool)
	chars := make(map[rune]int)
	for range 2000 {
		k := GenerateAPIKey()
		if seen[k.Plaintext] {
			t.Fatalf("duplicate key generated")
		}
		seen[k.Plaintext] = true
		for _, c := range k.Plaintext[len(APIKeyPrefix):] {
			chars[c]++
		}
	}
	if len(chars) != 62 {
		t.Errorf("used %d distinct characters, want all 62", len(chars))
	}
}

func TestHashAPIKeyRejectsMalformed(t *testing.T) {
	valid := GenerateAPIKey().Plaintext
	for name, in := range map[string]string{
		"empty":        "",
		"no prefix":    "xx_" + valid[3:],
		"too short":    valid[:len(valid)-1],
		"too long":     valid + "a",
		"bad char":     valid[:len(valid)-1] + "-",
		"bearer token": "Bearer " + valid,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := HashAPIKey(in); !errors.Is(err, ErrMalformedAPIKey) {
				t.Errorf("err = %v, want ErrMalformedAPIKey", err)
			}
		})
	}
}

func TestAPIKeyIsRedacted(t *testing.T) {
	k := GenerateAPIKey()
	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("created", "key", k)
	for name, s := range map[string]string{
		"%v":  fmt.Sprintf("%v", k),
		"%#v": fmt.Sprintf("%#v", k),
		"log": logBuf.String(),
	} {
		if strings.Contains(s, k.Plaintext) {
			t.Errorf("%s output leaks plaintext key: %q", name, s)
		}
		if !strings.Contains(s, k.Prefix) {
			t.Errorf("%s output missing prefix: %q", name, s)
		}
	}
}

func TestGenerateSigningSecret(t *testing.T) {
	s := GenerateSigningSecret()
	if !strings.HasPrefix(s, SigningSecretPrefix) {
		t.Fatalf("missing prefix: %q", s)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(s, SigningSecretPrefix))
	if err != nil || len(raw) != 32 {
		t.Fatalf("body must be base64 of 32 bytes: len=%d err=%v", len(raw), err)
	}
	if s == GenerateSigningSecret() {
		t.Error("signing secrets are not random")
	}
}
