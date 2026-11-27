package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
)

// Ciphertext layout:  version(1) || nonce(12) || AES-GCM sealed data + tag(16)
//
// The version byte leaves room for key rotation later (e.g. version 2 = new
// key) without ambiguity about how stored rows were encrypted.
const (
	cipherVersion1 byte = 1
	nonceSize           = 12
	tagSize             = 16
	overhead            = 1 + nonceSize + tagSize
)

// ErrDecrypt is returned for any decryption failure. It deliberately gives no
// detail (wrong key, tampering, wrong associated data, truncation).
var ErrDecrypt = errors.New("secrets: decryption failed")

// Cipher encrypts and decrypts small secrets with AES-256-GCM. It is safe for
// concurrent use.
type Cipher struct {
	aead cipher.AEAD
}

// NewCipher returns a Cipher using key.
func NewCipher(key Key) (*Cipher, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

// Encrypt seals plaintext with a fresh random nonce. associatedData is
// authenticated but not stored. The same value must be passed to Decrypt,
// which binds a ciphertext to its context (for example an endpoint ID) so it
// cannot be copied onto another row. It may be nil.
func (c *Cipher) Encrypt(plaintext, associatedData []byte) []byte {
	out := make([]byte, 1+nonceSize, overhead+len(plaintext))
	out[0] = cipherVersion1
	_, _ = rand.Read(out[1 : 1+nonceSize]) // crypto/rand.Read never returns an error.
	return c.aead.Seal(out, out[1:1+nonceSize], plaintext, associatedData)
}

// Decrypt opens a ciphertext produced by Encrypt with the same associatedData.
func (c *Cipher) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < overhead || ciphertext[0] != cipherVersion1 {
		return nil, ErrDecrypt
	}
	nonce, sealed := ciphertext[1:1+nonceSize], ciphertext[1+nonceSize:]
	plaintext, err := c.aead.Open(nil, nonce, sealed, associatedData)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plaintext, nil
}
