package apikey

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

const keyBytes = 32

func Generate() (rawKey string, hashedKey string, err error) {
	randomBytes := make([]byte, keyBytes)

	if _, err := rand.Read(randomBytes); err != nil {
		return "", "", err
	}

	rawKey = hex.EncodeToString(randomBytes)

	hashedKey = Hash(rawKey)

	return rawKey, hashedKey, nil
}

// Hash returns the hex-encoded SHA-256 of a raw API key.
// Keys carry 256 bits of entropy, so a fast salted-equivalent hash is
// sufficient; the single helper keeps generation and verification in sync.
func Hash(rawKey string) string {
	sum := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(sum[:])
}
