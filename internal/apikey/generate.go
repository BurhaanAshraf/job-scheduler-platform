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

	sum := sha256.Sum256([]byte(rawKey))

	hashedKey = hex.EncodeToString(sum[:])

	return rawKey, hashedKey, nil
}
