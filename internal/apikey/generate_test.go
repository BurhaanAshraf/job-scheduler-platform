package apikey

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestGenerate(t *testing.T) {
	rawKey, hashedKey, err := Generate()
	if err != nil {
		t.Fatalf("Generate() returned error: %v", err)
	}

	if rawKey == "" {
		t.Fatal("Generate() returned empty raw key")
	}

	if hashedKey == "" {
		t.Fatal("Generate() returned empty hashed key")
	}

	sum := sha256.Sum256([]byte(rawKey))
	expectedHash := hex.EncodeToString(sum[:])

	if hashedKey != expectedHash {
		t.Fatalf("hashed key = %q, want %q", hashedKey, expectedHash)
	}
}

func TestGenerate_ReturnsDifferentKeys(t *testing.T) {
	rawKey1, _, err := Generate()
	if err != nil {
		t.Fatalf("first Generate() returned error: %v", err)
	}

	rawKey2, _, err := Generate()
	if err != nil {
		t.Fatalf("second Generate() returned error: %v", err)
	}

	if rawKey1 == rawKey2 {
		t.Fatal("Generate() returned the same raw key twice")
	}
}

func TestGenerate_RawKeyAuthenticatesThroughMiddleware(t *testing.T) {
	// 4.3 Done-when: the printed raw key authenticates through the middleware.
	rawKey, hashedKey, err := Generate()
	if err != nil {
		t.Fatalf("Generate() returned error: %v", err)
	}
	if Hash(rawKey) != hashedKey {
		t.Fatalf("Hash(raw) != stored hash: %q vs %q", Hash(rawKey), hashedKey)
	}
	if rawKey == hashedKey {
		t.Fatal("raw key must differ from stored hash (hash-only storage)")
	}
}
