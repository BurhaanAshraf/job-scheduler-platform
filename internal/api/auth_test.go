package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/apikey"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
)

func apikeyGenerateForTest() (string, string, error) {
	return apikey.Generate()
}

// hashEchoStore accepts only the hash of expectHashOf.
type hashEchoStore struct {
	expectHashOf string
}

func (s *hashEchoStore) GetAPIKeyByHash(_ context.Context, hash string) (*repository.APIKey, error) {
	_ = rand.Reader
	_ = hex.EncodeToString
	if hash != apikey.Hash(s.expectHashOf) {
		return nil, repository.ErrNotFound
	}
	return &repository.APIKey{ClientName: "cli-client"}, nil
}

type stubKeyStore struct {
	keys map[string]*repository.APIKey
	err  error
}

func (s *stubKeyStore) GetAPIKeyByHash(_ context.Context, hash string) (*repository.APIKey, error) {
	if s.err != nil {
		return nil, s.err
	}
	k, ok := s.keys[hash]
	if !ok {
		return nil, repository.ErrNotFound
	}
	return k, nil
}

func TestAPIKeyAuth_MissingHeader401(t *testing.T) {
	store := &stubKeyStore{keys: map[string]*repository.APIKey{}}
	h := APIKeyAuth(store)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing header: got %d, want 401", rec.Code)
	}
}

func TestAPIKeyAuth_BadScheme401(t *testing.T) {
	store := &stubKeyStore{keys: map[string]*repository.APIKey{}}
	h := APIKeyAuth(store)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Token abc123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad scheme: got %d, want 401", rec.Code)
	}
}

func TestAPIKeyAuth_RevokedKey401(t *testing.T) {
	revokedAt := time.Now().UTC()
	store := &stubKeyStore{keys: map[string]*repository.APIKey{
		"deadbeef": {ClientName: "revoked-client", RevokedAt: &revokedAt},
	}}
	// Hash lookup is stubbed by exact string; bypass hashing by pre-seeding
	// the middleware path via a raw key whose Hash() we control is complex,
	// so instead assert the revoked branch directly through a store that
	// returns a revoked key for any hash.
	h := APIKeyAuth(&revokedAnyStore{revokedAt: revokedAt})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	_ = store
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer anything")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked key: got %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got == "" {
		t.Fatal("expected JSON error content type")
	}
}

type revokedAnyStore struct {
	revokedAt time.Time
}

func (s *revokedAnyStore) GetAPIKeyByHash(_ context.Context, _ string) (*repository.APIKey, error) {
	return &repository.APIKey{ClientName: "revoked-client", RevokedAt: &s.revokedAt}, nil
}

func TestAPIKeyAuth_UnknownKey401(t *testing.T) {
	store := &stubKeyStore{keys: map[string]*repository.APIKey{}}
	h := APIKeyAuth(store)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer unknown-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown key: got %d, want 401", rec.Code)
	}
}

func TestAPIKeyAuth_ValidKeyAttachesClient(t *testing.T) {
	store := &validStore{}
	h := APIKeyAuth(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, ok := ClientNameFromContext(r.Context())
		if !ok || name != "acme" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid key: got %d, want 200", rec.Code)
	}
}

type validStore struct{}

func (s *validStore) GetAPIKeyByHash(_ context.Context, _ string) (*repository.APIKey, error) {
	return &repository.APIKey{ClientName: "acme"}, nil
}

func TestAPIKeyAuth_GeneratedRawKeyRoundTrip(t *testing.T) {
	// 4.3 Done-when: CLI-printed raw key authenticates through the middleware.
	raw, _, err := apikeyGenerateForTest()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	store := &hashEchoStore{expectHashOf: raw}
	h := APIKeyAuth(store)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("generated raw key: got %d, want 200", rec.Code)
	}
}
