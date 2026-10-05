package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
)

func TestExecuteRejectsMalformedURL(t *testing.T) {
	e := NewHTTPExecutor()
	if _, err := e.Execute(context.Background(), "://not-a-url", []byte(`{}`)); err == nil {
		t.Fatal("expected error for malformed URL, got nil")
	}
}

func TestExecuteRejectsBlockedURL(t *testing.T) {
	e := NewHTTPExecutor()
	// Link-local metadata endpoint must be rejected before any HTTP call.
	if _, err := e.Execute(context.Background(), "http://169.254.169.254/", []byte(`{}`)); err == nil {
		t.Fatal("expected SSRF rejection, got nil")
	}
}

func TestHTTPExecutor_FollowsSameHost307PreservingMethod(t *testing.T) {
	var gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/redir", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final", http.StatusTemporaryRedirect)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	e := NewHTTPExecutor()
	code, err := e.ExecuteJob(context.Background(), server.URL+"/redir", []byte(`{}`), "", 0, validator.Config{AllowPrivateIPs: true})
	if err != nil {
		t.Fatalf("ExecuteJob: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("code = %d, want 200", code)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", gotMethod)
	}
}

func TestHTTPExecutor_Rejects302Downgrade(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/redir", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	e := NewHTTPExecutor()
	if _, err := e.ExecuteJob(context.Background(), server.URL+"/redir", []byte(`{}`), "", 0, validator.Config{AllowPrivateIPs: true}); err == nil {
		t.Fatal("302 redirect: want error (no silent POST->GET downgrade), got nil")
	}
}

func TestHTTPExecutor_RejectsOversizedResponseBody(t *testing.T) {
	big := make([]byte, maxResponseBodyBytes+100)
	for i := range big {
		big[i] = 'x'
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(big)
	}))
	defer server.Close()

	e := NewHTTPExecutor()
	if _, err := e.ExecuteJob(context.Background(), server.URL, []byte(`{}`), "", 0, validator.Config{AllowPrivateIPs: true}); err == nil {
		t.Fatal("oversized body: want error, got nil")
	}
}
