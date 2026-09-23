package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPExecutor_ExecutesCallbackWithExactPayload(t *testing.T) {
	payload := []byte(`{"type":"email","to":"test@example.com","message":"hello"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}

		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf(
				"expected Content-Type application/json, got %q",
				r.Header.Get("Content-Type"),
			)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
			return
		}

		if string(body) != string(payload) {
			t.Errorf(
				"expected body %q, got %q",
				string(payload),
				string(body),
			)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	executor := NewHTTPExecutor()

	responseCode, err := executor.Execute(context.Background(), server.URL, payload)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if responseCode != http.StatusOK {
		t.Fatalf("expected response code %d, got %d", http.StatusOK, responseCode)
	}
}

func TestHTTPExecutor_TreatsNon2xxAsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	executor := NewHTTPExecutor()

	responseCode, err := executor.Execute(
		context.Background(),
		server.URL,
		[]byte(`{"hello":"world"}`),
	)

	if err == nil {
		t.Fatal("expected Execute to fail")
	}

	if responseCode != http.StatusInternalServerError {
		t.Fatalf(
			"expected response code %d, got %d",
			http.StatusInternalServerError,
			responseCode,
		)
	}
}
