package executor

import (
	"context"
	"testing"
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
