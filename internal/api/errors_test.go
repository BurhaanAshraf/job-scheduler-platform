package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteError(t *testing.T) {
	recorder := httptest.NewRecorder()

	WriteError(
		recorder, http.StatusBadRequest, "INVALID_REQUEST" , "invalid request",
	)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}

	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("expected application/json, got %s", contentType)
	}

	expected := `{"error":{"code":"INVALID_REQUEST","message":"invalid request"}}` + "\n"
	if recorder.Body.String() != expected {
		t.Fatalf("unexpected body: %s", recorder.Body.String())
	}
}
