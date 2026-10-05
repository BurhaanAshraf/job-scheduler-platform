package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServer_UnknownRouteReturns404(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	handler := NewHandler(nil, nil, nil, slog.Default())
	router := Server(logger, handler, nil, nil, nil)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/testHttp404", nil)
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404:, got %d", recorder.Code)
	}
}
