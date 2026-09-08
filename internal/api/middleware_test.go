package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecovery(t *testing.T) {

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := Recovery(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went wrong")
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/recoverytest", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", recorder.Code)
	}

	if !strings.Contains(buf.String(), "something went wrong") {
		t.Fatal("expected panic message in log")
	}

	if !strings.Contains(buf.String(), "stack") {
		t.Fatal("expected stack trace in log")
	}

}

func TestLogging(t *testing.T) {
	var buf bytes.Buffer

	log := slog.New(
		slog.NewJSONHandler(&buf, nil),
	)

	handler := Logging(
		log,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}),
	)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/jobs",
		nil,
	)

	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", recorder.Code)
	}

	var entry map[string]any

	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to decode log: %v", err)
	}

	if bytes.Count(buf.Bytes(), []byte("\n")) != 1 {
		t.Fatalf("expected exactly one log line, got %d", bytes.Count(buf.Bytes(), []byte("\n")))
	}

	if entry["method"] != http.MethodPost {
		t.Fatalf("expected method POST, got %v", entry["method"])
	}

	if entry["path"] != "/v1/jobs" {
		t.Fatalf("expected path /v1/jobs, got %v", entry["path"])
	}

	if entry["status"] != float64(http.StatusCreated) {
		t.Fatalf("expected status 201, got %v", entry["status"])
	}

	if entry["request_id"] == nil || entry["request_id"] == "" {
		t.Fatal("expected request_id")
	}

	if entry["latency"] == nil || entry["latency"] == "" {
		t.Fatal("expected latency")
	}
}
