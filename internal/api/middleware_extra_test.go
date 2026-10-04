package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusWriterImplicit200(t *testing.T) {
	rec := httptest.NewRecorder()
	w := &StatusWriter{ResponseWriter: rec}
	n, err := w.Write([]byte("hi"))
	if err != nil || n != 2 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected implicit 200, got %d", rec.Code)
	}
	// Second WriteHeader must be a no-op (first-write-wins).
	w.WriteHeader(http.StatusTeapot)
	if rec.Code != http.StatusOK {
		t.Fatalf("second WriteHeader changed status to %d", rec.Code)
	}
}

func TestRecoveryCatchesPanic(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})
	h := Recovery(log, next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped Recovery middleware: %v", r)
			}
		}()
		h.ServeHTTP(rec, req)
	}()
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 after panic, got %d", rec.Code)
	}
}

func TestRecoveryPassThrough(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h := Recovery(log, next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("expected 418, got %d", rec.Code)
	}
}

func TestRequestIDHelpers(t *testing.T) {
	ctx := context.WithValue(context.Background(), RequestIDKey, "abc-123")
	id, ok := RequestIDFromContext(ctx)
	if !ok || id != "abc-123" {
		t.Fatalf("RequestIDFromContext = %q, %v", id, ok)
	}
	if got := RequestIDFromContextOrEmpty(ctx); got != "abc-123" {
		t.Fatalf("OrEmpty = %q", got)
	}
	if _, ok := RequestIDFromContext(context.Background()); ok {
		t.Fatal("expected missing request ID")
	}
	if got := RequestIDFromContextOrEmpty(context.Background()); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}
