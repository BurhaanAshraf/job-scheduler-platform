package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

func testIPRedis(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}
	return c
}

func TestIPRateLimit_ThrottlesPastQuota(t *testing.T) {
	limiter := ratelimit.New(testIPRedis(t), 2, time.Minute)
	handler := IPRateLimit(limiter)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func() int {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
		req.RemoteAddr = "10.9.9.9:1234"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := do(); code != http.StatusOK {
		t.Fatalf("request 1: want 200, got %d", code)
	}
	if code := do(); code != http.StatusOK {
		t.Fatalf("request 2: want 200, got %d", code)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs", nil)
	req.RemoteAddr = "10.9.9.9:1234"
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("request 3: want 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 without Retry-After")
	}
}

func TestIPRateLimit_GroupsByForwardedFor(t *testing.T) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	limiter := ratelimit.New(testIPRedis(t), 1, time.Minute)
	handler := IPRateLimit(limiter)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func(remote, fwd string) int {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
		req.RemoteAddr = remote
		if fwd != "" {
			req.Header.Set("X-Forwarded-For", fwd)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	// Same proxy connection, different original clients: separate buckets.
	if code := do("10.0.0.1:80", "203.0.113.7-"+suffix); code != http.StatusOK {
		t.Fatalf("client A: want 200, got %d", code)
	}
	if code := do("10.0.0.1:80", "203.0.113.8-"+suffix); code != http.StatusOK {
		t.Fatalf("client B behind same proxy: want 200, got %d", code)
	}
	// Client A again: over its own quota of 1.
	if code := do("10.0.0.1:80", "203.0.113.7-"+suffix); code != http.StatusTooManyRequests {
		t.Fatalf("client A replay: want 429, got %d", code)
	}
}

func TestIPRateLimit_NilLimiterPassesThrough(t *testing.T) {
	handler := IPRateLimit(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("nil limiter: want passthrough 200, got %d", rec.Code)
	}
}
