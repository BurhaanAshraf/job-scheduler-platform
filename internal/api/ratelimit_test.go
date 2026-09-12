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

func TestRateLimit_Returns429WithRetryAfter(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	t.Cleanup(func() {
		redisClient.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}

	limiter := ratelimit.New(redisClient, 1, time.Minute)

	handler := RateLimit(limiter)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	clientID := "test-client-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	makeRequest := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		req = req.WithContext(
			context.WithValue(req.Context(), clientNameKey, clientID),
		)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		return rec
	}

	// First request is allowed.
	first := makeRequest()

	if first.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", first.Code)
	}

	// Second request exceeds the limit.
	second := makeRequest()

	if second.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"second request: expected 429, got %d",
			second.Code,
		)
	}

	retryAfter := second.Header().Get("Retry-After")

	if retryAfter == "" {
		t.Fatal("Retry-After header is missing")
	}

	retryAfterSeconds, err := strconv.Atoi(retryAfter)
	if err != nil {
		t.Fatalf(
			"Retry-After = %q, want positive integer",
			retryAfter,
		)
	}

	if retryAfterSeconds <= 0 {
		t.Fatalf(
			"Retry-After = %d, want positive integer",
			retryAfterSeconds,
		)
	}
}
