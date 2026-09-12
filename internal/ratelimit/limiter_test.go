package ratelimit

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Fatal("REDIS_ADDR is required")
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Fatalf("failed to connect to Redis: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

func TestLimiter_AllowsRequestsWithinLimit(t *testing.T) {
	client := testRedisClient(t)

	limiter := New(client, 3, time.Minute)

	clientID := fmt.Sprintf("test-client-%d", time.Now().UnixNano())

	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		allowed, err := limiter.Allow(ctx, clientID)
		if err != nil {
			t.Fatalf("request %d returned error: %v", i, err)
		}

		if !allowed {
			t.Fatalf("request %d was rejected, want allowed", i)
		}
	}
}

func TestLimiter_RejectsRequestAfterLimit(t *testing.T) {
	client := testRedisClient(t)

	limiter := New(client, 3, time.Minute)

	clientID := fmt.Sprintf("test-client-%d", time.Now().UnixNano())

	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		allowed, err := limiter.Allow(ctx, clientID)
		if err != nil {
			t.Fatalf("request %d returned error: %v", i, err)
		}

		if !allowed {
			t.Fatalf("request %d was rejected, want allowed", i)
		}
	}

	allowed, err := limiter.Allow(ctx, clientID)
	if err != nil {
		t.Fatalf("request 4 returned error: %v", err)
	}

	if allowed {
		t.Fatal("request 4 was allowed, want rejected")
	}
}

func TestLimiter_IsolatesClients(t *testing.T) {
	client := testRedisClient(t)

	limiter := New(client, 2, time.Minute)

	clientA := fmt.Sprintf("client-a-%d", time.Now().UnixNano())
	clientB := fmt.Sprintf("client-b-%d", time.Now().UnixNano())

	ctx := context.Background()

	// Exhaust client A's limit.
	for i := 1; i <= 2; i++ {
		allowed, err := limiter.Allow(ctx, clientA)
		if err != nil {
			t.Fatalf("client A request %d returned error: %v", i, err)
		}

		if !allowed {
			t.Fatalf("client A request %d was rejected, want allowed", i)
		}
	}

	// Client A is now rejected.
	allowed, err := limiter.Allow(ctx, clientA)
	if err != nil {
		t.Fatalf("client A request 3 returned error: %v", err)
	}

	if allowed {
		t.Fatal("client A request 3 was allowed, want rejected")
	}

	// Client B has its own independent counter.
	allowed, err = limiter.Allow(ctx, clientB)
	if err != nil {
		t.Fatalf("client B request 1 returned error: %v", err)
	}

	if !allowed {
		t.Fatal("client B request 1 was rejected, want allowed")
	}
}

func TestLimiter_SetsExpiration(t *testing.T) {
	client := testRedisClient(t)

	window := 10 * time.Second
	limiter := New(client, 3, window)

	clientID := fmt.Sprintf("test-client-%d", time.Now().UnixNano())

	ctx := context.Background()

	allowed, err := limiter.Allow(ctx, clientID)
	if err != nil {
		t.Fatalf("Allow() returned error: %v", err)
	}

	if !allowed {
		t.Fatal("first request was rejected, want allowed")
	}

	windowNumber := time.Now().Unix() / int64(window.Seconds())
	key := fmt.Sprintf("rate_limit:%s:%d", clientID, windowNumber)

	ttl, err := client.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("failed to get Redis TTL: %v", err)
	}

	if ttl <= 0 {
		t.Fatalf("TTL = %v, want positive TTL", ttl)
	}

	if ttl > window {
		t.Fatalf("TTL = %v, want <= %v", ttl, window)
	}
}
