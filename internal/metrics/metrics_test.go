package metrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func testClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}
	return c
}

func TestNewCollector(t *testing.T) {
	if c := NewCollector(testClient(t)); c == nil {
		t.Fatal("NewCollector returned nil")
	}
}

func TestDescribe(t *testing.T) {
	c := NewCollector(testClient(t))
	ch := make(chan *prometheus.Desc, 8)
	c.Describe(ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	if n != 4 {
		t.Fatalf("Describe sent %d descs, want 4", n)
	}
}

func TestCollectWithValues(t *testing.T) {
	ctx := context.Background()
	r := testClient(t)
	_ = r.Del(ctx, JobsSubmittedKey, JobsCompletedKey, JobsFailedKey).Err()
	if err := Increment(ctx, r, JobsSubmittedKey); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := Increment(ctx, r, JobsCompletedKey); err != nil {
		t.Fatalf("Increment: %v", err)
	}

	c := NewCollector(r)
	ch := make(chan prometheus.Metric, 8)
	c.Collect(ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	if n != 4 {
		t.Fatalf("Collect sent %d metrics, want 4", n)
	}
	if got, _ := r.Get(ctx, JobsSubmittedKey).Int64(); got != 1 {
		t.Fatalf("submitted = %d, want 1", got)
	}
	_ = r.Del(ctx, JobsSubmittedKey, JobsCompletedKey).Err()
}

func TestIncrementError(t *testing.T) {
	bad := redis.NewClient(&redis.Options{Addr: "localhost:1"})
	defer func() { _ = bad.Close() }()
	if err := Increment(context.Background(), bad, JobsSubmittedKey); err == nil {
		t.Fatal("expected error for unreachable Redis, got nil")
	}
}
