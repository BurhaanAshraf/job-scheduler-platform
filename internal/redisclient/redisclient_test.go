package redisclient

import (
	"context"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/redis/go-redis/v9"
)

func testConfig(addr string) config.Config {
	return config.Config{
		DBDSN:      "postgres://u:p@localhost:5432/db?sslmode=disable",
		RedisAddr:  addr,
		APIPort:    "4000",
		LogLevel:   "info",
		DBMaxConns: 5,
	}
}

func TestNewOK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probe := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = probe.Close() }()
	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}

	c, err := New(ctx, testConfig("localhost:6379"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()
}

func TestNewUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := New(ctx, testConfig("localhost:1")); err == nil {
		t.Fatal("expected error for unreachable Redis, got nil")
	}
}
