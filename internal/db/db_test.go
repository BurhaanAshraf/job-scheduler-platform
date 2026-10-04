package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
)

func testConfig(dsn string) config.Config {
	return config.Config{
		DBDSN:      dsn,
		RedisAddr:  "localhost:6379",
		APIPort:    "4000",
		LogLevel:   "info",
		DBMaxConns: 5,
	}
}

func TestNewPoolOK(t *testing.T) {
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Skip("JOB_SCHEDULER_DB_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, testConfig(dsn))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	if err := Ping(ctx, pool); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestNewPoolBadDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := NewPool(ctx, testConfig("://not-a-dsn")); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestNewPoolUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Nothing listens on port 1: fast refusal, no hang.
	cfg := testConfig("postgres://u:p@localhost:1/db?sslmode=disable")
	if _, err := NewPool(ctx, cfg); err == nil {
		t.Fatal("expected connection error, got nil")
	}
}

func TestPingClosedPool(t *testing.T) {
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Skip("JOB_SCHEDULER_DB_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, testConfig(dsn))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	pool.Close()
	if err := Ping(ctx, pool); err == nil {
		t.Fatal("expected ping error on closed pool, got nil")
	}
}
