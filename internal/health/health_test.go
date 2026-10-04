package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubPinger struct {
	err   error
	block time.Duration
}

func (s stubPinger) Ping(ctx context.Context) error {
	if s.block > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.block):
		}
	}
	return s.err
}

func TestNewChecker(t *testing.T) {
	c := NewChecker(stubPinger{}, stubPinger{}, time.Second)
	if c == nil {
		t.Fatal("NewChecker returned nil")
	}
}

func TestCheckHealthy(t *testing.T) {
	c := NewChecker(stubPinger{}, stubPinger{}, time.Second)
	if err := c.Check(context.Background()); err != nil {
		t.Fatalf("expected healthy, got %v", err)
	}
}

func TestCheckDBFailure(t *testing.T) {
	c := NewChecker(stubPinger{err: errors.New("db down")}, stubPinger{}, time.Second)
	err := c.Check(context.Background())
	if err == nil {
		t.Fatal("expected database error, got nil")
	}
}

func TestCheckRedisFailure(t *testing.T) {
	c := NewChecker(stubPinger{}, stubPinger{err: errors.New("redis down")}, time.Second)
	err := c.Check(context.Background())
	if err == nil {
		t.Fatal("expected redis error, got nil")
	}
}

func TestCheckTimeout(t *testing.T) {
	c := NewChecker(stubPinger{block: 5 * time.Second}, stubPinger{}, 50*time.Millisecond)
	if err := c.Check(context.Background()); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}
