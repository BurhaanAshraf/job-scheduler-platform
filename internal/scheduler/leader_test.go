package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLeaderLock_SecondAcquisitionFails(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	const lockKey = "test:scheduler:leader"

	if err := client.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("delete test lock: %v", err)
	}

	lockA := NewLeaderLock(
		client,
		lockKey,
		"scheduler-a",
		10*time.Second,
	)

	lockB := NewLeaderLock(
		client,
		lockKey,
		"scheduler-b",
		10*time.Second,
	)

	acquired, err := lockA.Acquire(ctx)
	if err != nil {
		t.Fatalf("scheduler A acquire: %v", err)
	}

	if !acquired {
		t.Fatal("scheduler A should acquire the lock")
	}

	acquired, err = lockB.Acquire(ctx)
	if err != nil {
		t.Fatalf("scheduler B acquire: %v", err)
	}

	if acquired {
		t.Fatal("scheduler B should not acquire the lock while A holds it")
	}
}
func TestLeaderLock_HeartbeatRenewsLock(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	const lockKey = "test:scheduler:leader:heartbeat"

	if err := client.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("delete test lock: %v", err)
	}

	lock := NewLeaderLock(
		client,
		lockKey,
		"scheduler-a",
		2*time.Second,
	)

	acquired, err := lock.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}

	if !acquired {
		t.Fatal("scheduler should acquire the lock")
	}

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)

	heartbeatDone := make(chan error, 1)

	go func() {
		heartbeatDone <- lock.Heartbeat(heartbeatCtx)
	}()

	// Wait longer than the original 2-second TTL.
	// The heartbeat runs every 1 second and should keep renewing the lock.
	time.Sleep(3 * time.Second)

	heartbeatCancel()

	// Wait for the heartbeat goroutine to actually stop.
	if err := <-heartbeatDone; err != context.Canceled {
		t.Fatalf("heartbeat should stop with context cancellation, got %v", err)
	}

	// The lock was renewed while the heartbeat was running.
	stillOwned, err := lock.Renew(ctx)
	if err != nil {
		t.Fatalf("check renewed lock: %v", err)
	}

	if !stillOwned {
		t.Fatal("lock should still be owned after its original TTL")
	}
}
func TestLeaderLock_ReleaseDoesNotDeleteAnotherOwnersLock(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	const lockKey = "test:scheduler:leader:release"

	if err := client.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("delete test lock: %v", err)
	}

	lockA := NewLeaderLock(
		client,
		lockKey,
		"scheduler-a",
		500*time.Millisecond,
	)

	lockB := NewLeaderLock(
		client,
		lockKey,
		"scheduler-b",
		500*time.Millisecond,
	)

	acquired, err := lockA.Acquire(ctx)
	if err != nil {
		t.Fatalf("scheduler A acquire: %v", err)
	}

	if !acquired {
		t.Fatal("scheduler A should acquire the lock")
	}

	time.Sleep(600 * time.Millisecond)

	acquired, err = lockB.Acquire(ctx)
	if err != nil {
		t.Fatalf("scheduler B acquire: %v", err)
	}

	if !acquired {
		t.Fatal("scheduler B should acquire the lock after A's TTL expires")
	}

	released, err := lockA.Release(ctx)
	if err != nil {
		t.Fatalf("scheduler A release: %v", err)
	}

	if released {
		t.Fatal("scheduler A must not release scheduler B's lock")
	}

	isLeader, err := lockB.IsLeader(ctx)
	if err != nil {
		t.Fatalf("check scheduler B leadership: %v", err)
	}

	if !isLeader {
		t.Fatal("scheduler B's lock should still be held")
	}
}
