package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 9.5: kill-leader handoff within ~2x TTL.
func TestLeaderLock_HandoffWithinTwoTTL(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	const lockKey = "test:scheduler:leader:handoff"
	if err := client.Del(ctx, lockKey).Err(); err != nil {
		t.Fatalf("clean lock: %v", err)
	}

	ttl := 1 * time.Second
	leader := NewLeaderLock(client, lockKey, "leader", ttl)
	standby := NewLeaderLock(client, lockKey, "standby", ttl)

	ok, err := leader.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("leader acquire: %v %v", ok, err)
	}

	// Leader dies without release/heartbeat: let the TTL expire.
	start := time.Now()
	deadline := start.Add(2*ttl + 5*time.Second)
	var acquired time.Time
	for {
		ok, err := standby.Acquire(ctx)
		if err != nil {
			t.Fatalf("standby acquire: %v", err)
		}
		if ok {
			acquired = time.Now()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("standby never acquired the lock after leader death")
		}
		time.Sleep(100 * time.Millisecond)
	}
	handoff := acquired.Sub(start)
	if handoff > 2*ttl+2*time.Second {
		t.Fatalf("handoff took %v, want within ~2x TTL (%v)", handoff, 2*ttl)
	}
	isLeader, err := standby.IsLeader(ctx)
	if err != nil || !isLeader {
		t.Fatalf("standby should be leader after handoff: %v %v", isLeader, err)
	}
}
