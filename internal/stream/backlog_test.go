package stream

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Uses an isolated stream/group so parallel packages and the live stack
// can never pollute the assertions.
func TestStreamBacklog(t *testing.T) {
	ctx := context.Background()
	c := testRedisClient(t)

	streamName := "test:backlog:" + uuid.NewString()
	groupName := "test-group-" + uuid.NewString()
	t.Cleanup(func() {
		_ = c.Del(ctx, streamName).Err()
		_ = c.XGroupDestroy(ctx, streamName, groupName).Err()
	})

	// No stream yet: zero backlog, no error (XINFO/XLEN on missing key).
	if n, err := StreamBacklog(ctx, c, streamName, groupName); err != nil || n != 0 {
		t.Fatalf("empty = %d, %v; want 0, nil", n, err)
	}

	if err := c.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := c.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: map[string]any{"i": i}}).Err(); err != nil {
			t.Fatalf("XAdd: %v", err)
		}
	}

	// 3 undelivered: backlog 3.
	if n, err := StreamBacklog(ctx, c, streamName, groupName); err != nil || n != 3 {
		t.Fatalf("undelivered = %d, %v; want 3, nil", n, err)
	}

	// Read 2 without ack: still backlog 3 (1 undelivered + 2 pending).
	msgs, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: groupName, Consumer: "c1", Streams: []string{streamName, ">"}, Count: 2, Block: 0,
	}).Result()
	if err != nil || len(msgs[0].Messages) != 2 {
		t.Fatalf("XReadGroup = %v, %v", msgs, err)
	}
	if n, err := StreamBacklog(ctx, c, streamName, groupName); err != nil || n != 3 {
		t.Fatalf("pending = %d, %v; want 3, nil", n, err)
	}

	// Ack both: backlog drops to the 1 remaining undelivered entry.
	if err := c.XAck(ctx, streamName, groupName, msgs[0].Messages[0].ID, msgs[0].Messages[1].ID).Err(); err != nil {
		t.Fatalf("XAck: %v", err)
	}
	if n, err := StreamBacklog(ctx, c, streamName, groupName); err != nil || n != 1 {
		t.Fatalf("acked = %d, %v; want 1, nil", n, err)
	}
}
