package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRemoveScheduled(t *testing.T) {
	ctx := context.Background()
	c := testRedisClient(t)

	// Missing member: best-effort, no error.
	if err := RemoveScheduled(ctx, c, "no-such-job"); err != nil {
		t.Fatalf("RemoveScheduled missing member: %v", err)
	}

	// Present member + payload hash: both removed.
	if err := c.ZAdd(ctx, ScheduledSet, redis.Z{Score: 1, Member: "job-1"}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if err := c.HSet(ctx, ScheduledPayloads, "job-1", "{}").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := RemoveScheduled(ctx, c, "job-1"); err != nil {
		t.Fatalf("RemoveScheduled: %v", err)
	}
	if _, err := c.ZRank(ctx, ScheduledSet, "job-1").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("member still present after RemoveScheduled (err=%v)", err)
	}
	if exists, _ := c.HExists(ctx, ScheduledPayloads, "job-1").Result(); exists {
		t.Fatal("payload still present after RemoveScheduled")
	}
	_ = c.Del(ctx, ScheduledSet, ScheduledPayloads).Err()
}

func TestParseQueueGeneration(t *testing.T) {
	cases := []struct {
		in   any
		want int64
	}{
		{"7", 7},
		{"", 1},
		{"bogus", 1},
		{"0", 1},
		{"-3", 1},
		{int64(4), 4},
		{int64(0), 1},
		{5, 5},
		{0, 1},
		{nil, 1},
		{3.0, 1},
	}
	for _, c := range cases {
		if got := parseQueueGeneration(c.in); got != c.want {
			t.Errorf("parseQueueGeneration(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestClaimRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := testRedisClient(t)

	if err := EnsureConsumerGroup(ctx, c); err != nil {
		t.Fatalf("EnsureConsumerGroup: %v", err)
	}
	msgID, err := EnqueueDue(ctx, c, "claim-job-1", []byte(`{}`), 1)
	if err != nil {
		t.Fatalf("EnqueueDue: %v", err)
	}
	// First delivery to consumer-a makes it pending for the group.
	read, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: "consumer-a",
		Streams:  []string{ReadyStream, ">"},
		Count:    10,
		Block:    2 * time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	found := false
	for _, s := range read {
		for _, m := range s.Messages {
			if m.ID == msgID {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("enqueued message %s not delivered", msgID)
	}

	// Fresh message is not idle: tiny-idle claim grabs it for consumer-b.
	time.Sleep(50 * time.Millisecond)
	claimed, err := Claim(ctx, c, "consumer-b", time.Millisecond, msgID)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != msgID || claimed[0].JobID != "claim-job-1" {
		t.Fatalf("unexpected claim result: %+v", claimed)
	}
	_ = c.XAck(ctx, ReadyStream, ConsumerGroup, msgID).Err()
}
