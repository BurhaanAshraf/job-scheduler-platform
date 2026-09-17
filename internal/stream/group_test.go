package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	t.Cleanup(func() {
		client.Close()
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}

	return client
}

func TestEnsureConsumerGroup_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	client := testRedisClient(t)

	// Isolate this test from previous runs.
	if err := client.Del(ctx, ReadyStream).Err(); err != nil {
		t.Fatal(err)
	}

	if err := EnsureConsumerGroup(ctx, client); err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	if err := EnsureConsumerGroup(ctx, client); err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	groups, err := client.XInfoGroups(ctx, ReadyStream).Result()
	if err != nil {
		t.Fatalf("XINFO GROUPS failed: %v", err)
	}

	if len(groups) != 1 {
		t.Fatalf("expected exactly 1 consumer group, got %d", len(groups))
	}

	if groups[0].Name != ConsumerGroup {
		t.Fatalf("expected group %q, got %q", ConsumerGroup, groups[0].Name)
	}
}

func TestEnqueueDue_RoundTripsMessage(t *testing.T) {
	ctx := context.Background()
	client := testRedisClient(t)

	if err := client.Del(ctx, ReadyStream).Err(); err != nil {
		t.Fatal(err)
	}

	jobID := "test-job-123"
	payload := []byte(`{"type":"email","to":"test@example.com"}`)

	messageID, err := EnqueueDue(ctx, client, jobID, payload)
	if err != nil {
		t.Fatalf("EnqueueDue failed: %v", err)
	}

	if messageID == "" {
		t.Fatal("expected Redis to return a message ID")
	}

	messages, err := client.XRange(ctx, ReadyStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE failed: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 stream message, got %d", len(messages))
	}

	message := messages[0]

	if message.Values["job_id"] != jobID {
		t.Fatalf("expected job_id %q, got %q", jobID, message.Values["job_id"])
	}

	if message.Values["payload"] != string(payload) {
		t.Fatalf(
			"expected payload %q, got %q",
			string(payload),
			message.Values["payload"],
		)
	}
}

func TestReadNext_ConsumesMessageFromConsumerGroup(t *testing.T) {
	ctx := context.Background()
	client := testRedisClient(t)

	if err := client.Del(ctx, ReadyStream).Err(); err != nil {
		t.Fatal(err)
	}

	if err := EnsureConsumerGroup(ctx, client); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}

	jobID := "test-job-456"
	payload := []byte(`{"type":"email","to":"worker@example.com"}`)

	if _, err := EnqueueDue(ctx, client, jobID, payload); err != nil {
		t.Fatalf("EnqueueDue failed: %v", err)
	}

	messages, err := ReadNext(ctx, client, "worker-1")
	if err != nil {
		t.Fatalf("ReadNext failed: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	message := messages[0]

	if message.ID == "" {
		t.Fatal("expected message ID")
	}

	if message.JobID != jobID {
		t.Fatalf("expected job ID %q, got %q", jobID, message.JobID)
	}

	if message.Payload != string(payload) {
		t.Fatalf(
			"expected payload %q, got %q",
			string(payload),
			message.Payload,
		)
	}
}
func TestAcknowledge_RemovesMessageFromPending(t *testing.T) {
	ctx := context.Background()
	client := testRedisClient(t)

	if err := client.Del(ctx, ReadyStream).Err(); err != nil {
		t.Fatal(err)
	}

	if err := EnsureConsumerGroup(ctx, client); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}

	jobID := "ack-test-" + uuid.NewString()
	payload := []byte(`{"message":"ack test"}`)

	messageID, err := EnqueueDue(ctx, client, jobID, payload)
	if err != nil {
		t.Fatalf("EnqueueDue failed: %v", err)
	}

	messages, err := ReadNext(ctx, client, "worker-ack")
	if err != nil {
		t.Fatalf("ReadNext failed: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: ReadyStream, Group: ConsumerGroup, Start: messageID, End: messageID, Count: 1}).Result()
	if err != nil {
		t.Fatalf("XPENDING failed: %v", err)
	}

	if len(pending) != 1 {
		t.Fatalf("expected message to be pending, got %d entries", len(pending))
	}

	ackCount, err := Acknowledge(ctx, client, messageID)
	if err != nil {
		t.Fatalf("Acknowledge failed: %v", err)
	}

	if ackCount != 1 {
		t.Fatalf("expected 1 acknowledged message, got %d", ackCount)
	}

	pending, err = client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: ReadyStream,
		Group:  ConsumerGroup,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING after XACK failed: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected message to no longer be pending, got %d entries", len(pending))
	}
}

func TestListStalePending_ReturnsIdleMessages(t *testing.T) {
	ctx := context.Background()

	client := testRedisClient(t)

	if err := client.Del(ctx, ReadyStream).Err(); err != nil {
		t.Fatalf("failed to clean stream: %v", err)
	}

	if err := EnsureConsumerGroup(ctx, client); err != nil {
		t.Fatalf("EnsureConsumerGroup failed: %v", err)
	}

	jobID := uuid.NewString()

	messageID, err := EnqueueDue(
		ctx,
		client,
		jobID,
		[]byte(`{"hello":"world"}`),
	)
	if err != nil {
		t.Fatalf("EnqueueDue failed: %v", err)
	}

	consumer := "worker-stale-" + uuid.NewString()

	messages, err := ReadNext(
		ctx,
		client,
		consumer,
	)
	if err != nil {
		t.Fatalf("ReadNext failed: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// Give Redis enough time for the message to become idle.
	time.Sleep(10 * time.Millisecond)

	stale, err := ListStalePending(
		ctx,
		client,
		5*time.Millisecond,
		100,
	)
	if err != nil {
		t.Fatalf("ListStalePending failed: %v", err)
	}

	if len(stale) != 1 {
		t.Fatalf("expected 1 stale message, got %d", len(stale))
	}

	if stale[0].ID != messageID {
		t.Fatalf(
			"message ID = %q, want %q",
			stale[0].ID,
			messageID,
		)
	}

	if stale[0].Consumer != consumer {
		t.Fatalf(
			"consumer = %q, want %q",
			stale[0].Consumer,
			consumer,
		)
	}

	if stale[0].Idle < 5*time.Millisecond {
		t.Fatalf(
			"idle = %v, expected at least 5ms",
			stale[0].Idle,
		)
	}

	if stale[0].DeliveryCount != 1 {
		t.Fatalf(
			"delivery count = %d, want 1",
			stale[0].DeliveryCount,
		)
	}
}

func TestScheduleJob_AddsJobToScheduledSet(t *testing.T) {
	ctx := context.Background()
	client := testRedisClient(t)

	if err := client.Del(ctx, ScheduledSet, ScheduledPayloads, ReadyStream).Err(); err != nil {
		t.Fatal(err)
	}

	jobID := uuid.NewString()
	runAt := time.Now().UTC().Add(1 * time.Hour).Truncate(time.Second)

	if err := ScheduleJob(
		ctx,
		client,
		jobID,
		[]byte(`{"message":"scheduled"}`),
		runAt,
	); err != nil {
		t.Fatalf("ScheduleJob failed: %v", err)
	}

	scheduled, err := client.ZRangeWithScores(ctx, ScheduledSet, 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to read scheduled jobs: %v", err)
	}

	if len(scheduled) != 1 {
		t.Fatalf("expected 1 scheduled job, got %d", len(scheduled))
	}

	if scheduled[0].Member != jobID {
		t.Fatalf(
			"member = %v, want %s",
			scheduled[0].Member,
			jobID,
		)
	}

	if scheduled[0].Score != float64(runAt.Unix()) {
		t.Fatalf(
			"score = %v, want %v",
			scheduled[0].Score,
			float64(runAt.Unix()),
		)
	}

	ready, err := client.XRange(ctx, ReadyStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("failed to read ready stream: %v", err)
	}

	if len(ready) != 0 {
		t.Fatalf(
			"expected future job not to be in ready stream, got %d messages",
			len(ready),
		)
	}
}
func TestPromoteDue_ConcurrentCallsDoNotDuplicate(t *testing.T) {
	ctx := context.Background()
	client := testRedisClient(t)

	if err := client.Del(
		ctx,
		ScheduledSet,
		ScheduledPayloads,
		ReadyStream,
	).Err(); err != nil {
		t.Fatalf("failed to clean Redis: %v", err)
	}

	t.Cleanup(func() {
		if err := client.Del(
			ctx,
			ScheduledSet,
			ScheduledPayloads,
			ReadyStream,
		).Err(); err != nil {
			t.Errorf("failed to clean Redis: %v", err)
		}
	})

	jobID := uuid.NewString()
	payload := []byte(`{"message":"concurrent"}`)

	runAt := time.Now().UTC().Add(-1 * time.Second)

	if err := ScheduleJob(
		ctx,
		client,
		jobID,
		payload,
		runAt,
	); err != nil {
		t.Fatalf("ScheduleJob failed: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(2)

	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			_, err := PromoteDue(
				ctx,
				client,
				time.Now().UTC(),
			)
			errs <- err
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("PromoteDue failed: %v", err)
		}
	}

	messages, err := client.XRange(
		ctx,
		ReadyStream,
		"-",
		"+",
	).Result()
	if err != nil {
		t.Fatalf("failed to inspect ready stream: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf(
			"expected exactly 1 ready message, got %d",
			len(messages),
		)
	}

	if messages[0].Values["job_id"] != jobID {
		t.Fatalf(
			"job_id = %v, want %s",
			messages[0].Values["job_id"],
			jobID,
		)
	}

	scheduledCount, err := client.ZCard(
		ctx,
		ScheduledSet,
	).Result()
	if err != nil {
		t.Fatalf("failed to inspect scheduled set: %v", err)
	}

	if scheduledCount != 0 {
		t.Fatalf(
			"expected scheduled set to be empty, got %d",
			scheduledCount,
		)
	}
}
