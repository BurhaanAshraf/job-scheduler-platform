package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ReadyStream       = "jobs:ready"
	ConsumerGroup     = "workers"
	ScheduledSet      = "jobs:scheduled"
	ScheduledPayloads = "jobs:scheduled:data"
	DeadLetterStream  = "jobs:dead"
)

type Message struct {
	ID              string
	JobID           string
	Payload         string
	QueueGeneration int64
}

func EnsureConsumerGroup(ctx context.Context, client *redis.Client) error {
	// Create with ID "0" (not "$") so a recreated group redelivers
	// history already in the stream. Group loss (key eviction, FLUSHDB,
	// Redis restore/failover) must not silently drop jobs submitted
	// while the group was gone; worker-side dedupe (generation guard,
	// ack-and-forget for stale deliveries) makes redelivery safe.
	// On an empty stream (fresh MKSTREAM) "0" and "$" are identical.
	err := client.XGroupCreateMkStream(
		ctx,
		ReadyStream,
		ConsumerGroup,
		"0",
	).Err()

	if err == nil {
		return nil
	}

	if strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil
	}

	return fmt.Errorf("create consumer group %q: %w", ConsumerGroup, err)
}

func EnqueueDue(
	ctx context.Context,
	client *redis.Client,
	jobID string,
	payload []byte,
	queueGeneration int64,
) (string, error) {
	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: ReadyStream,
		MaxLen: 100000,
		Approx: true,
		ID:     "*",
		Values: map[string]any{
			"job_id":           jobID,
			"payload":          string(payload),
			"queue_generation": queueGeneration,
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("enqueue job %q: %w", jobID, err)
	}

	return id, nil
}

func DeadLetter(ctx context.Context, client *redis.Client, jobID string, payload []byte) (string, error) {
	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: DeadLetterStream,
		MaxLen: 100000,
		Approx: true,
		ID:     "*",
		Values: map[string]any{
			"job_id":  jobID,
			"payload": string(payload),
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("dead-letter job %q: %w", jobID, err)
	}

	return id, nil
}

// RemoveScheduled removes a job from the delayed queue. Best-effort:
// missing members are not errors. Used on cancel so a canceled job
// is never promoted after the DB status flips.
func RemoveScheduled(ctx context.Context, client *redis.Client, jobID string) error {
	pipe := client.TxPipeline()
	pipe.ZRem(ctx, ScheduledSet, jobID)
	pipe.HDel(ctx, ScheduledPayloads, jobID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("remove scheduled job %q: %w", jobID, err)
	}
	return nil
}

func parseQueueGeneration(raw any) int64 {
	switch v := raw.(type) {
	case string:
		if v == "" {
			return 1
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 1
		}
		if n <= 0 {
			return 1
		}
		return n
	case int64:
		if v <= 0 {
			return 1
		}
		return v
	case int:
		if v <= 0 {
			return 1
		}
		return int64(v)
	default:
		return 1
	}
}

func ReadNextWithTimeout(ctx context.Context, client *redis.Client, consumerName string, block time.Duration) ([]Message, error) {
	if block < 0 {
		block = 0
	}
	result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: consumerName,
		Streams:  []string{ReadyStream, ">"},
		Count:    10,
		Block:    block,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("read from stream: %w", err)
	}

	return toMessages(result)
}

func ReadNext(ctx context.Context, client *redis.Client, consumerName string) ([]Message, error) {
	return ReadNextWithTimeout(ctx, client, consumerName, 2*time.Second)
}

func toMessages(result []redis.XStream) ([]Message, error) {
	messages := make([]Message, 0)

	for _, stream := range result {
		for _, message := range stream.Messages {
			jobID, ok := message.Values["job_id"].(string)
			if !ok {
				return nil, fmt.Errorf(
					"stream message %q missing job_id",
					message.ID,
				)
			}

			payload, ok := message.Values["payload"].(string)
			if !ok {
				return nil, fmt.Errorf(
					"stream message %q missing payload",
					message.ID,
				)
			}

			messages = append(messages, Message{
				ID:              message.ID,
				JobID:           jobID,
				Payload:         payload,
				QueueGeneration: parseQueueGeneration(message.Values["queue_generation"]),
			})
		}
	}

	return messages, nil
}

func Acknowledge(ctx context.Context, client *redis.Client, messageID string) (int64, error) {
	count, err := client.XAck(ctx, ReadyStream, ConsumerGroup, messageID).Result()
	if err != nil {
		return 0, fmt.Errorf("acknowledge message %q: %w", messageID, err)
	}

	return count, nil
}

type PendingMessage struct {
	ID            string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int64
}

func ListStalePending(ctx context.Context, client *redis.Client, minIdle time.Duration, count int64) ([]PendingMessage, error) {
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: ReadyStream,
		Group:  ConsumerGroup,
		Start:  "-",
		End:    "+",
		Count:  count,
		Idle:   minIdle,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list stale pending messages: %w", err)
	}

	messages := make([]PendingMessage, 0, len(pending))

	for _, entry := range pending {
		messages = append(messages, PendingMessage{
			ID:            entry.ID,
			Consumer:      entry.Consumer,
			Idle:          entry.Idle,
			DeliveryCount: entry.RetryCount,
		})
	}

	return messages, nil
}

func Claim(
	ctx context.Context,
	client *redis.Client,
	consumerName string,
	minIdle time.Duration,
	messageIDs ...string,
) ([]Message, error) {
	claimed, err := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   ReadyStream,
		Group:    ConsumerGroup,
		Consumer: consumerName,
		MinIdle:  minIdle,
		Messages: messageIDs,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("claim pending messages: %w", err)
	}

	messages := make([]Message, 0, len(claimed))

	for _, message := range claimed {
		jobID, ok := message.Values["job_id"].(string)
		if !ok {
			return nil, fmt.Errorf(
				"claimed message %q missing job_id",
				message.ID,
			)
		}

		payload, ok := message.Values["payload"].(string)
		if !ok {
			return nil, fmt.Errorf(
				"claimed message %q missing payload",
				message.ID,
			)
		}

		messages = append(messages, Message{
			ID:              message.ID,
			JobID:           jobID,
			Payload:         payload,
			QueueGeneration: parseQueueGeneration(message.Values["queue_generation"]),
		})
	}

	return messages, nil
}

func ScheduleJob(
	ctx context.Context,
	client *redis.Client,
	jobID string,
	payload []byte,
	queueGeneration int64,
	runAt time.Time,
) error {
	scheduledPayload, err := json.Marshal(struct {
		Payload         string `json:"payload"`
		QueueGeneration int64  `json:"queue_generation"`
	}{
		Payload:         string(payload),
		QueueGeneration: queueGeneration,
	})
	if err != nil {
		return fmt.Errorf("marshal scheduled job %q: %w", jobID, err)
	}

	// Atomic ZADD+HSET via Lua so a crash cannot leave a ZSET member
	// without its payload (which would poison PromoteDue forever).
	if err := client.Eval(
		ctx,
		`redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1]); redis.call("HSET", KEYS[2], ARGV[1], ARGV[3]); return 1`,
		[]string{ScheduledSet, ScheduledPayloads},
		jobID,
		runAt.Unix(),
		string(scheduledPayload),
	).Err(); err != nil {
		return fmt.Errorf("schedule job %q: %w", jobID, err)
	}

	return nil
}
