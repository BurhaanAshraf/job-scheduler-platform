package stream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// StreamBacklog reports how many stream entries still need work: entries
// never delivered to groupName plus entries delivered but not yet
// acknowledged. XLEN alone is wrong for this: acknowledged history stays in
// the stream (bounded by MAXLEN), so a plain length grows forever and any
// depth alarm built on it eventually false-fires permanently.
//
// If group telemetry is unavailable (e.g. the group does not exist yet),
// it falls back to the raw stream length; if that fails too, the error is
// returned so callers can decide how to degrade.
func StreamBacklog(ctx context.Context, client *redis.Client, streamName, groupName string) (int64, error) {
	groups, err := client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		return fallbackLen(ctx, client, streamName, err)
	}
	var lag int64
	found := false
	for _, g := range groups {
		if g.Name == groupName {
			found = true
			lag = g.Lag
			break
		}
	}
	if !found {
		return fallbackLen(ctx, client, streamName, nil)
	}
	pending, err := client.XPending(ctx, streamName, groupName).Result()
	if err != nil {
		return fallbackLen(ctx, client, streamName, err)
	}
	return lag + pending.Count, nil
}

// QueueBacklog is StreamBacklog for the ready queue consumed by workers.
func QueueBacklog(ctx context.Context, client *redis.Client) (int64, error) {
	return StreamBacklog(ctx, client, ReadyStream, ConsumerGroup)
}

func fallbackLen(ctx context.Context, client *redis.Client, streamName string, cause error) (int64, error) {
	n, err := client.XLen(ctx, streamName).Result()
	if err != nil {
		if cause != nil {
			return 0, cause
		}
		return 0, err
	}
	return n, nil
}
