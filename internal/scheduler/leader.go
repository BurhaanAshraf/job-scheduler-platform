package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const renewScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end

return 0
`

const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end

return 0
`

type LeaderLock struct {
	redis      *redis.Client
	key        string
	instanceID string
	ttl        time.Duration
}

func NewLeaderLock(
	redisClient *redis.Client,
	key string,
	instanceID string,
	ttl time.Duration,
) *LeaderLock {
	return &LeaderLock{
		redis:      redisClient,
		key:        key,
		instanceID: instanceID,
		ttl:        ttl,
	}
}

func (l *LeaderLock) InstanceID() string {
	return l.instanceID
}

func (l *LeaderLock) Acquire(ctx context.Context) (bool, error) {
	acquired, err := l.redis.SetNX(
		ctx,
		l.key,
		l.instanceID,
		l.ttl,
	).Result()
	if err != nil {
		return false, fmt.Errorf("acquire leader lock: %w", err)
	}

	return acquired, nil
}
func (l *LeaderLock) Renew(ctx context.Context) (bool, error) {
	result, err := l.redis.Eval(
		ctx,
		renewScript,
		[]string{l.key},
		l.instanceID,
		l.ttl.Milliseconds(),
	).Int()
	if err != nil {
		return false, fmt.Errorf("renew leader lock: %w", err)
	}

	return result == 1, nil
}
func (l *LeaderLock) Heartbeat(ctx context.Context) error {
	interval := l.ttl / 2
	if interval <= 0 {
		return fmt.Errorf("leader lock TTL must be greater than zero")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			renewed, err := l.Renew(ctx)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return context.Canceled
				}
				return err
			}

			if !renewed {
				return fmt.Errorf("leader lock is no longer owned")
			}
		}
	}
}

func (l *LeaderLock) IsLeader(ctx context.Context) (bool, error) {
	owner, err := l.redis.Get(ctx, l.key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}

		return false, fmt.Errorf("check leader lock: %w", err)
	}

	return owner == l.instanceID, nil
}
func (l *LeaderLock) Release(ctx context.Context) (bool, error) {
	result, err := l.redis.Eval(
		ctx,
		releaseScript,
		[]string{l.key},
		l.instanceID,
	).Int()
	if err != nil {
		return false, fmt.Errorf("release leader lock: %w", err)
	}

	return result == 1, nil
}
