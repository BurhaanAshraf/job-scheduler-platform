package ratelimit

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	client *redis.Client
	limit  int64
	window time.Duration
}

func New(client *redis.Client, limit int64, window time.Duration) *Limiter {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	return &Limiter{
		client: client,
		limit:  limit,
		window: window,
	}
}

// Sliding-window log: each request is a member in a sorted set scored by
// arrival time (ms). The Lua script evicts entries older than the window,
// then admits the request only if fewer than limit entries remain. This is a
// true sliding window (no fixed-window edge burst), atomic via a single EVAL.
var rateLimitScript = redis.NewScript(`
local count = redis.call("ZCARD", KEYS[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local now = tonumber(ARGV[1])
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, now - window)
count = redis.call("ZCARD", KEYS[1])
if count < limit then
	redis.call("ZADD", KEYS[1], now, ARGV[4])
	redis.call("PEXPIRE", KEYS[1], window)
	local ttl = redis.call("PTTL", KEYS[1])
	return {1, ttl}
else
	redis.call("PEXPIRE", KEYS[1], window)
	local ttl = redis.call("PTTL", KEYS[1])
	return {0, ttl}
end
`)

func (l *Limiter) Allow(ctx context.Context, clientID string) (bool, time.Duration, error) {
	windowMillis := l.window.Milliseconds()
	if windowMillis <= 0 {
		windowMillis = 60000
	}
	nowMillis := time.Now().UnixMilli()
	member := fmt.Sprintf("%d-%d", nowMillis, rand.Int63())

	key := fmt.Sprintf("rate_limit:%s", clientID)

	res, err := rateLimitScript.Run(
		ctx,
		l.client,
		[]string{key},
		nowMillis,
		windowMillis,
		l.limit,
		member,
	).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("unexpected rate limit script result: %v", res)
	}
	allowedInt, ok := res[0].(int64)
	if !ok {
		return false, 0, fmt.Errorf("unexpected allowed type %T", res[0])
	}
	ttlMillis, ok := res[1].(int64)
	if !ok {
		return false, 0, fmt.Errorf("unexpected ttl type %T", res[1])
	}
	ttl := time.Duration(ttlMillis) * time.Millisecond
	if ttl <= 0 {
		ttl = l.window
	}

	return allowedInt == 1, ttl, nil
}
