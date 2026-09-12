package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	client *redis.Client
	limit  int64
	window time.Duration
}

func New(client *redis.Client, limit int64, window time.Duration) *Limiter {
	return &Limiter{
		client: client,
		limit:  limit,
		window: window,
	}
}

var rateLimitScript = redis.NewScript(`
local count = redis.call("INCR", KEYS[1])

if count == 1 then
	redis.call("EXPIRE", KEYS[1], ARGV[1])
end

return count
`)

func (l *Limiter) Allow(ctx context.Context, clientID string) (bool, error) {
	windowSeconds := int64(l.window.Seconds())
	windowNumber := time.Now().Unix() / windowSeconds

	key := fmt.Sprintf("rate_limit:%s:%d", clientID, windowNumber)

	count, err := rateLimitScript.Run(
		ctx,
		l.client,
		[]string{key},
		windowSeconds,
	).Int64()
	if err != nil {
		return false, err
	}

	return count <= l.limit, nil
}
