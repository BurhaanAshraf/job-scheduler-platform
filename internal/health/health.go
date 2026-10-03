package health

import (
	"context"
	"fmt"
	"time"
)

type Pinger interface {
	Ping(context.Context) error
}

type Checker struct {
	db      Pinger
	redis   Pinger
	timeout time.Duration
}

func NewChecker(
	db Pinger,
	redis Pinger,
	timeout time.Duration,
) *Checker {
	return &Checker{
		db:      db,
		redis:   redis,
		timeout: timeout,
	}
}

func (c *Checker) Check(ctx context.Context) error {
	dbCtx, dbCancel := context.WithTimeout(ctx, c.timeout)
	defer dbCancel()

	if err := c.db.Ping(dbCtx); err != nil {
		return fmt.Errorf("database health check: %w", err)
	}

	redisCtx, redisCancel := context.WithTimeout(ctx, c.timeout)
	defer redisCancel()

	if err := c.redis.Ping(redisCtx); err != nil {
		return fmt.Errorf("redis health check: %w", err)
	}

	return nil
}
