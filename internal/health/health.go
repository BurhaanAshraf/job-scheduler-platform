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
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := c.db.Ping(ctx); err != nil {
		return fmt.Errorf("database health check: %w", err)
	}

	if err := c.redis.Ping(ctx); err != nil {
		return fmt.Errorf("redis health check: %w", err)
	}

	return nil
}
