package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/redis/go-redis/v9"
)

type Scheduler struct {
	redis        *redis.Client
	pollInterval time.Duration
}

// A shorter interval reduces dispatch latency but increases Redis polling;
// a longer interval reduces polling overhead but increases scheduling latency.

func New(redisClient *redis.Client, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		redis:        redisClient,
		pollInterval: pollInterval,
	}
}

func (s *Scheduler) PromoteDue(
	ctx context.Context,
	now time.Time,
) (int64, error) {
	count, err := stream.PromoteDue(ctx, s.redis, now)
	if err != nil {
		return 0, fmt.Errorf("promote due jobs: %w", err)
	}

	return count, nil
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if _, err := s.PromoteDue(ctx, time.Now().UTC()); err != nil {
				return fmt.Errorf("promote due jobs: %w", err)
			}
		}
	}
}
