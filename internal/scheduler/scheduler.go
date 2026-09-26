package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/redis/go-redis/v9"
)

type Scheduler struct {
	redis        *redis.Client
	cronRepo     *repository.CronJobRepository
	pollInterval time.Duration
	leaderLock   *LeaderLock
	log          *slog.Logger
}

// A shorter interval reduces dispatch latency but increases Redis polling;
// a longer interval reduces polling overhead but increases scheduling latency.

func New(
	redisClient *redis.Client,
	cronRepo *repository.CronJobRepository,
	pollInterval time.Duration,
	leaderLock *LeaderLock,
	log *slog.Logger,

) *Scheduler {
	return &Scheduler{
		redis:        redisClient,
		cronRepo:     cronRepo,
		pollInterval: pollInterval,
		leaderLock:   leaderLock,
		log:          log,
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
	defer s.releaseLeadership()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	var heartbeatCancel context.CancelFunc
	var heartbeatDone <-chan error

	startHeartbeat := func() {
		if heartbeatDone != nil {
			return
		}

		heartbeatCtx, cancel := context.WithCancel(ctx)
		heartbeatCancel = cancel

		done := make(chan error, 1)
		heartbeatDone = done

		go func() {
			done <- s.leaderLock.Heartbeat(heartbeatCtx)
		}()
	}

	stopHeartbeat := func() {
		if heartbeatCancel == nil {
			return
		}

		heartbeatCancel()
		heartbeatCancel = nil
		heartbeatDone = nil
	}

	defer stopHeartbeat()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if heartbeatDone != nil {
				select {
				case err := <-heartbeatDone:
					heartbeatCancel = nil
					heartbeatDone = nil

					if err != nil && !errors.Is(err, context.Canceled) {
						s.log.Warn(
							"lost scheduler leadership",
							"instance_id", s.leaderLock.InstanceID(),
							"error", err,
						)
					}
				default:
				}
			}

			isLeader, err := s.leaderLock.IsLeader(ctx)
			if err != nil {
				return fmt.Errorf("check scheduler leadership: %w", err)
			}

			if !isLeader {
				acquired, err := s.leaderLock.Acquire(ctx)
				if err != nil {
					return fmt.Errorf("acquire scheduler leadership: %w", err)
				}

				if !acquired {
					continue
				}

				s.log.Info(
					"acquired scheduler leadership",
					"instance_id", s.leaderLock.InstanceID(),
				)
			}

			startHeartbeat()

			now := time.Now().UTC()

			if _, err := s.PromoteDue(ctx, now); err != nil {
				return fmt.Errorf("promote due jobs: %w", err)
			}

			if _, err := s.TickCronJobs(ctx, now); err != nil {
				return fmt.Errorf("tick cron jobs: %w", err)
			}
		}
	}
}

func (s *Scheduler) TickCronJobs(ctx context.Context, now time.Time,
) (int, error) {
	cronJobs, err := s.cronRepo.ListDue(ctx, now)
	if err != nil {
		return 0, fmt.Errorf("list due cron jobs: %w", err)
	}

	created := 0

	for _, cronJob := range cronJobs {
		instance, ok, err := s.cronRepo.CreateDueInstance(
			ctx,
			cronJob.ID,
			now,
			NextRunAt,
		)
		if err != nil {
			return created, fmt.Errorf(
				"create cron instance for %d: %w",
				cronJob.ID,
				err,
			)
		}

		if !ok {
			continue
		}

		_, err = stream.EnqueueDue(
			ctx,
			s.redis,
			instance.ID.String(),
			instance.Payload,
			instance.QueueGeneration,
		)
		if err != nil {
			return created, fmt.Errorf(
				"enqueue cron instance %s: %w",
				instance.ID,
				err,
			)
		}

		created++
	}

	return created, nil
}

func (s *Scheduler) releaseLeadership() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	released, err := s.leaderLock.Release(ctx)
	if err != nil {
		s.log.Error(
			"failed to release scheduler leadership",
			"instance_id", s.leaderLock.InstanceID(),
			"error", err,
		)
		return
	}

	if released {
		s.log.Info(
			"released scheduler leadership",
			"instance_id", s.leaderLock.InstanceID(),
		)
	}
}
