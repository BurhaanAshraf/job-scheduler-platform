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
	jobRepo      *repository.JobRepository
	pollInterval time.Duration
	leaderLock   *LeaderLock
	log          *slog.Logger
	outboxGrace  time.Duration
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
		outboxGrace:  outboxGracePeriod,
	}
}

func (s *Scheduler) PromoteDue(
	ctx context.Context,
	now time.Time,
) (int64, error) {
	ids, err := stream.PromoteDueWithIDs(ctx, s.redis, now)
	if err != nil {
		return 0, fmt.Errorf("promote due jobs: %w", err)
	}
	// 10.3 correlation: one log line per job so `grep job_id` shows the
	// scheduler leg of the lifecycle (API submit and worker legs already
	// carry job_id).
	for _, id := range ids {
		s.log.Info("job promoted", "job_id", id)
	}

	return int64(len(ids)), nil
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

	// 14.7 observability: report ready-queue depth once a minute so a
	// CloudWatch Logs metric filter can alarm on sustained backlog.
	// Tick-level logging would drown the log group (500ms polls).
	var lastDepthLog time.Time
	var lastReconcile time.Time

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
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return ctx.Err()
				}
				s.log.Error("check scheduler leadership failed, retrying next tick", "error", err)
				continue
			}

			if !isLeader {
				acquired, err := s.leaderLock.Acquire(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || ctx.Err() != nil {
						return ctx.Err()
					}
					s.log.Error("acquire scheduler leadership failed, retrying next tick", "error", err)
					continue
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

			promoted, err := s.PromoteDue(ctx, now)
			if err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return ctx.Err()
				}
				s.log.Error("promote due jobs failed, retrying next tick", "error", err)
				continue
			}

			if promoted > 0 {
				s.log.Info("jobs promoted", "count", promoted)
			}

			if _, err := s.TickCronJobs(ctx, now); err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return ctx.Err()
				}
				s.log.Error("tick cron jobs failed, retrying next tick", "error", err)
				continue
			}

			if time.Since(lastReconcile) >= outboxReconcileInterval {
				if n, err := s.ReconcileOutbox(ctx); err != nil {
					if errors.Is(err, context.Canceled) || ctx.Err() != nil {
						return ctx.Err()
					}
					s.log.Error("reconcile outbox failed, retrying next cycle", "error", err)
				} else if n > 0 {
					s.log.Info("outbox reconciled", "count", n)
				}
				lastReconcile = time.Now().UTC()
			}

			if time.Since(lastDepthLog) >= time.Minute {
				if n, err := stream.QueueBacklog(ctx, s.redis); err != nil {
					s.log.Debug("queue depth read failed", "error", err)
				} else {
					s.log.Info("queue depth", "depth", n)
					lastDepthLog = time.Now().UTC()
				}
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
			// Don't abort the whole tick: one bad cron template must not
			// starve all other crons or kill the scheduler.
			s.log.Error("create cron instance failed, skipping", "cron_job_id", cronJob.ID, "error", err)
			continue
		}

		if !ok {
			continue
		}
		s.log.Info(
			"cron job promoted",
			"job_id", instance.ID,
			"cron_job_id", cronJob.ID,
		)

		_, err = stream.EnqueueDue(
			ctx,
			s.redis,
			instance.ID.String(),
			instance.Payload,
			instance.QueueGeneration,
		)
		if err != nil {
			// The outbox row written by CreateDueInstance lets the next
			// reconcile cycle finish this handoff. Continue ticking other
			// crons instead of crashing.
			s.log.Error(
				"enqueue cron instance failed; outbox reconciler will retry",
				"job_id", instance.ID,
				"cron_job_id", cronJob.ID,
				"error", err,
			)
			continue
		}

		// Best-effort: the reconciler reaps leftovers. Guarded for
		// tests that construct a scheduler without a job repository.
		if s.jobRepo != nil {
			if derr := s.jobRepo.DeleteOutboxEntries(ctx, instance.ID); derr != nil {
				s.log.Error("delete cron outbox entry failed", "job_id", instance.ID, "error", derr)
			}
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
