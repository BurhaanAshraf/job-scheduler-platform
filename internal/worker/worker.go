package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/redis/go-redis/v9"
)

const (
	pollInterval      = 100 * time.Millisecond
	reclaimInterval   = 30 * time.Second
	stalePendingAfter = 60 * time.Second
	reclaimBatch      = 10
)

type Worker struct {
	redis     *redis.Client
	processor *Processor
	consumer  string
	logger    *slog.Logger
}

func NewWorker(redisClient *redis.Client, processor *Processor, consumer string, logger *slog.Logger) *Worker {
	if consumer == "" {
		consumer = "worker-unknown"
	}
	return &Worker{
		redis:     redisClient,
		processor: processor,
		consumer:  consumer,
		logger:    logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if err := stream.EnsureConsumerGroup(ctx, w.redis); err != nil {
		return err
	}

	reclaimTicker := time.NewTicker(reclaimInterval)
	defer reclaimTicker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Periodically reclaim stale pending messages left by crashed
		// workers (crash between PG update and XACK). Without this,
		// those messages sit in the PEL forever.
		select {
		case <-reclaimTicker.C:
			w.reclaimStale(ctx)
		default:
		}

		messages, err := stream.ReadNext(ctx, w.redis, w.consumer)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			w.logger.Error("failed to read job", "error", err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		for _, message := range messages {
			if err := w.processor.Process(ctx, message); err != nil {
				w.logger.Error(
					"job processing failed",
					"message_id", message.ID,
					"job_id", message.JobID,
					"error", err,
				)

				continue
			}

			w.logger.Info(
				"job processed successfully",
				"message_id", message.ID,
				"job_id", message.JobID,
			)
		}
	}
}

func (w *Worker) reclaimStale(ctx context.Context) {
	pending, err := stream.ListStalePending(ctx, w.redis, stalePendingAfter, reclaimBatch)
	if err != nil {
		w.logger.Error("failed to list stale pending messages", "error", err)
		return
	}
	if len(pending) == 0 {
		return
	}

	ids := make([]string, 0, len(pending))
	for _, p := range pending {
		ids = append(ids, p.ID)
	}

	claimed, err := stream.Claim(ctx, w.redis, w.consumer, stalePendingAfter, ids...)
	if err != nil {
		w.logger.Error("failed to claim stale pending messages", "error", err)
		return
	}

	for _, message := range claimed {
		if err := w.processor.Process(ctx, message); err != nil {
			w.logger.Error(
				"reclaimed job processing failed",
				"message_id", message.ID,
				"job_id", message.JobID,
				"error", err,
			)
			continue
		}
		w.logger.Info(
			"reclaimed job processed successfully",
			"message_id", message.ID,
			"job_id", message.JobID,
		)
	}
}
