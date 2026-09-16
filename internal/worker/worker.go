package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/redis/go-redis/v9"
)

const pollInterval = 100 * time.Millisecond

type Worker struct {
	redis     *redis.Client
	processor *Processor
	consumer  string
	logger    *slog.Logger
}

func NewWorker(redisClient *redis.Client, processor *Processor, consumer string, logger *slog.Logger) *Worker {
	return &Worker{
		redis:     redisClient,
		processor: processor,
		consumer:  consumer,
		logger:    logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		messages, err := stream.ReadNext(ctx, w.redis, w.consumer)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			w.logger.Error("failed to read job", "error", err)
		} else {
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

		if len(messages) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
}
