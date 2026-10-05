package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/metrics"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/retry"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Processor struct {
	jobRepo       *repository.JobRepository
	executionRepo *repository.JobExecutionRepository
	redis         *redis.Client
	executor      *executor.HTTPExecutor
	validatorCfg  validator.Config
}

func NewProcessor(
	jobRepo *repository.JobRepository,
	executionRepo *repository.JobExecutionRepository,
	redisClient *redis.Client,
	httpExecutor *executor.HTTPExecutor,
	validatorCfg validator.Config,
) *Processor {
	return &Processor{
		jobRepo:       jobRepo,
		executionRepo: executionRepo,
		redis:         redisClient,
		executor:      httpExecutor,
		validatorCfg:  validatorCfg,
	}
}

func (p *Processor) Process(ctx context.Context, message stream.Message) error {
	jobID, err := uuid.Parse(message.JobID)
	if err != nil {
		return fmt.Errorf("parse job ID %q: %w", message.JobID, err)
	}

	job, err := p.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job %q: %w", message.JobID, err)
	}

	if job.CallbackURL == nil || *job.CallbackURL == "" {
		return errors.New("job has no callback URL")
	}

	attempt, err := p.jobRepo.StartExecution(
		ctx,
		job.ID,
		message.QueueGeneration,
	)
	if err != nil {
		return fmt.Errorf("start execution for job %s: %w", job.ID, err)
	}

	if attempt == 0 {
		// Stale or duplicate delivery (generation mismatch, already
		// running/done, or canceled). PG state is authoritative, but a
		// redelivery is only safe to ack-and-forget when the outcome is
		// already durable. A running job with NO completed execution for
		// this generation means the callback never finished (crash between
		// StartExecution and Complete, or an execution-Create failure):
		// marking it done would be a lie, so re-drive it instead. A
		// generation mismatch means a newer run already owns the job, so
		// that message is genuinely stale and only needs an ack.
		if job.Status == repository.StatusRunning && message.QueueGeneration == job.QueueGeneration {
			completed, err := p.executionRepo.HasCompletedExecution(ctx, job.ID, job.QueueGeneration)
			if err != nil {
				return fmt.Errorf("check completed executions for job %q: %w", message.JobID, err)
			}
			if completed {
				// XACK-lost-after-success: the callback did run and its
				// execution row is done; the redelivery only needs the
				// status flip that the crash skipped.
				if err := p.jobRepo.UpdateStatus(ctx, job.ID, repository.StatusDone, nil); err != nil {
					return fmt.Errorf("finalize stale job %q as done: %w", message.JobID, err)
				}
				if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
					return fmt.Errorf("acknowledge stale message for job %q: %w", message.JobID, err)
				}
				return nil
			}
			if job.Attempts >= job.MaxAttempts {
				// Crash-loop guard: every re-drive consumes an attempt via
				// StartExecution, so attempts only grows. Cap it here the
				// same way the normal failure path does.
				lastError := "worker crashed before execution completed"
				if _, err := stream.DeadLetter(ctx, p.redis, job.ID.String(), []byte(message.Payload)); err != nil {
					return fmt.Errorf("dead-letter stale job %q: %w", message.JobID, err)
				}
				if err := p.jobRepo.UpdateStatus(ctx, job.ID, repository.StatusDead, &lastError); err != nil {
					return fmt.Errorf("mark stale job %q as dead: %w", message.JobID, err)
				}
				if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
					return fmt.Errorf("acknowledge dead-lettered stale job %q: %w", message.JobID, err)
				}
				return nil
			}
			// Re-drive: flip back to scheduled (same generation, so the
			// redelivered message passes the generation guard) and
			// re-enqueue immediately. The next delivery runs StartExecution
			// (attempts+1) and executes the callback: at-least-once.
			if err := p.jobRepo.UpdateStatus(ctx, job.ID, repository.StatusScheduled, nil); err != nil {
				return fmt.Errorf("reschedule stale job %q: %w", message.JobID, err)
			}
			if _, err := stream.EnqueueDue(ctx, p.redis, job.ID.String(), []byte(message.Payload), job.QueueGeneration); err != nil {
				return fmt.Errorf("re-enqueue stale job %q: %w", message.JobID, err)
			}
			if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
				return fmt.Errorf("acknowledge re-driven stale job %q: %w", message.JobID, err)
			}
			return nil
		}
		if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
			return fmt.Errorf("acknowledge stale message for job %q: %w", message.JobID, err)
		}

		return nil
	}

	executionID, err := p.executionRepo.Create(
		ctx,
		repository.CreateJobExecutionInput{
			JobID:           job.ID,
			AttemptNumber:   attempt,
			StartedAt:       time.Now().UTC(),
			Status:          "running",
			QueueGeneration: message.QueueGeneration,
		},
	)
	if err != nil {
		return fmt.Errorf("create execution for job %q: %w", message.JobID, err)
	}

	responseCode, executeErr := p.executor.ExecuteJob(
		ctx,
		*job.CallbackURL,
		[]byte(message.Payload),
		job.ID.String(),
		attempt,
		p.validatorCfg,
	)

	finishedAt := time.Now().UTC()

	if executeErr != nil {
		executionError := executeErr.Error()

		var responseCodePtr *int
		if responseCode != 0 {
			responseCodePtr = &responseCode
		}

		if err := p.executionRepo.Complete(
			ctx,
			executionID,
			finishedAt,
			"failed",
			&executionError,
			responseCodePtr,
		); err != nil {
			return fmt.Errorf(
				"complete failed execution for job %q: %w",
				message.JobID,
				err,
			)
		}

		// Metrics are observability only. A metrics failure must not
		// change the outcome of the job execution.
		_ = metrics.Increment(
			ctx,
			p.redis,
			metrics.JobsFailedKey,
		)
	} else {
		if err := p.executionRepo.Complete(
			ctx,
			executionID,
			finishedAt,
			"done",
			nil,
			&responseCode,
		); err != nil {
			return fmt.Errorf(
				"complete successful execution for job %q: %w",
				message.JobID,
				err,
			)
		}
	}

	if executeErr != nil {
		lastError := executeErr.Error()

		if attempt < job.MaxAttempts {
			nextRunAt := retry.NextRunAt(time.Now().UTC(), attempt)
			newGeneration := job.QueueGeneration + 1

			// Enqueue first with the next generation; DB still holds the
			// old generation so a crash here leaves the message pending
			// and redelivery can retry safely.
			if err := stream.ScheduleJob(
				ctx,
				p.redis,
				job.ID.String(),
				[]byte(message.Payload),
				newGeneration,
				nextRunAt,
			); err != nil {
				return fmt.Errorf(
					"schedule retry for job %q: %w",
					message.JobID,
					err,
				)
			}

			if _, schedErr := p.jobRepo.ScheduleRetry(ctx, job.ID, nextRunAt, &lastError); schedErr != nil {
				// Compensate: DB did not advance, remove the Redis entry
				// we just created so it can never be promoted stale.
				_ = stream.RemoveScheduled(ctx, p.redis, job.ID.String())
				return fmt.Errorf("schedule retry for job %q: %w", message.JobID, schedErr)
			}

			if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
				return fmt.Errorf(
					"acknowledge failed job %q after scheduling retry: %w",
					message.JobID,
					err,
				)
			}

			return fmt.Errorf(
				"execute job %q: %w",
				message.JobID,
				executeErr,
			)
		}

		if _, err := stream.DeadLetter(
			ctx,
			p.redis,
			job.ID.String(),
			[]byte(message.Payload),
		); err != nil {
			return fmt.Errorf(
				"dead-letter job %q: %w",
				message.JobID,
				err,
			)
		}

		if updateErr := p.jobRepo.UpdateStatus(
			ctx,
			job.ID,
			repository.StatusDead,
			&lastError,
		); updateErr != nil {
			return fmt.Errorf(
				"mark job %q as dead: %w",
				message.JobID,
				updateErr,
			)
		}

		if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
			return fmt.Errorf(
				"acknowledge dead-lettered job %q: %w",
				message.JobID,
				err,
			)
		}

		return fmt.Errorf(
			"execute job %q after max attempts: %w",
			message.JobID,
			executeErr,
		)
	}

	if err := p.jobRepo.UpdateStatus(
		ctx,
		job.ID,
		repository.StatusDone,
		nil,
	); err != nil {
		return fmt.Errorf(
			"mark job %q as done: %w",
			message.JobID,
			err,
		)
	}

	// Only count the job as completed after its durable DB status
	// has successfully been changed to done.
	_ = metrics.Increment(
		ctx,
		p.redis,
		metrics.JobsCompletedKey,
	)

	if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
		return fmt.Errorf(
			"acknowledge job %q: %w",
			message.JobID,
			err,
		)
	}

	return nil
}

func acknowledgeOnce(ctx context.Context, client *redis.Client, messageID string) error {
	count, err := stream.Acknowledge(ctx, client, messageID)
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("expected to acknowledge 1 message %q, acknowledged %d", messageID, count)
	}
	return nil
}

// Quarantine dead-letters a stream message that has been delivered
// (XREADGROUP/XCLAIM) at least maxPoisonDeliveries times without ever
// reaching a terminal state. Such messages are poison: unparseable IDs,
// deleted jobs, or rows that fail every guard in Process. Without this,
// the reclaim loop redrives them forever (one XCLAIM per tick, never
// progressing, never alerting). Terminal or missing jobs are simply
// acknowledged; anything else goes to the DLQ with an explanatory
// last_error so the dead-letters view shows why.
func (p *Processor) Quarantine(ctx context.Context, message stream.Message, deliveries int64) error {
	jobID, err := uuid.Parse(message.JobID)
	if err != nil {
		// Not even a job reference: nothing to dead-letter, just drop it.
		if ackErr := acknowledgeOnce(ctx, p.redis, message.ID); ackErr != nil {
			return fmt.Errorf("acknowledge unparseable message %q: %w", message.ID, ackErr)
		}
		return nil
	}

	job, err := p.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			if ackErr := acknowledgeOnce(ctx, p.redis, message.ID); ackErr != nil {
				return fmt.Errorf("acknowledge message for missing job %q: %w", message.JobID, ackErr)
			}
			return nil
		}
		return fmt.Errorf("get job %q for quarantine: %w", message.JobID, err)
	}

	switch job.Status {
	case repository.StatusDone, repository.StatusCancelled, repository.StatusDead:
		if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
			return fmt.Errorf("acknowledge terminal job %q: %w", message.JobID, err)
		}
		return nil
	}

	reason := fmt.Sprintf(
		"poison message: %d deliveries without progress; quarantined to dead-letter queue",
		deliveries,
	)
	if _, err := stream.DeadLetter(ctx, p.redis, job.ID.String(), []byte(message.Payload)); err != nil {
		return fmt.Errorf("dead-letter poison job %q: %w", message.JobID, err)
	}
	if err := p.jobRepo.UpdateStatus(ctx, job.ID, repository.StatusDead, &reason); err != nil {
		return fmt.Errorf("mark poison job %q as dead: %w", message.JobID, err)
	}
	_ = metrics.Increment(ctx, p.redis, metrics.JobsFailedKey)
	if err := acknowledgeOnce(ctx, p.redis, message.ID); err != nil {
		return fmt.Errorf("acknowledge quarantined job %q: %w", message.JobID, err)
	}
	return nil
}
