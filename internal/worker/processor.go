package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/retry"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Processor struct {
	jobRepo       *repository.JobRepository
	executionRepo *repository.JobExecutionRepository
	redis         *redis.Client
	executor      *executor.HTTPExecutor
}

func NewProcessor(jobRepo *repository.JobRepository, executionRepo *repository.JobExecutionRepository, redisClient *redis.Client, httpExecutor *executor.HTTPExecutor) *Processor {
	return &Processor{
		jobRepo:       jobRepo,
		executionRepo: executionRepo,
		redis:         redisClient,
		executor:      httpExecutor,
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
		ackCount, err := stream.Acknowledge(ctx, p.redis, message.ID)
		if err != nil {
			return fmt.Errorf(
				"acknowledge stale message for job %q: %w",
				message.JobID,
				err,
			)
		}

		if ackCount != 1 {
			return fmt.Errorf(
				"expected to acknowledge 1 stale message %q, acknowledged %d",
				message.ID,
				ackCount,
			)
		}

		return nil
	}

	executionID, err := p.executionRepo.Create(ctx, repository.CreateJobExecutionInput{
		JobID:         job.ID,
		AttemptNumber: attempt,
		StartedAt:     time.Now().UTC(),
		Status:        "running",
	})
	if err != nil {
		return fmt.Errorf("create execution for job %q: %w", message.JobID, err)
	}

	responseCode, executeErr := p.executor.Execute(
		ctx,
		*job.CallbackURL,
		[]byte(message.Payload),
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
			nextRunAt := time.Now().UTC().Add(retry.Backoff(attempt))

			if err := stream.ScheduleJob(
				ctx,
				p.redis,
				job.ID.String(),
				[]byte(message.Payload),
				job.QueueGeneration,
				nextRunAt,
			); err != nil {
				return fmt.Errorf(
					"schedule retry for job %q: %w",
					message.JobID,
					err,
				)
			}

			if updateErr := p.jobRepo.UpdateStatus(
				ctx,
				job.ID,
				job.Status,
				&lastError,
			); updateErr != nil {
				return fmt.Errorf(
					"record failure for job %q: %w",
					message.JobID,
					updateErr,
				)
			}

			ackCount, err := stream.Acknowledge(ctx, p.redis, message.ID)
			if err != nil {
				return fmt.Errorf(
					"acknowledge failed job %q after scheduling retry: %w",
					message.JobID,
					err,
				)
			}

			if ackCount != 1 {
				return fmt.Errorf(
					"expected to acknowledge 1 message %q, acknowledged %d",
					message.ID,
					ackCount,
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

		ackCount, err := stream.Acknowledge(ctx, p.redis, message.ID)
		if err != nil {
			return fmt.Errorf(
				"acknowledge dead-lettered job %q: %w",
				message.JobID,
				err,
			)
		}

		if ackCount != 1 {
			return fmt.Errorf(
				"expected to acknowledge 1 message %q, acknowledged %d",
				message.ID,
				ackCount,
			)
		}

		return fmt.Errorf(
			"execute job %q after max attempts: %w",
			message.JobID,
			executeErr,
		)
	}

	if err := p.jobRepo.UpdateStatus(ctx, job.ID, repository.StatusDone, nil); err != nil {
		return fmt.Errorf("mark job %q as done: %w", message.JobID, err)
	}

	ackCount, err := stream.Acknowledge(ctx, p.redis, message.ID)
	if err != nil {
		return fmt.Errorf("acknowledge job %q: %w", message.JobID, err)
	}

	if ackCount != 1 {
		return fmt.Errorf(
			"expected to acknowledge 1 message %q, acknowledged %d",
			message.ID,
			ackCount,
		)
	}

	return nil
}
