package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Processor struct {
	jobRepo  *repository.JobRepository
	redis    *redis.Client
	executor *executor.HTTPExecutor
}

func NewProcessor(jobRepo *repository.JobRepository, redisClient *redis.Client, httpExecutor *executor.HTTPExecutor) *Processor {
	return &Processor{
		jobRepo:  jobRepo,
		redis:    redisClient,
		executor: httpExecutor,
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

	if err := p.executor.Execute(ctx, *job.CallbackURL, []byte(message.Payload)); err != nil {
		lastError := err.Error()

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

		return fmt.Errorf("execute job %q: %w", message.JobID, err)
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
