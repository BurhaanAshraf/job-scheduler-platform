package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
)

const (
	outboxReconcileInterval = 30 * time.Second
	outboxBatchSize         = 100
	outboxGracePeriod       = 30 * time.Second
)

// SetOutboxGracePeriod overrides how old an outbox row must be before the
// reconciler touches it. Production uses outboxGracePeriod (30 s) so the
// normal path wins the race; tests set it to zero.
func (s *Scheduler) SetOutboxGracePeriod(d time.Duration) {
	s.outboxGrace = d
}

// SetJobRepository attaches the job store the outbox reconciler needs.
// It is a setter (not a New parameter) so existing single-purpose scheduler
// tests keep compiling; ReconcileOutbox is a no-op until it is set, and
// cmd/scheduler always sets it.
func (s *Scheduler) SetJobRepository(repo *repository.JobRepository) {
	s.jobRepo = repo
}

// ReconcileOutbox re-enqueues jobs whose DB commit succeeded but whose Redis
// enqueue never happened (crash in the handoff window). Only unclaimed rows
// past the grace period are touched, so the normal path (which deletes its
// row right after enqueueing) usually wins the race. A duplicate enqueue
// from a lost race is safe: the worker's StartExecution generation guard
// turns same-generation redelivery into an ack-and-forget. Stale rows (job
// gone, terminal, or generation moved on) are deleted without enqueueing.
func (s *Scheduler) ReconcileOutbox(ctx context.Context) (int, error) {
	if s.jobRepo == nil {
		return 0, nil
	}

	entries, err := s.jobRepo.ClaimOutbox(
		ctx,
		outboxBatchSize,
		time.Now().UTC().Add(-s.outboxGrace),
	)
	if err != nil {
		return 0, err
	}

	reconciled := 0
	now := time.Now().UTC()

	for _, entry := range entries {
		job, err := s.jobRepo.GetByID(ctx, entry.JobID)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				_ = s.jobRepo.DeleteOutboxEntries(ctx, entry.JobID)
				continue
			}
			return reconciled, err
		}

		if job.Status != repository.StatusPending && job.Status != repository.StatusScheduled {
			_ = s.jobRepo.DeleteOutboxEntries(ctx, entry.JobID)
			continue
		}
		if job.QueueGeneration != entry.QueueGeneration {
			// Superseded (cancel/retry/cron respawn wrote a newer row and
			// its own outbox entry): drop the stale claim.
			_ = s.jobRepo.DeleteOutboxEntries(ctx, entry.JobID)
			continue
		}

		if entry.RunAt.After(now) {
			err = stream.ScheduleJob(ctx, s.redis, job.ID.String(), job.Payload, entry.QueueGeneration, entry.RunAt)
		} else {
			_, err = stream.EnqueueDue(ctx, s.redis, job.ID.String(), job.Payload, entry.QueueGeneration)
		}
		if err != nil {
			// Leave the row for the next tick (usually Redis is down,
			// and the error is already visible via /healthz).
			return reconciled, err
		}

		if derr := s.jobRepo.DeleteOutboxEntries(ctx, entry.JobID); derr != nil {
			s.log.Error("delete reconciled outbox entry failed", "job_id", entry.JobID, "error", derr)
		}
		s.log.Info("outbox job re-enqueued", "job_id", entry.JobID)
		reconciled++
	}

	return reconciled, nil
}
