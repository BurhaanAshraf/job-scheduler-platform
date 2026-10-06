package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/redis/go-redis/v9"
)

const (
	pollInterval      = 100 * time.Millisecond
	reclaimInterval   = 30 * time.Second
	stalePendingAfter = 60 * time.Second
	reclaimBatch      = 10
	// readBlock bounds every blocking stream read. It must stay short:
	// a canceled context does NOT abort an in-flight XREADGROUP (the
	// read runs to the socket deadline, ~block+10s), so stop latency
	// equals this block plus one iteration. 500ms keeps stops prompt
	// while idle polling stays at a negligible 2 QPS per worker.
	readBlock = 500 * time.Millisecond
	// maxPoisonDeliveries bounds redelivery of a message that never
	// progresses: at this many XREADGROUP/XCLAIM deliveries the message
	// is quarantined to the DLQ instead of reclaimed again. Must stay
	// well below any MAXLEN trim horizon so poison never silently drops.
	maxPoisonDeliveries = 5
)

type Worker struct {
	redis      *redis.Client
	processor  *Processor
	consumer   string
	logger     *slog.Logger
	staleAfter time.Duration
}

func NewWorker(redisClient *redis.Client, processor *Processor, consumer string, logger *slog.Logger) *Worker {
	if consumer == "" {
		consumer = "worker-unknown"
	}
	return &Worker{
		redis:      redisClient,
		processor:  processor,
		consumer:   consumer,
		logger:     logger,
		staleAfter: stalePendingAfter,
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

		// The blocking read runs in a child goroutine with the result
		// selected against ctx: a canceled context does NOT abort an
		// in-flight XREADGROUP (it runs to the socket deadline, ~10s
		// past the block), so selecting here is what makes stop prompt. The orphaned read always terminates by its deadline
		// (sooner on client close) and its buffered send never blocks.
		type readResult struct {
			messages []stream.Message
			err      error
		}
		readCh := make(chan readResult, 1)
		go func() {
			msgs, rerr := stream.ReadNextWithTimeout(ctx, w.redis, w.consumer, readBlock)
			readCh <- readResult{messages: msgs, err: rerr}
		}()

		var messages []stream.Message
		var readErr error
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-readCh:
			messages = res.messages
			readErr = res.err
		}

		if readErr != nil {
			err := readErr
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.Canceled) {
				return err
			}

			if isNoGroupError(err) {
				// The consumer group vanished after startup (key
				// eviction, FLUSHDB, Redis restore/failover). Recreate
				// it and keep going: without this the worker
				// error-loops forever and no job is ever processed
				// again until restart.
				w.logger.Error("consumer group missing, recreating", "error", err)
				if gerr := stream.EnsureConsumerGroup(ctx, w.redis); gerr != nil {
					w.logger.Error("failed to recreate consumer group", "error", gerr)
				}
			} else {
				w.logger.Error("failed to read job", "error", err)
			}

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
	pending, err := stream.ListStalePending(ctx, w.redis, w.staleAfter, reclaimBatch)
	if err != nil {
		w.logger.Error("failed to list stale pending messages", "error", err)
		return
	}
	if len(pending) == 0 {
		return
	}

	// Poison partition: messages delivered maxPoisonDeliveries times without
	// progress are quarantined to the DLQ; the rest are claimed for redrive.
	// Without this split, one poison pill spins through reclaim forever.
	var poison, normal []stream.PendingMessage
	for _, p := range pending {
		if p.DeliveryCount >= maxPoisonDeliveries {
			poison = append(poison, p)
		} else {
			normal = append(normal, p)
		}
	}

	w.quarantinePoison(ctx, poison)

	if len(normal) == 0 {
		return
	}

	ids := make([]string, 0, len(normal))
	for _, p := range normal {
		ids = append(ids, p.ID)
	}

	claimed, err := stream.Claim(ctx, w.redis, w.consumer, w.staleAfter, ids...)
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

// isNoGroupError reports whether err is Redis's NOGROUP reply: the stream
// exists (or not) but our consumer group is gone, so XREADGROUP can never
// succeed until the group is recreated.
func isNoGroupError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

// quarantinePoison resolves each over-delivered PEL entry to its stream
// message and hands it to Processor.Quarantine. Entries whose stream data
// is gone (trimmed past MAXLEN) are acknowledged to clear the orphaned PEL
// slot: their payloads are unrecoverable, and holding the slot helps no one.
func (w *Worker) quarantinePoison(ctx context.Context, poison []stream.PendingMessage) {
	if len(poison) == 0 {
		return
	}

	ids := make([]string, 0, len(poison))
	for _, p := range poison {
		ids = append(ids, p.ID)
	}

	messages, err := stream.ReadMessagesByIDs(ctx, w.redis, ids...)
	if err != nil {
		// Resolve individually so one trimmed entry doesn't block the rest.
		for _, p := range poison {
			msgs, rerr := stream.ReadMessagesByIDs(ctx, w.redis, p.ID)
			if rerr != nil {
				w.logger.Error(
					"poison message data lost, acknowledging orphaned PEL entry",
					"message_id", p.ID,
					"error", rerr,
				)
				if _, aerr := stream.Acknowledge(ctx, w.redis, p.ID); aerr != nil {
					w.logger.Error("acknowledge orphaned PEL entry failed", "message_id", p.ID, "error", aerr)
				}
				continue
			}
			w.quarantineOne(ctx, msgs[0], p.DeliveryCount)
		}
		return
	}

	counts := make(map[string]int64, len(poison))
	for _, p := range poison {
		counts[p.ID] = p.DeliveryCount
	}
	for _, m := range messages {
		w.quarantineOne(ctx, m, counts[m.ID])
	}
}

func (w *Worker) quarantineOne(ctx context.Context, m stream.Message, deliveries int64) {
	if err := w.processor.Quarantine(ctx, m, deliveries); err != nil {
		w.logger.Error(
			"quarantine poison message failed, will retry next tick",
			"message_id", m.ID,
			"job_id", m.JobID,
			"error", err,
		)
		return
	}
	w.logger.Info(
		"poison message quarantined to dead-letter queue",
		"message_id", m.ID,
		"job_id", m.JobID,
		"deliveries", deliveries,
	)
}
