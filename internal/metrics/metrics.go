package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	JobsSubmittedKey = "metrics:jobs_submitted_total"
	JobsCompletedKey = "metrics:jobs_completed_total"
	JobsFailedKey    = "metrics:jobs_failed_total"
)

type Collector struct {
	redis *redis.Client

	jobsSubmitted *prometheus.Desc
	jobsCompleted *prometheus.Desc
	jobsFailed    *prometheus.Desc
	queueDepth    *prometheus.Desc
}

func NewCollector(redisClient *redis.Client) *Collector {
	return &Collector{
		redis: redisClient,

		jobsSubmitted: prometheus.NewDesc(
			"jobs_submitted_total",
			"Total number of jobs submitted.",
			nil,
			nil,
		),
		jobsCompleted: prometheus.NewDesc(
			"jobs_completed_total",
			"Total number of jobs completed.",
			nil,
			nil,
		),
		jobsFailed: prometheus.NewDesc(
			"jobs_failed_total",
			"Total number of jobs failed.",
			nil,
			nil,
		),
		queueDepth: prometheus.NewDesc(
			"queue_depth",
			"Current queue depth from the ready stream length.",
			nil,
			nil,
		),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobsSubmitted
	ch <- c.jobsCompleted
	ch <- c.jobsFailed
	ch <- c.queueDepth
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	submitted, submittedOK := int64(0), true
	if v, err := c.redis.Get(ctx, JobsSubmittedKey).Int64(); err != nil && err != redis.Nil {
		// Report the error only: emitting a zero-valued sample alongside
		// the invalid metric would both alarm and mislead dashboards.
		ch <- prometheus.NewInvalidMetric(
			c.jobsSubmitted,
			fmt.Errorf("read submitted jobs metric: %w", err),
		)
		submittedOK = false
	} else {
		submitted = v
	}

	completed, completedOK := int64(0), true
	if v, err := c.redis.Get(ctx, JobsCompletedKey).Int64(); err != nil && err != redis.Nil {
		ch <- prometheus.NewInvalidMetric(
			c.jobsCompleted,
			fmt.Errorf("read completed jobs metric: %w", err),
		)
		completedOK = false
	} else {
		completed = v
	}

	failed, failedOK := int64(0), true
	if v, err := c.redis.Get(ctx, JobsFailedKey).Int64(); err != nil && err != redis.Nil {
		ch <- prometheus.NewInvalidMetric(
			c.jobsFailed,
			fmt.Errorf("read failed jobs metric: %w", err),
		)
		failedOK = false
	} else {
		failed = v
	}

	queueDepth, depthOK := int64(0), true
	if v, err := stream.QueueBacklog(ctx, c.redis); err != nil {
		ch <- prometheus.NewInvalidMetric(
			c.queueDepth,
			fmt.Errorf("read queue depth: %w", err),
		)
		depthOK = false
	} else {
		queueDepth = v
	}

	if submittedOK {
		ch <- prometheus.MustNewConstMetric(
			c.jobsSubmitted,
			prometheus.CounterValue,
			float64(submitted),
		)
	}

	if completedOK {
		ch <- prometheus.MustNewConstMetric(
			c.jobsCompleted,
			prometheus.CounterValue,
			float64(completed),
		)
	}

	if failedOK {
		ch <- prometheus.MustNewConstMetric(
			c.jobsFailed,
			prometheus.CounterValue,
			float64(failed),
		)
	}

	if depthOK {
		ch <- prometheus.MustNewConstMetric(
			c.queueDepth,
			prometheus.GaugeValue,
			float64(queueDepth),
		)
	}
}

func Increment(ctx context.Context, redisClient *redis.Client, key string) error {
	return redisClient.Incr(ctx, key).Err()
}
