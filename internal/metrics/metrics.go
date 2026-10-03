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

	submitted, err := c.redis.Get(ctx, JobsSubmittedKey).Int64()
	if err != nil && err != redis.Nil {
		ch <- prometheus.NewInvalidMetric(
			c.jobsSubmitted,
			fmt.Errorf("read submitted jobs metric: %w", err),
		)
		submitted = 0
	}

	completed, err := c.redis.Get(ctx, JobsCompletedKey).Int64()
	if err != nil && err != redis.Nil {
		ch <- prometheus.NewInvalidMetric(
			c.jobsCompleted,
			fmt.Errorf("read completed jobs metric: %w", err),
		)
		completed = 0
	}

	failed, err := c.redis.Get(ctx, JobsFailedKey).Int64()
	if err != nil && err != redis.Nil {
		ch <- prometheus.NewInvalidMetric(
			c.jobsFailed,
			fmt.Errorf("read failed jobs metric: %w", err),
		)
		failed = 0
	}

	queueDepth, err := c.redis.XLen(ctx, stream.ReadyStream).Result()
	if err != nil {
		ch <- prometheus.NewInvalidMetric(
			c.queueDepth,
			fmt.Errorf("read queue depth: %w", err),
		)
		queueDepth = 0
	}

	ch <- prometheus.MustNewConstMetric(
		c.jobsSubmitted,
		prometheus.CounterValue,
		float64(submitted),
	)

	ch <- prometheus.MustNewConstMetric(
		c.jobsCompleted,
		prometheus.CounterValue,
		float64(completed),
	)

	ch <- prometheus.MustNewConstMetric(
		c.jobsFailed,
		prometheus.CounterValue,
		float64(failed),
	)

	ch <- prometheus.MustNewConstMetric(
		c.queueDepth,
		prometheus.GaugeValue,
		float64(queueDepth),
	)
}

func Increment(ctx context.Context, redisClient *redis.Client, key string) error {
	return redisClient.Incr(ctx, key).Err()
}
