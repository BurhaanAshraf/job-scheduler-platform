package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/executor"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/redisclient"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/worker"
	"github.com/google/uuid"
)

func main() {
	for _, a := range os.Args[1:] {
		if a == "-healthcheck" || a == "--healthcheck" {
			os.Exit(runHealthcheck())
		}
	}
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load()
	bootstrap := logger.New("worker")
	if err != nil {
		bootstrap.Error("invalid configuration", "err", err)
		return 1
	}
	log := logger.NewWithLevel("worker", logger.ParseLevel(cfg.LogLevel))

	config.LogSnapshot(log, cfg)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := db.NewPool(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		return 1
	}
	defer pool.Close()

	redisClient, err := redisclient.New(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		return 1
	}
	defer func() { _ = redisClient.Close() }()

	if err := stream.EnsureConsumerGroup(startupCtx, redisClient); err != nil {
		log.Error("failed to ensure consumer group", "error", err)
		return 1
	}

	jobRepo := repository.NewJobRepository(pool)
	executionRepo := repository.NewJobExecutionRepository(pool)

	processor := worker.NewProcessor(
		jobRepo,
		executionRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "worker"
	}
	if h := os.Getenv("HOSTNAME"); h != "" {
		hostname = h
	}
	if h := os.Getenv("SCHEDULER_INSTANCE_ID"); h != "" && os.Getenv("HOSTNAME") == "" {
		hostname = h
	}
	consumer := fmt.Sprintf("%s-%s", hostname, uuid.NewString()[:8])

	w := worker.NewWorker(
		redisClient,
		processor,
		consumer,
		log,
	)

	log.Info("worker started", "consumer", consumer)

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker stopped", "error", err)
		return 1
	}

	log.Info("worker stopped")
	return 0
}
