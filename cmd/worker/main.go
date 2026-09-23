package main

import (
	"context"
	"errors"
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
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	log := logger.New("worker")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	pool, err := db.NewPool(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	redisClient, err := redisclient.New(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	jobRepo := repository.NewJobRepository(pool)
	executionRepo := repository.NewJobExecutionRepository(pool)

	processor := worker.NewProcessor(
		jobRepo,
		executionRepo,
		redisClient,
		executor.NewHTTPExecutor(),
	)

	w := worker.NewWorker(
		redisClient,
		processor,
		"worker-"+os.Getenv("HOSTNAME"),
		log,
	)

	log.Info("worker started")

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker stopped", "error", err)
		os.Exit(1)
	}

	log.Info("worker stopped")
}
