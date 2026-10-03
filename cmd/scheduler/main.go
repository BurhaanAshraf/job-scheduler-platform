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
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/redisclient"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
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
	bootstrap := logger.New("scheduler")
	if err != nil {
		bootstrap.Error("invalid configuration", "err", err)
		return 1
	}
	log := logger.NewWithLevel("scheduler", logger.ParseLevel(cfg.LogLevel))

	config.LogSnapshot(log, cfg)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	redisClient, err := redisclient.New(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		return 1
	}
	defer redisClient.Close()

	pool, err := db.NewPool(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Postgres", "error", err)
		return 1
	}
	defer pool.Close()

	cronRepo := repository.NewCronJobRepository(pool)

	instanceID := os.Getenv("SCHEDULER_INSTANCE_ID")

	if instanceID == "" {
		hostname, herr := os.Hostname()
		if herr != nil || hostname == "" {
			hostname = "scheduler"
		}
		instanceID = fmt.Sprintf("%s-%s", hostname, uuid.NewString()[:8])
	}

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		"scheduler:leader",
		instanceID,
		30*time.Second,
	)

	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		log,
	)

	log.Info("scheduler started", "instance_id", instanceID)

	if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("scheduler stopped", "error", err)
		return 1
	}

	log.Info("scheduler stopped")
	return 0
}
