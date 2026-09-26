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
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/redisclient"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	log := logger.New("scheduler")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	startupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	redisClient, err := redisclient.New(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	pool, err := db.NewPool(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	cronRepo := repository.NewCronJobRepository(pool)

	instanceID := os.Getenv("SCHEDULER_INSTANCE_ID")

	if instanceID == "" {
		instanceID, err = os.Hostname()
		if err != nil {
			log.Error("failed to determine scheduler instance id", "error", err)
			os.Exit(1)
		}
	}

	leaderLock := scheduler.NewLeaderLock(
		redisClient,
		"scheduler:leader",
		instanceID,
		10*time.Second,
	)

	s := scheduler.New(
		redisClient,
		cronRepo,
		cfg.PollInterval,
		leaderLock,
		log,
	)

	log.Info("scheduler started")

	if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("scheduler stopped", "error", err)
		os.Exit(1)
	}

	log.Info("scheduler stopped")
}
