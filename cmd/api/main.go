package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/redisclient"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
)

func main() {

	log := logger.New("api")

	cfg, err := config.Load()
	if err != nil {
		log.Error("something wrong with config", "err", err)
		return
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := db.NewPool(startupCtx, cfg)
	if err != nil {
		log.Error("error creating connection pool", "err", err)
		return
	}

	defer pool.Close()

	jobRepo := repository.NewJobRepository(pool)
	handler := NewHandler(jobRepo)

	redisClient, err := redisclient.New(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	limiter := ratelimit.New(redisClient, 5, time.Minute)

	server := &http.Server{
		Addr:    ":" + cfg.APIPort,
		Handler: Server(log, handler, limiter),
	}
	log.Info("API server listening", "addr", server.Addr)
	err = server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		log.Error("HTTP server failed", "err", err)
	}

}
