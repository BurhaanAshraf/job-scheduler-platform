package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/health"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/metrics"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/redisclient"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {

	log := logger.New("api")

	cfg, err := config.Load()
	if err != nil {
		log.Error("something wrong with config", "err", err)
		return
	}

	config.LogSnapshot(log, cfg)

	startupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := db.NewPool(startupCtx, cfg)
	if err != nil {
		log.Error("error creating connection pool", "err", err)
		return
	}

	defer pool.Close()

	redisClient, err := redisclient.New(startupCtx, cfg)
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	metricsCollector := metrics.NewCollector(redisClient)

	prometheus.MustRegister(metricsCollector)

	defer redisClient.Close()

	healthChecker := health.NewChecker(
		pool,
		redisPinger{client: redisClient},
		2*time.Second,
	)

	healthHandler := NewHealthHandler(healthChecker)

	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)

	handler := NewHandler(jobRepo, cronRepo, redisClient, log)

	limiter := ratelimit.New(redisClient, 5, time.Minute)

	server := &http.Server{
		Addr:    ":" + cfg.APIPort,
		Handler: Server(log, handler, healthHandler, limiter),
	}
	log.Info("API server listening", "addr", server.Addr)
	err = server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		log.Error("HTTP server failed", "err", err)
	}

}
