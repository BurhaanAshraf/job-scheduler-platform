package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
	for _, a := range os.Args[1:] {
		if a == "-healthcheck" || a == "--healthcheck" {
			os.Exit(runHealthcheck())
		}
	}
	os.Exit(run())
}

func run() int {
	// Load config first with a bootstrap logger; LOG_LEVEL applies after.
	bootstrap := logger.New("api")
	cfg, err := config.Load()
	if err != nil {
		bootstrap.Error("invalid configuration", "err", err)
		return 1
	}

	log := logger.NewWithLevel("api", logger.ParseLevel(cfg.LogLevel))
	config.LogSnapshot(log, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	pool, err := db.NewPool(dbCtx, cfg)
	dbCancel()
	if err != nil {
		log.Error("error creating connection pool", "err", err)
		return 1
	}
	defer pool.Close()

	redisCtx, redisCancel := context.WithTimeout(ctx, 5*time.Second)
	redisClient, err := redisclient.New(redisCtx, cfg)
	redisCancel()
	if err != nil {
		log.Error("failed to connect to Redis", "error", err)
		return 1
	}
	defer redisClient.Close()

	metricsCollector := metrics.NewCollector(redisClient)
	if err := prometheus.Register(metricsCollector); err != nil {
		log.Error("failed to register metrics", "error", err)
		return 1
	}

	healthChecker := health.NewChecker(
		pool,
		redisPinger{client: redisClient},
		2*time.Second,
	)

	healthHandler := NewHealthHandler(healthChecker)

	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)

	handler := NewHandler(jobRepo, cronRepo, redisClient, log)

	limiter := ratelimit.New(redisClient, 60, time.Minute)

	server := &http.Server{
		Addr:              ":" + cfg.APIPort,
		Handler:           Server(log, handler, healthHandler, limiter),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Info("API server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
		return 1
	}
	return 0
}
