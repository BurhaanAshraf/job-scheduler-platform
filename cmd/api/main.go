package main

import (
	"context"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
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

	log.Info("API server is not implemented yet...")

}
