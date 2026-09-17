package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBDSN        string
	RedisAddr    string
	APIPort      string
	LogLevel     string
	DBMaxConns   int
	PollInterval time.Duration
}

func Load() (Config, error) {

	cfg := Config{
		DBDSN:     os.Getenv("JOB_SCHEDULER_DB_DSN"),
		RedisAddr: os.Getenv("REDIS_ADDR"),
		APIPort:   os.Getenv("API_PORT"),
		LogLevel:  os.Getenv("LOG_LEVEL"),
	}

	if cfg.DBDSN == "" {
		return Config{}, errors.New("DB_DSN is required")
	}
	if cfg.RedisAddr == "" {
		return Config{}, errors.New("REDIS_ADDR is required")
	}

	if cfg.APIPort == "" {
		return Config{}, errors.New("API_PORT is required")
	}
	if cfg.LogLevel == "" {
		return Config{}, errors.New("LOG_LEVEL is required")
	}
	pollInterval := os.Getenv("SCHEDULER_POLL_INTERVAL")

	if pollInterval == "" {
		pollInterval = "500ms"
	}

	parsedPollInterval, err := time.ParseDuration(pollInterval)
	if err != nil || parsedPollInterval <= 0 {
		return Config{}, fmt.Errorf(
			"invalid SCHEDULER_POLL_INTERVAL %q: must be a positive duration",
			pollInterval,
		)
	}

	cfg.PollInterval = parsedPollInterval
	maxConns, err := strconv.Atoi(os.Getenv("DB_MAX_CONNS"))
	if err != nil || maxConns <= 0 {
		return Config{}, errors.New("DB_MAX_CONNS must be a positive integer")
	}
	cfg.DBMaxConns = maxConns
	return cfg, nil
}
