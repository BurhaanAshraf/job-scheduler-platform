package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBDSN         string
	RedisAddr     string
	RedisPassword string
	APIPort       string
	LogLevel      string
	DBMaxConns    int
	PollInterval  time.Duration
}

func Load() (Config, error) {

	cfg := Config{
		DBDSN:         firstNonEmpty(os.Getenv("JOB_SCHEDULER_DB_DSN"), os.Getenv("DB_DSN")),
		RedisAddr:     os.Getenv("REDIS_ADDR"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		APIPort:       os.Getenv("API_PORT"),
		LogLevel:      os.Getenv("LOG_LEVEL"),
	}

	if cfg.DBDSN == "" {
		return Config{}, errors.New("JOB_SCHEDULER_DB_DSN (or DB_DSN) is required")
	}
	if cfg.RedisAddr == "" {
		return Config{}, errors.New("REDIS_ADDR is required")
	}

	if cfg.APIPort == "" {
		return Config{}, errors.New("API_PORT is required")
	}
	if _, err := strconv.Atoi(cfg.APIPort); err != nil {
		return Config{}, fmt.Errorf("invalid API_PORT %q: must be a numeric port", cfg.APIPort)
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, fmt.Errorf("invalid LOG_LEVEL %q: must be one of debug, info, warn, error", cfg.LogLevel)
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
	maxConnsRaw := os.Getenv("DB_MAX_CONNS")
	if maxConnsRaw == "" {
		cfg.DBMaxConns = 10
		return cfg, nil
	}
	maxConns, err := strconv.Atoi(maxConnsRaw)
	if err != nil || maxConns <= 0 {
		return Config{}, errors.New("DB_MAX_CONNS must be a positive integer")
	}
	cfg.DBMaxConns = maxConns
	return cfg, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
