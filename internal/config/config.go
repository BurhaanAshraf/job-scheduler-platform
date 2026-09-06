package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	DBDSN      string
	RedisAddr  string
	APIPort    string
	LogLevel   string
	DBMaxConns int
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
	maxConns, err := strconv.Atoi(os.Getenv("DB_MAX_CONNS"))
	if err != nil || maxConns <= 0 {
		return Config{}, errors.New("DB_MAX_CONNS must be a positive integer")
	}
	cfg.DBMaxConns = maxConns
	return cfg, nil
}
