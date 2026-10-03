package config

import "log/slog"

func LogSnapshot(log *slog.Logger, cfg Config) {
	log.Info(
		"resolved configuration",
		"JOB_SCHEDULER_DB_DSN", "***",
		"REDIS_ADDR", cfg.RedisAddr,
		"API_PORT", cfg.APIPort,
		"LOG_LEVEL", cfg.LogLevel,
		"DB_MAX_CONNS", cfg.DBMaxConns,
		"SCHEDULER_POLL_INTERVAL", cfg.PollInterval.String(),
	)
}
