package logger

import (
	"log/slog"
	"os"
	"strings"
)

// ParseLevel converts a LOG_LEVEL string to slog.Level. Unknown values
// default to info so a misconfigured env var never crashes the process.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func New(service string) *slog.Logger {
	return NewWithLevel(service, slog.LevelInfo)
}

func NewWithLevel(service string, level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level, ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		switch a.Key {
		case slog.TimeKey:
			a.Key = "timestamp"
		case slog.MessageKey:
			a.Key = "message"
		}

		return a
	}})
	logger := slog.New(handler)
	return logger.With("service", service)
}
