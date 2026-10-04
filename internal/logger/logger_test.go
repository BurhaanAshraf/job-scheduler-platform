package logger

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"DEBUG":    slog.LevelDebug,
		"  debug ": slog.LevelDebug,
		"info":     slog.LevelInfo,
		"":         slog.LevelInfo,
		"bogus":    slog.LevelInfo,
		"warn":     slog.LevelWarn,
		"warning":  slog.LevelWarn,
		"WARN":     slog.LevelWarn,
		"error":    slog.LevelError,
		"ERROR":    slog.LevelError,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestNew(t *testing.T) {
	l := New("test-service")
	if l == nil {
		t.Fatal("New returned nil")
	}
	// Must not panic when logging.
	l.Info("hello", "k", "v")
}

func TestNewWithLevel(t *testing.T) {
	l := NewWithLevel("test-service", slog.LevelDebug)
	if l == nil {
		t.Fatal("NewWithLevel returned nil")
	}
	if !l.Enabled(nil, slog.LevelDebug) {
		t.Error("debug-level logger should enable debug records")
	}
	l.Debug("debug line")
	l.Error("error line")
}
