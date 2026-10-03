package config

import (
	"os"
	"testing"
	"time"
)

func setEnv(t *testing.T, k, v string) {
	t.Helper()
	t.Setenv(k, v)
}

func baseEnv(t *testing.T) {
	t.Helper()
	setEnv(t, "JOB_SCHEDULER_DB_DSN", "postgres://u:p@localhost:5432/db?sslmode=disable")
	setEnv(t, "DB_DSN", "")
	setEnv(t, "REDIS_ADDR", "localhost:6379")
	setEnv(t, "API_PORT", "4000")
	setEnv(t, "LOG_LEVEL", "info")
	setEnv(t, "DB_MAX_CONNS", "10")
}

func TestLoad_PollIntervalDefault(t *testing.T) {
	baseEnv(t)
	_ = os.Unsetenv("SCHEDULER_POLL_INTERVAL")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Fatalf("default poll interval = %v, want 500ms", cfg.PollInterval)
	}
}

func TestLoad_PollIntervalEnvChangesDispatchLatency(t *testing.T) {
	// 6.4 Done-when: changing the env var changes observed dispatch latency
	// (here: the parsed poll interval driving the scheduler ticker).
	baseEnv(t)
	setEnv(t, "SCHEDULER_POLL_INTERVAL", "100ms")
	fast, err := Load()
	if err != nil {
		t.Fatalf("Load fast: %v", err)
	}
	setEnv(t, "SCHEDULER_POLL_INTERVAL", "2s")
	slow, err := Load()
	if err != nil {
		t.Fatalf("Load slow: %v", err)
	}
	if fast.PollInterval != 100*time.Millisecond {
		t.Fatalf("fast = %v, want 100ms", fast.PollInterval)
	}
	if slow.PollInterval != 2*time.Second {
		t.Fatalf("slow = %v, want 2s", slow.PollInterval)
	}
	if fast.PollInterval >= slow.PollInterval {
		t.Fatalf("env change did not alter interval: fast=%v slow=%v", fast.PollInterval, slow.PollInterval)
	}
	// A ticker with the fast interval fires strictly sooner than the slow one.
	fastCh := time.After(fast.PollInterval + 200*time.Millisecond)
	select {
	case <-time.After(slow.PollInterval):
		t.Fatal("slow interval elapsed before fast tick observation window")
	case <-fastCh:
	}
}

func TestLoad_RejectsNonPositivePollInterval(t *testing.T) {
	baseEnv(t)
	for _, v := range []string{"0s", "-1s", "bogus"} {
		setEnv(t, "SCHEDULER_POLL_INTERVAL", v)
		if _, err := Load(); err == nil {
			t.Fatalf("Load with %q: want error, got nil", v)
		}
	}
}

func TestLoad_DBDsnFallback(t *testing.T) {
	baseEnv(t)
	setEnv(t, "JOB_SCHEDULER_DB_DSN", "")
	setEnv(t, "DB_DSN", "postgres://u:p@localhost:5432/fallback?sslmode=disable")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DBDSN != "postgres://u:p@localhost:5432/fallback?sslmode=disable" {
		t.Fatalf("fallback DSN not honored: %q", cfg.DBDSN)
	}
}
