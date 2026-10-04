package config

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLogSnapshotRedactsDSN(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	cfg := Config{
		DBDSN:        "postgres://user:s3cr3t-pw@localhost:5432/db?sslmode=disable",
		RedisAddr:    "localhost:6379",
		APIPort:      "4000",
		LogLevel:     "info",
		DBMaxConns:   10,
		PollInterval: 500 * time.Millisecond,
	}

	LogSnapshot(log, cfg)

	out := buf.String()
	if strings.Contains(out, "s3cr3t-pw") {
		t.Fatalf("snapshot leaked password: %s", out)
	}
	for _, want := range []string{"resolved configuration", "localhost:6379", "4000", "info", "500ms"} {
		if !strings.Contains(out, want) {
			t.Errorf("snapshot missing %q: %s", want, out)
		}
	}
}
