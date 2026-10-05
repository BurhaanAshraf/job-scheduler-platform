package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

// runHealthcheck probes the local /healthz endpoint (which itself checks
// Postgres + Redis) so Dockerfile HEALTHCHECK works without shell/wget on a
// minimal (scratch) image. Used as: /api -healthcheck
func runHealthcheck() int {
	port := os.Getenv("API_PORT")
	if port == "" {
		port = "4000"
	}
	url := fmt.Sprintf("http://127.0.0.1:%s/healthz", port)
	// /healthz checks Postgres + Redis sequentially (up to ~4s worst case),
	// so the probe budget must exceed that plus scheduling slack.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthz status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}
