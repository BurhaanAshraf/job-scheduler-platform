// Command stress hammers every API endpoint concurrently and reports
// per-endpoint results. It exercises success paths, validation rejections,
// state conflicts, auth failures and rate limiting in one run, while the
// live worker executes the submitted jobs against the compose callback sink.
//
// Required env: STRESS_API_KEYS (comma-separated, at least 2 keys).
// Optional: STRESS_OPS (total ops, default 1000), STRESS_WORKERS (default 20).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const baseURL = "http://localhost:4000"

var (
	totalOps = envInt("STRESS_OPS", 1000)
	workers  = envInt("STRESS_WORKERS", 20)
	apiKeys  = loadKeys()
	runID    = time.Now().UTC().Format("20060102-150405")

	client = &http.Client{Timeout: 10 * time.Second}
)

func loadKeys() []string {
	raw := os.Getenv("STRESS_API_KEYS")
	if raw == "" {
		fmt.Println("FATAL: STRESS_API_KEYS is required (comma-separated, >= 2 keys)")
		os.Exit(2)
	}
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) < 2 {
		fmt.Println("FATAL: STRESS_API_KEYS needs at least 2 keys (rate-limit headroom)")
		os.Exit(2)
	}
	return keys
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return def
}

type outcome struct {
	endpoint string
	want     int
	got      int
	latency  time.Duration
}

func call(ctx context.Context, method, path, key string, body any) (int, []byte, time.Duration) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, baseURL+path, r)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := client.Do(req)
	lat := time.Since(start)
	if err != nil {
		return -1, nil, lat
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, lat
}

func jobBody(seq int) map[string]any {
	return map[string]any{
		"type": "stress", "payload": map[string]int{"seq": seq},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 2, "idempotency_key": fmt.Sprintf("%s-stress-%d", runID, seq),
		"callback_url": "http://callback:8080/hook",
	}
}

func main() {
	ctx := context.Background()
	jobs := make(chan int, totalOps)
	for i := 0; i < totalOps; i++ {
		jobs <- i
	}
	close(jobs)

	var mu sync.Mutex
	type counters struct{ ok, limited, bad int64 }
	stats := map[string]*counters{}
	var latencies []time.Duration
	var wg sync.WaitGroup

	// A 429 is correct behavior under hammering (60 req/min/key): it proves
	// the limiter holds, so it is tallied separately and never a failure.
	record := func(endpoint string, want, got int, lat time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		s, ok := stats[endpoint]
		if !ok {
			s = &counters{}
			stats[endpoint] = s
		}
		switch {
		case want == got:
			s.ok++
		case got == 429:
			s.limited++
		default:
			s.bad++
			if s.bad <= 5 {
				fmt.Printf("  UNEXPECTED %s: want %d got %d\n", endpoint, want, got)
			}
		}
		latencies = append(latencies, lat)
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := apiKeys[w%len(apiKeys)]
			for seq := range jobs {
				kind := seq % 14
				switch kind {
				case 0, 1, 2: // submit immediate -> 201
					st, body, lat := call(ctx, "POST", "/v1/jobs", key, jobBody(seq*100+w))
					record("POST /v1/jobs", 201, st, lat)
					if st == 201 {
						var d map[string]string
						_ = json.Unmarshal(body, &d)
						id := d["id"]
						// get it -> 200
						st2, _, lat2 := call(ctx, "GET", "/v1/jobs/"+id, key, nil)
						record("GET /v1/jobs/{id}", 200, st2, lat2)
					}
				case 3: // submit future then cancel -> 201, 204
					b := jobBody(seq*100 + w)
					b["run_at"] = time.Now().Add(24 * time.Hour).Format(time.RFC3339)
					b["idempotency_key"] = fmt.Sprintf("%s-stress-cancel-%d-%d", runID, seq, w)
					st, body, lat := call(ctx, "POST", "/v1/jobs", key, b)
					record("POST /v1/jobs future", 201, st, lat)
					if st == 201 {
						var d map[string]string
						_ = json.Unmarshal(body, &d)
						st2, _, lat2 := call(ctx, "DELETE", "/v1/jobs/"+d["id"], key, nil)
						record("DELETE /v1/jobs/{id}", 204, st2, lat2)
					}
				case 4: // list -> 200
					st, _, lat := call(ctx, "GET", "/v1/jobs?limit=20", key, nil)
					record("GET /v1/jobs", 200, st, lat)
				case 5: // list filtered + capped -> 200
					st, _, lat := call(ctx, "GET", "/v1/jobs?status=done&limit=500", key, nil)
					record("GET /v1/jobs filtered+capped", 200, st, lat)
				case 6: // bad body -> 400
					st, _, lat := call(ctx, "POST", "/v1/jobs", key, map[string]any{"type": ""})
					record("POST /v1/jobs invalid", 400, st, lat)
				case 7: // SSRF -> 400
					b := jobBody(seq*100 + w)
					b["idempotency_key"] = fmt.Sprintf("%s-stress-ssrf-%d-%d", runID, seq, w)
					b["callback_url"] = "http://169.254.169.254/hook"
					st, _, lat := call(ctx, "POST", "/v1/jobs", key, b)
					record("POST /v1/jobs ssrf", 400, st, lat)
				case 8: // missing auth -> 401
					st, _, lat := call(ctx, "GET", "/v1/jobs", "", nil)
					record("GET /v1/jobs no-auth", 401, st, lat)
				case 9: // bad key -> 401
					st, _, lat := call(ctx, "GET", "/v1/jobs", "bad-key", nil)
					record("GET /v1/jobs bad-key", 401, st, lat)
				case 10: // malformed id -> 400
					st, _, lat := call(ctx, "GET", "/v1/jobs/not-a-uuid", key, nil)
					record("GET /v1/jobs bad-id", 400, st, lat)
				case 11: // unknown id -> 404
					st, _, lat := call(ctx, "GET", "/v1/jobs/00000000-0000-0000-0000-000000000000", key, nil)
					record("GET /v1/jobs missing", 404, st, lat)
				case 12: // dead-letters -> 200, retry missing -> 404
					st, _, lat := call(ctx, "GET", "/v1/dead-letters?limit=10", key, nil)
					record("GET /v1/dead-letters", 200, st, lat)
					st2, _, lat2 := call(ctx, "POST", "/v1/jobs/00000000-0000-0000-0000-000000000000/retry", key, nil)
					record("POST retry missing", 404, st2, lat2)
				case 13: // cron create + disable -> 201, 204
					cb := map[string]any{
						"cron_expression": "*/5 * * * *",
						"job_template": map[string]any{
							"type": "stress-cron", "payload": map[string]int{"s": seq},
							"max_attempts": 1, "callback_url": "http://callback:8080/hook",
						},
					}
					st, body, lat := call(ctx, "POST", "/v1/cron-jobs", key, cb)
					record("POST /v1/cron-jobs", 201, st, lat)
					if st == 201 {
						var d map[string]float64
						_ = json.Unmarshal(body, &d)
						id := fmt.Sprintf("%.0f", d["id"])
						st2, _, lat2 := call(ctx, "PATCH", "/v1/cron-jobs/"+id, key, map[string]bool{"enabled": false})
						record("PATCH /v1/cron-jobs", 204, st2, lat2)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var totalOK, totalLimited, totalBad int64
	names := make([]string, 0, len(stats))
	for n := range stats {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Printf("\n=== STRESS RESULTS (%d ops, %d workers, %d keys) ===\n", totalOps, workers, len(apiKeys))
	for _, n := range names {
		s := stats[n]
		totalOK += s.ok
		totalLimited += s.limited
		totalBad += s.bad
		fmt.Printf("%-32s ok=%-6d limited=%-6d unexpected=%d\n", n, s.ok, s.limited, s.bad)
	}
	if len(latencies) > 0 {
		fmt.Printf("latency p50=%v p95=%v p99=%v max=%v\n",
			latencies[len(latencies)/2].Round(time.Microsecond),
			latencies[int(float64(len(latencies))*0.95)].Round(time.Microsecond),
			latencies[int(float64(len(latencies))*0.99)].Round(time.Microsecond),
			latencies[len(latencies)-1].Round(time.Microsecond))
	}
	fmt.Printf("TOTAL ok=%d limited(429)=%d unexpected=%d\n", totalOK, totalLimited, totalBad)
	if totalBad > 0 {
		os.Exit(1)
	}
}
