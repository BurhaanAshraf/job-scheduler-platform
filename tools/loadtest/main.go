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
	"sync/atomic"
	"time"
)

const baseURL = "http://localhost:4000"

// LOADTEST_API_KEYS: comma-separated API keys (provision N keys first; each
// allows 60 req/min, so 5 keys sustain ~1200 requests over 4 minutes).
// LOADTEST_TOTAL / LOADTEST_CONCURRENCY / LOADTEST_MINUTES override defaults.
var (
	totalRequests = envInt("LOADTEST_TOTAL", 1200)
	concurrency   = envInt("LOADTEST_CONCURRENCY", 10)
	testDuration  = time.Duration(envInt("LOADTEST_MINUTES", 4)) * time.Minute
	apiKeys       = loadKeys()
	runID         = time.Now().UTC().Format("20060102-150405")

	_ = os.Getenv // keep os import if unused in future edits
)

func loadKeys() []string {
	raw := os.Getenv("LOADTEST_API_KEYS")
	if raw == "" {
		fmt.Println("FATAL: LOADTEST_API_KEYS is required (comma-separated API keys)")
		os.Exit(2)
	}
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		fmt.Println("FATAL: LOADTEST_API_KEYS contained no keys")
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

type jobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	RunAt          time.Time       `json:"run_at"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
	CallbackURL    string          `json:"callback_url"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), testDuration+30*time.Second)
	defer cancel()

	var (
		submitted   int64
		success     int64
		rateLimited int64
		errors      int64
		latencies   []time.Duration
		latencyMu   sync.Mutex
		wg          sync.WaitGroup
	)

	sem := make(chan struct{}, concurrency)
	client := &http.Client{Timeout: 10 * time.Second}

	keyIdx := 0
	keyMu := sync.Mutex{}

	start := time.Now()
	nextSend := start
	interval := testDuration / time.Duration(totalRequests)

	for i := 0; i < totalRequests; i++ {
		select {
		case <-ctx.Done():
			goto done
		default:
			// Wait until it's time to send this request
			now := time.Now()
			if now.Before(nextSend) {
				time.Sleep(nextSend.Sub(now))
			}
			nextSend = nextSend.Add(interval)
		}

		payload, _ := json.Marshal(map[string]int{"seq": i})
		req := jobRequest{
			Type:           "load-test-sustained",
			Payload:        payload,
			RunAt:          time.Now().Add(-time.Hour),
			MaxAttempts:    1,
			IdempotencyKey: fmt.Sprintf("%s-load-sustained-%d", runID, i),
			CallbackURL:    "http://callback:8080/hook",
		}

		select {
		case <-ctx.Done():
			goto done
		case sem <- struct{}{}:
			wg.Add(1)
			go func(r jobRequest) {
				defer func() { <-sem }()
				defer wg.Done()

				keyMu.Lock()
				key := apiKeys[keyIdx%len(apiKeys)]
				keyIdx++
				keyMu.Unlock()

				body, _ := json.Marshal(r)
				httpReq, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1/jobs", bytes.NewReader(body))
				httpReq.Header.Set("Authorization", "Bearer "+key)
				httpReq.Header.Set("Content-Type", "application/json")

				reqStart := time.Now()
				resp, err := client.Do(httpReq)
				latency := time.Since(reqStart)

				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()

				atomic.AddInt64(&submitted, 1)

				if err != nil {
					atomic.AddInt64(&errors, 1)
					return
				}
				defer func() { _ = resp.Body.Close() }()
				_, _ = io.Copy(io.Discard, resp.Body)

				switch resp.StatusCode {
				case 200, 201:
					atomic.AddInt64(&success, 1)
				case 429:
					atomic.AddInt64(&rateLimited, 1)
				default:
					atomic.AddInt64(&errors, 1)
				}
			}(req)
		}
	}

done:
	wg.Wait()
	elapsed := time.Since(start)

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50 := latencies[len(latencies)/2]
		p95 := latencies[int(float64(len(latencies))*0.95)]
		p99 := latencies[int(float64(len(latencies))*0.99)]
		maxLat := latencies[len(latencies)-1]

		fmt.Printf("\n=== SUSTAINED LOAD TEST RESULTS (%v, %d keys) ===\n", testDuration, len(apiKeys))
		fmt.Printf("Total requests:     %d\n", totalRequests)
		fmt.Printf("Concurrency:        %d\n", concurrency)
		fmt.Printf("API Keys used:      %d\n", len(apiKeys))
		fmt.Printf("Test duration:      %v\n", testDuration)
		fmt.Printf("Actual duration:    %v\n", elapsed.Round(time.Millisecond))
		fmt.Printf("Throughput:         %.2f req/s\n", float64(totalRequests)/elapsed.Seconds())
		fmt.Printf("\nSubmitted:          %d\n", atomic.LoadInt64(&submitted))
		fmt.Printf("Success (200/201):  %d\n", atomic.LoadInt64(&success))
		fmt.Printf("Rate limited (429): %d\n", atomic.LoadInt64(&rateLimited))
		fmt.Printf("Errors:             %d\n", atomic.LoadInt64(&errors))
		fmt.Printf("\nLatency p50:        %v\n", p50.Round(time.Microsecond))
		fmt.Printf("Latency p95:        %v\n", p95.Round(time.Microsecond))
		fmt.Printf("Latency p99:        %v\n", p99.Round(time.Microsecond))
		fmt.Printf("Latency max:        %v\n", maxLat.Round(time.Microsecond))
		fmt.Printf("\nSuccess rate:       %.2f%%\n", float64(atomic.LoadInt64(&success))/float64(totalRequests)*100)
		fmt.Printf("Expected max (%d keys): %d\n", len(apiKeys), len(apiKeys)*60*int(testDuration.Minutes()))
	}
}
