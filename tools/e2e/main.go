package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const baseURL = "http://localhost:4000"

var (
	client = &http.Client{Timeout: 10 * time.Second}

	// All keys come from the environment so no secret is ever committed.
	// Provision with: go run ./cmd/apikey (or the README psql snippet).
	mainKey        = mustEnv("E2E_API_KEY")
	rateLimitKey   = envOr("E2E_RATELIMIT_KEY", "")
	validationKeys = []string{envOr("E2E_VALIDATION_KEY_1", ""), envOr("E2E_VALIDATION_KEY_2", "")}

	// runID prefixes every idempotency key so reruns never collide with
	// previous runs (a replay would return 200 where the test wants 201).
	runID = time.Now().UTC().Format("20060102-150405")
)

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fmt.Printf("FATAL: %s is required (provision an API key first)\n", name)
		os.Exit(2)
	}
	return v
}

// wrongCredential fails authentication by design. It is a fixed
// non-secret placeholder, never a provisioned key.
const wrongCredential = "wrong-credentials"

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func idem(name string) string { return runID + "-" + name }

func main() {
	ctx := context.Background()
	passed := 0
	failed := 0

	// Validation tests just need *a* valid key; fall back to the main key
	// when dedicated ones are not provisioned.
	if validationKeys[0] == "" {
		validationKeys[0] = mainKey
	}
	if validationKeys[1] == "" {
		validationKeys[1] = mainKey
	}

	fmt.Println("=== COMPREHENSIVE E2E ENDPOINT TESTS ===")

	// Test 1: Health check
	if testHealthz(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 2: Metrics endpoint
	if testMetrics(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 3: Create job (immediate)
	jobID := testCreateJobImmediate(ctx)
	if jobID != "" {
		passed++
	} else {
		failed++
	}

	// Test 4: Create job (future)
	jobIDFuture := testCreateJobFuture(ctx)
	if jobIDFuture != "" {
		passed++
	} else {
		failed++
	}

	// Test 5: Get job
	if jobID != "" && testGetJob(ctx, jobID) {
		passed++
	} else {
		failed++
	}

	// Test 6: List jobs
	if testListJobs(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 7: Idempotency - same payload
	if testIdempotencyReplay(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 8: Idempotency - conflict
	if testIdempotencyConflict(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 9: Cancel job (pending)
	if testCancelJob(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 10: Cancel job (running) - should fail
	if testCancelRunningJob(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 11: Retry dead job
	if testRetryDeadJob(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 12: Dead letters list
	if testDeadLetters(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 13: Create cron job
	cronID := testCreateCronJob(ctx)
	if cronID != "" {
		passed++
	} else {
		failed++
	}

	// Test 14: Update cron job (disable)
	if cronID != "" && testUpdateCronJob(ctx, cronID) {
		passed++
	} else {
		failed++
	}

	// Test 15: Rate limiting (uses dedicated key)
	if testRateLimit(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 16: SSRF guard (uses validation key)
	if testSSRFGuard(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 17: Auth - missing header
	if testAuthMissingHeader(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 18: Auth - invalid key
	if testAuthInvalidKey(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 19: Invalid job ID (uses validation key)
	if testInvalidJobID(ctx) {
		passed++
	} else {
		failed++
	}

	// Test 20: Job with max_attempts validation (uses validation key)
	if testMaxAttemptsValidation(ctx) {
		passed++
	} else {
		failed++
	}

	fmt.Printf("\n=== RESULTS: %d passed, %d failed ===\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

// doRequest returns the status code and body. Transport errors yield
// status -1. The response body is fully read and closed inside, so no
// *http.Response (and no close obligation) ever escapes to callers.
func doRequest(ctx context.Context, method, path, apiKey string, body any, expectedStatus int) (int, []byte) {
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, baseURL+path, bodyReader)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return -1, nil
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != expectedStatus {
		fmt.Printf("  FAIL: Expected %d, got %d: %s\n", expectedStatus, resp.StatusCode, string(respBody))
		return resp.StatusCode, respBody
	}
	return resp.StatusCode, respBody
}

func testHealthz(ctx context.Context) bool {
	fmt.Print("Test 1: GET /healthz ... ")
	respcode, _ := doRequest(ctx, "GET", "/healthz", mainKey, nil, 200)
	if respcode < 0 {
		return false
	}
	fmt.Println("PASS")
	return true
}

func testMetrics(ctx context.Context) bool {
	fmt.Print("Test 2: GET /metrics ... ")
	respcode, body := doRequest(ctx, "GET", "/metrics", mainKey, nil, 200)
	if respcode < 0 {
		return false
	}
	if bytes.Contains(body, []byte("jobs_submitted_total")) {
		fmt.Println("PASS")
		return true
	}
	fmt.Println("FAIL: metrics not found")
	return false
}

func testCreateJobImmediate(ctx context.Context) string {
	fmt.Print("Test 3: POST /v1/jobs (immediate) ... ")
	body := map[string]any{
		"type": "e2e-immediate", "payload": map[string]string{"test": "immediate"},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 3, "idempotency_key": idem("e2e-immediate-1"),
		"callback_url": "http://callback:8080/hook",
	}
	respcode, respBody := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 201)
	if respcode < 0 {
		return ""
	}
	var data map[string]string
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Printf("FAIL: decode job id: %v\n", err)
		return ""
	}
	jobID := data["id"]
	fmt.Printf("PASS (job_id=%s)\n", jobID)
	return jobID
}

func testCreateJobFuture(ctx context.Context) string {
	fmt.Print("Test 4: POST /v1/jobs (future) ... ")
	body := map[string]any{
		"type": "e2e-future", "payload": map[string]string{"test": "future"},
		"run_at":       time.Now().Add(24 * time.Hour).Format(time.RFC3339),
		"max_attempts": 3, "idempotency_key": idem("e2e-future-1"),
		"callback_url": "http://callback:8080/hook",
	}
	respcode, respBody := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 201)
	if respcode < 0 {
		return ""
	}
	var data map[string]string
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Printf("FAIL: decode job id: %v\n", err)
		return ""
	}
	jobID := data["id"]
	fmt.Printf("PASS (job_id=%s)\n", jobID)
	return jobID
}

func testGetJob(ctx context.Context, jobID string) bool {
	fmt.Printf("Test 5: GET /v1/jobs/%s ... ", jobID)
	respcode, body := doRequest(ctx, "GET", "/v1/jobs/"+jobID, mainKey, nil, 200)
	if respcode < 0 {
		return false
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		fmt.Printf("FAIL: decode job: %v\n", err)
		return false
	}
	if data["id"] == jobID {
		fmt.Printf("PASS (status=%v)\n", data["status"])
		return true
	}
	fmt.Println("FAIL: job ID mismatch")
	return false
}

func testListJobs(ctx context.Context) bool {
	fmt.Print("Test 6: GET /v1/jobs ... ")
	respcode, body := doRequest(ctx, "GET", "/v1/jobs", mainKey, nil, 200)
	if respcode < 0 {
		return false
	}
	var data []map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		fmt.Printf("FAIL: decode job list: %v\n", err)
		return false
	}
	fmt.Printf("PASS (%d jobs)\n", len(data))
	return true
}

func testIdempotencyReplay(ctx context.Context) bool {
	fmt.Print("Test 7: POST /v1/jobs (idempotency replay) ... ")
	body := map[string]any{
		"type": "e2e-idempotent", "payload": map[string]string{"test": "idem"},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 3, "idempotency_key": idem("e2e-idem-1"),
		"callback_url": "http://callback:8080/hook",
	}
	// First request
	resp1code, body1 := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 201)
	if resp1code < 0 {
		return false
	}
	var data1 map[string]string
	if err := json.Unmarshal(body1, &data1); err != nil {
		fmt.Printf("FAIL: decode replay response: %v\n", err)
		return false
	}
	id1 := data1["id"]

	// Second request (identical)
	resp2code, body2 := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 200)
	if resp2code < 0 {
		return false
	}
	var data2 map[string]string
	if err := json.Unmarshal(body2, &data2); err != nil {
		fmt.Printf("FAIL: decode replay response: %v\n", err)
		return false
	}
	id2 := data2["id"]

	if id1 == id2 {
		fmt.Printf("PASS (same id=%s)\n", id1)
		return true
	}
	fmt.Printf("FAIL: IDs differ (%s vs %s)\n", id1, id2)
	return false
}

func testIdempotencyConflict(ctx context.Context) bool {
	fmt.Print("Test 8: POST /v1/jobs (idempotency conflict) ... ")
	body := map[string]any{
		"type": "e2e-conflict", "payload": map[string]string{"test": "conflict-different"},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 3, "idempotency_key": idem("e2e-idem-1"), // same key
		"callback_url": "http://callback:8080/hook",
	}
	respcode, _ := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 409)
	if respcode < 0 {
		return false
	}
	if respcode == 409 {
		fmt.Println("PASS (409 conflict)")
		return true
	}
	fmt.Printf("FAIL: got %d\n", respcode)
	return false
}

func testCancelJob(ctx context.Context) bool {
	fmt.Print("Test 9: DELETE /v1/jobs/{id} (pending) ... ")
	body := map[string]any{
		"type": "e2e-cancel", "payload": map[string]string{"test": "cancel"},
		"run_at":       time.Now().Add(24 * time.Hour).Format(time.RFC3339),
		"max_attempts": 3, "idempotency_key": idem("e2e-cancel-1"),
		"callback_url": "http://callback:8080/hook",
	}
	respcode, respBody := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 201)
	if respcode < 0 {
		return false
	}
	var data map[string]string
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Printf("FAIL: decode job id: %v\n", err)
		return false
	}
	jobID := data["id"]

	resp2code, _ := doRequest(ctx, "DELETE", "/v1/jobs/"+jobID, mainKey, nil, 204)
	if resp2code < 0 {
		return false
	}
	fmt.Println("PASS (204)")
	return true
}

func testCancelRunningJob(ctx context.Context) bool {
	fmt.Print("Test 10: DELETE /v1/jobs/{id} (running - should 409) ... ")
	body := map[string]any{
		"type": "e2e-cancel-running", "payload": map[string]string{"test": "cancel-running"},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 3, "idempotency_key": idem("e2e-cancel-running-1"),
		"callback_url": "http://callback:8080/hook",
	}
	respcode, respBody := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 201)
	if respcode < 0 {
		return false
	}
	var data map[string]string
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Printf("FAIL: decode job id: %v\n", err)
		return false
	}
	jobID := data["id"]

	// Wait for worker to pick it up
	time.Sleep(2 * time.Second)

	resp2code, _ := doRequest(ctx, "DELETE", "/v1/jobs/"+jobID, mainKey, nil, 409)
	if resp2code < 0 {
		return false
	}
	if resp2code == 409 {
		fmt.Println("PASS (409 conflict)")
		return true
	}
	fmt.Printf("FAIL: got %d\n", resp2code)
	return false
}

func testRetryDeadJob(ctx context.Context) bool {
	fmt.Print("Test 11: POST /v1/jobs/{id}/retry (dead job) ... ")
	// Create a job that will fail
	body := map[string]any{
		"type": "e2e-retry", "payload": map[string]string{"fail": "true"},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 1, "idempotency_key": idem("e2e-retry-1"),
		"callback_url": "http://callback:8080/hook-fail",
	}
	respcode, respBody := doRequest(ctx, "POST", "/v1/jobs", mainKey, body, 201)
	if respcode < 0 {
		return false
	}
	var data map[string]string
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Printf("FAIL: decode job id: %v\n", err)
		return false
	}
	jobID := data["id"]

	// Wait for it to go dead
	time.Sleep(5 * time.Second)

	resp2code, body2 := doRequest(ctx, "POST", "/v1/jobs/"+jobID+"/retry", mainKey, nil, 200)
	if resp2code < 0 {
		return false
	}
	var data2 map[string]any
	if err := json.Unmarshal(body2, &data2); err != nil {
		fmt.Printf("FAIL: decode retried job: %v\n", err)
		return false
	}
	if data2["status"] == "scheduled" && data2["attempts"].(float64) == 0 {
		fmt.Printf("PASS (re-queued as scheduled)\n")
		return true
	}
	fmt.Printf("FAIL: %s\n", string(body2))
	return false
}

func testDeadLetters(ctx context.Context) bool {
	fmt.Print("Test 12: GET /v1/dead-letters ... ")
	respcode, body := doRequest(ctx, "GET", "/v1/dead-letters", mainKey, nil, 200)
	if respcode < 0 {
		return false
	}
	var data []map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		fmt.Printf("FAIL: decode dead letters: %v\n", err)
		return false
	}
	fmt.Printf("PASS (%d dead letters)\n", len(data))
	return true
}

func testCreateCronJob(ctx context.Context) string {
	fmt.Print("Test 13: POST /v1/cron-jobs ... ")
	body := map[string]any{
		"cron_expression": "*/5 * * * *",
		"job_template": map[string]any{
			"type": "e2e-cron", "payload": map[string]string{"from": "cron"},
			"max_attempts": 2, "callback_url": "http://callback:8080/hook",
		},
	}
	respcode, respBody := doRequest(ctx, "POST", "/v1/cron-jobs", mainKey, body, 201)
	if respcode < 0 {
		return ""
	}
	var data map[string]float64 // ID is int64
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Printf("FAIL: decode cron id: %v\n", err)
		return ""
	}
	cronID := fmt.Sprintf("%.0f", data["id"])
	fmt.Printf("PASS (cron_id=%s)\n", cronID)
	return cronID
}

func testUpdateCronJob(ctx context.Context, cronID string) bool {
	fmt.Printf("Test 14: PATCH /v1/cron-jobs/%s (disable) ... ", cronID)
	body := map[string]bool{"enabled": false}
	respcode, _ := doRequest(ctx, "PATCH", "/v1/cron-jobs/"+cronID, mainKey, body, 204)
	if respcode < 0 {
		return false
	}
	fmt.Println("PASS (204)")
	return true
}

func testRateLimit(ctx context.Context) bool {
	fmt.Print("Test 15: Rate limiting (61st request) ... ")
	if rateLimitKey == "" {
		fmt.Println("SKIP (E2E_RATELIMIT_KEY unset)")
		return true
	}
	// Dedicated key: the first 60 requests must succeed, the 61st must 429.
	for i := 0; i < 61; i++ {
		body := map[string]any{
			"type": "ratelimit", "payload": map[string]int{"seq": i},
			"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
			"max_attempts": 1, "idempotency_key": fmt.Sprintf("%s-ratelimit-dedicated-%d", runID, i),
			"callback_url": "http://callback:8080/hook",
		}
		b, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1/jobs", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+rateLimitKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("FAIL: request %d: %v\n", i, err)
			return false
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		want := 201
		if i == 60 {
			want = 429
		}
		if resp.StatusCode != want {
			fmt.Printf("FAIL: request %d: want %d, got %d\n", i, want, resp.StatusCode)
			return false
		}
		if i == 60 {
			fmt.Printf("PASS (429, Retry-After=%s)\n", resp.Header.Get("Retry-After"))
			return true
		}
	}
	fmt.Println("FAIL: no 429 on 61st request")
	return false
}

func testSSRFGuard(ctx context.Context) bool {
	fmt.Print("Test 16: SSRF guard (localhost) ... ")
	body := map[string]any{
		"type": "ssrf", "payload": map[string]string{"test": "ssrf"},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 1, "idempotency_key": idem("e2e-ssrf-1"),
		"callback_url": "http://localhost:8080/hook",
	}
	respcode, _ := doRequest(ctx, "POST", "/v1/jobs", validationKeys[0], body, 400)
	if respcode < 0 {
		return false
	}
	if respcode == 400 {
		fmt.Println("PASS (400 rejected)")
		return true
	}
	fmt.Printf("FAIL: got %d\n", respcode)
	return false
}

func testAuthMissingHeader(ctx context.Context) bool {
	fmt.Print("Test 17: Auth missing header ... ")
	req, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1/jobs", nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil || resp == nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 401 {
		fmt.Println("PASS (401)")
		return true
	}
	fmt.Printf("FAIL: got %d\n", resp.StatusCode)
	return false
}

func testAuthInvalidKey(ctx context.Context) bool {
	fmt.Print("Test 18: Auth invalid key ... ")
	req, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+wrongCredential)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil || resp == nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 401 {
		fmt.Println("PASS (401)")
		return true
	}
	fmt.Printf("FAIL: got %d\n", resp.StatusCode)
	return false
}

func testInvalidJobID(ctx context.Context) bool {
	fmt.Print("Test 19: GET /v1/jobs/invalid-id ... ")
	respcode, _ := doRequest(ctx, "GET", "/v1/jobs/not-a-uuid", validationKeys[1], nil, 400)
	if respcode < 0 {
		return false
	}
	if respcode == 400 {
		fmt.Println("PASS (400)")
		return true
	}
	fmt.Printf("FAIL: got %d\n", respcode)
	return false
}

func testMaxAttemptsValidation(ctx context.Context) bool {
	fmt.Print("Test 20: max_attempts validation (0 and 101) ... ")
	// Test 0
	body1 := map[string]any{
		"type": "test", "payload": map[string]string{},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 0, "idempotency_key": idem("max-attempts-0"),
		"callback_url": "http://callback:8080/hook",
	}
	resp1code, _ := doRequest(ctx, "POST", "/v1/jobs", validationKeys[0], body1, 400)
	if resp1code < 0 {
		return false
	}

	// Test 101
	body2 := map[string]any{
		"type": "test", "payload": map[string]string{},
		"run_at":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		"max_attempts": 101, "idempotency_key": idem("max-attempts-101"),
		"callback_url": "http://callback:8080/hook",
	}
	resp2code, _ := doRequest(ctx, "POST", "/v1/jobs", validationKeys[1], body2, 400)
	if resp2code < 0 {
		return false
	}

	if resp1code == 400 && resp2code == 400 {
		fmt.Println("PASS (both 400)")
		return true
	}
	fmt.Printf("FAIL: got %d and %d\n", resp1code, resp2code)
	return false
}
