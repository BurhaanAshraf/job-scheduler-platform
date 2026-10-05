package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/health"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/logger"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func TestOpenAPIContract(t *testing.T) {
	// Load OpenAPI spec from embedded file
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData([]byte(openAPISpec))
	if err != nil {
		t.Fatalf("failed to load OpenAPI spec: %v", err)
	}
	if err := doc.Validate(loader.Context); err != nil {
		t.Fatalf("OpenAPI spec validation failed: %v", err)
	}

	// Setup test dependencies
	dsn := "postgres://burhaan:testpass123@localhost:5432/job_scheduler?sslmode=disable"
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping db: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to ping redis: %v", err)
	}

	// Clean up
	redisClient.Del(ctx, stream.ReadyStream)

	// Insert test API key
	apiKey := "test-openapi-key"
	// Compute actual SHA256 hash
	hashBytes := sha256.Sum256([]byte(apiKey))
	hash := hex.EncodeToString(hashBytes[:])
	_, err = pool.Exec(ctx, `
		INSERT INTO api_keys (id, client_name, hashed_key, created_at)
		VALUES (gen_random_uuid(), 'openapi-test', $1, NOW())
		ON CONFLICT (hashed_key) DO NOTHING
	`, hash)
	if err != nil {
		t.Fatalf("failed to insert api key: %v", err)
	}

	// Build the router
	log := logger.New("test")
	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)
	limiter := ratelimit.New(redisClient, 60, 60*1000*1000*1000)
	handler := NewHandler(jobRepo, cronRepo, redisClient, log)

	dbPinger := pinger{pool}
	redisPinger := redisPinger{redisClient}
	healthChecker := health.NewChecker(dbPinger, redisPinger, 2*time.Second)

	router := Server(log, handler, NewHealthHandler(healthChecker), limiter)

	// Test cases for each endpoint
	testCases := []struct {
		name           string
		method         string
		path           string
		body           interface{}
		expectedStatus int
		validate       func(*testing.T, *http.Response)
	}{
		{
			name:           "GET /healthz",
			method:         "GET",
			path:           "/healthz",
			expectedStatus: 200,
			validate: func(t *testing.T, resp *http.Response) {
				var result map[string]string
				json.NewDecoder(resp.Body).Decode(&result)
				if result["status"] != "ok" {
					t.Errorf("expected status=ok, got %v", result["status"])
				}
			},
		},
		{
			name:           "GET /metrics",
			method:         "GET",
			path:           "/metrics",
			expectedStatus: 200,
			validate: func(t *testing.T, resp *http.Response) {
				if resp.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
					t.Logf("Content-Type: %s", resp.Header.Get("Content-Type"))
				}
			},
		},
		{
			name:   "POST /v1/jobs - missing auth",
			method: "POST",
			path:   "/v1/jobs",
			body: map[string]interface{}{
				"type": "test", "payload": map[string]string{},
				"run_at": "2020-01-01T00:00:00Z", "max_attempts": 1,
				"idempotency_key": "test-1", "callback_url": "http://example.com/hook",
			},
			expectedStatus: 401,
			validate:       nil,
		},
		{
			name:   "POST /v1/jobs - invalid payload",
			method: "POST",
			path:   "/v1/jobs",
			body: map[string]interface{}{
				"type": "", "payload": map[string]string{},
				"run_at": "2020-01-01T00:00:00Z", "max_attempts": 1,
				"idempotency_key": "test-2", "callback_url": "http://example.com/hook",
			},
			expectedStatus: 400,
			validate:       validateErrorResponse,
		},
		{
			name:   "POST /v1/jobs - valid",
			method: "POST",
			path:   "/v1/jobs",
			body: map[string]interface{}{
				"type": "test", "payload": map[string]string{"msg": "hello"},
				"run_at": "2020-01-01T00:00:00Z", "max_attempts": 3,
				// Unique per run: jobs persist in the shared test DB, so a
				// fixed key replays as 200 on reruns instead of 201.
				"idempotency_key": "openapi-test-valid-1-" + uuid.NewString(),
				"callback_url":    "http://example.com/hook",
			},
			expectedStatus: 201,
			validate:       validateJobCreatedResponse,
		},
		{
			name:           "GET /v1/jobs - missing auth",
			method:         "GET",
			path:           "/v1/jobs",
			expectedStatus: 401,
			validate:       nil,
		},
		{
			name:           "GET /v1/dead-letters - missing auth",
			method:         "GET",
			path:           "/v1/dead-letters",
			expectedStatus: 401,
			validate:       nil,
		},
		{
			name:   "POST /v1/cron-jobs - missing auth",
			method: "POST",
			path:   "/v1/cron-jobs",
			body: map[string]interface{}{
				"cron_expression": "0 * * * *",
				"job_template": map[string]interface{}{
					"type": "test", "payload": map[string]string{}, "max_attempts": 1,
					"callback_url": "http://example.com/hook",
				},
			},
			expectedStatus: 401,
			validate:       nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.body != nil {
				bodyBytes, _ := json.Marshal(tc.body)
				req = httptest.NewRequest(tc.method, tc.path, bytes.NewReader(bodyBytes))
				req.Header.Set("Content-Type", "application/json")
			} else {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}

			// Add auth for authenticated tests
			if tc.expectedStatus != 401 && tc.expectedStatus != 429 {
				req.Header.Set("Authorization", "Bearer "+apiKey)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tc.expectedStatus {
				t.Errorf("expected status %d, got %d: %s", tc.expectedStatus, w.Code, w.Body.String())
				return
			}

			if tc.validate != nil {
				resp := w.Result()
				tc.validate(t, resp)
			}
		})
	}
}

func validateErrorResponse(t *testing.T, resp *http.Response) {
	var errResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	errorObj, ok := errResp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("response missing 'error' object")
	}
	if _, ok := errorObj["code"].(string); !ok {
		t.Fatal("error missing 'code' field")
	}
	if _, ok := errorObj["message"].(string); !ok {
		t.Fatal("error missing 'message' field")
	}
}

func validateJobCreatedResponse(t *testing.T, resp *http.Response) {
	var jobResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		t.Fatalf("failed to decode job response: %v", err)
	}
	id, ok := jobResp["id"].(string)
	if !ok {
		t.Fatal("response missing 'id' field")
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Errorf("id is not a valid UUID: %s", id)
	}
}

// pinger wraps a client to implement health.Pinger
type pinger struct {
	client interface{ Ping(context.Context) error }
}

func (p pinger) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// TestResponseHeaders validates that error responses include required headers
func TestResponseHeaders(t *testing.T) {
	dsn := "postgres://burhaan:testpass123@localhost:5432/job_scheduler?sslmode=disable"
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer redisClient.Close()

	// Insert test API key
	apiKey := "test-openapi-key"
	// Compute actual SHA256 hash
	hashBytes := sha256.Sum256([]byte(apiKey))
	hash := hex.EncodeToString(hashBytes[:])
	_, err = pool.Exec(ctx, `
		INSERT INTO api_keys (id, client_name, hashed_key, created_at)
		VALUES (gen_random_uuid(), 'openapi-test', $1, NOW())
		ON CONFLICT (hashed_key) DO NOTHING
	`, hash)
	if err != nil {
		t.Fatalf("failed to insert api key: %v", err)
	}

	log := logger.New("test")
	jobRepo := repository.NewJobRepository(pool)
	cronRepo := repository.NewCronJobRepository(pool)
	limiter := ratelimit.New(redisClient, 1, 60*1000*1000*1000) // 1 req/min for testing
	handler := NewHandler(jobRepo, cronRepo, redisClient, log)

	dbPinger := pinger{pool}
	redisPinger := redisPinger{client: redisClient}
	healthChecker := health.NewChecker(dbPinger, redisPinger, 2*time.Second)

	router := Server(log, handler, NewHealthHandler(healthChecker), limiter)

	// Test 429 includes Retry-After header
	req := httptest.NewRequest("POST", "/v1/jobs", bytes.NewReader([]byte(`{
		"type":"test","payload":{},"run_at":"2020-01-01T00:00:00Z",
		"max_attempts":1,"idempotency_key":"ratelimit-test","callback_url":"http://example.com/hook"
	}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req) // First request - allowed

	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req) // Second request - should be 429

	if w2.Code != 429 {
		t.Fatalf("expected 429, got %d", w2.Code)
	}

	retryAfter := w2.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("429 response missing Retry-After header")
	}
	t.Logf("Retry-After: %s", retryAfter)
}
