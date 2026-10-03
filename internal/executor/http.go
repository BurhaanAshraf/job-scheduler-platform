package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	callbackTimeout = 10 * time.Second
	maxBodyBytes    = 1 << 20 // 1 MiB: enough for status, prevents conn-reuse poisoning.
)

type HTTPExecutor struct {
	client *http.Client
}

func NewHTTPExecutor() *HTTPExecutor {
	return &HTTPExecutor{
		client: &http.Client{
			Timeout: callbackTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return http.ErrUseLastResponse
				}
				// Preserve method/body on redirect; default Go would turn
				// 301/302 POST into GET and silently drop the payload.
				return nil
			},
		},
	}
}

func (e *HTTPExecutor) Execute(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	return e.ExecuteJob(ctx, callbackURL, payload, "", 0)
}

func (e *HTTPExecutor) ExecuteJob(ctx context.Context, callbackURL string, payload []byte, jobID string, attempt int) (int, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		callbackURL,
		bytes.NewReader(payload),
	)
	if err != nil {
		return 0, fmt.Errorf("create callback request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "job-scheduler-worker/1.0")
	if jobID != "" {
		// At-least-once delivery: receivers MUST be idempotent.
		// The key lets them dedupe retries safely.
		req.Header.Set("Idempotency-Key", fmt.Sprintf("%s:%d", jobID, attempt))
		req.Header.Set("X-Job-ID", jobID)
		req.Header.Set("X-Job-Attempt", strconv.Itoa(attempt))
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute callback: %w", err)
	}

	defer resp.Body.Close()
	// Drain bounded body so the connection can be reused.
	_, _ = io.CopyN(io.Discard, resp.Body, maxBodyBytes)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf(
			"callback returned status %d",
			resp.StatusCode,
		)
	}

	return resp.StatusCode, nil
}
