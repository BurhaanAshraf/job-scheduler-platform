package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
)

const (
	callbackTimeout       = 10 * time.Second
	maxBodyBytes          = 1 << 20 // 1 MiB
	maxResponseBodyBytes  = 1 << 20 // 1 MiB max response body
	connectionTimeout     = 5 * time.Second
	tlsHandshakeTimeout   = 5 * time.Second
	responseHeaderTimeout = 10 * time.Second
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
				return nil
			},
		},
	}
}

func (e *HTTPExecutor) Execute(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	return e.ExecuteJob(ctx, callbackURL, payload, "", 0, validator.Config{})
}

func (e *HTTPExecutor) ExecuteJob(ctx context.Context, callbackURL string, payload []byte, jobID string, attempt int, validatorCfg validator.Config) (int, error) {
	// Validate the callback URL before making the request (SSRF + DNS rebinding protection)
	_, err := validator.ValidateCallbackURL(ctx, callbackURL, validatorCfg)
	if err != nil {
		return 0, fmt.Errorf("callback URL validation failed: %w", err)
	}

	// Create a transport with the validated IPs to prevent DNS rebinding
	transport, _, err := validator.CreateValidatingTransport(ctx, callbackURL, validatorCfg)
	if err != nil {
		return 0, fmt.Errorf("create validating transport: %w", err)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   callbackTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

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
		req.Header.Set("Idempotency-Key", fmt.Sprintf("%s:%d", jobID, attempt))
		req.Header.Set("X-Job-ID", jobID)
		req.Header.Set("X-Job-Attempt", strconv.Itoa(attempt))
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute callback: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	// Read response body with size limit to prevent OOM
	limitedBody := io.LimitReader(resp.Body, maxResponseBodyBytes)
	_, _ = io.Copy(io.Discard, limitedBody)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf(
			"callback returned status %d",
			resp.StatusCode,
		)
	}

	return resp.StatusCode, nil
}
