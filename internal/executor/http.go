package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
)

const (
	callbackTimeout      = 10 * time.Second
	maxResponseBodyBytes = 1 << 20 // 1 MiB max response body
	maxRedirects         = 3
)

type HTTPExecutor struct{}

// NewHTTPExecutor builds an executor. Per-request clients are constructed in
// ExecuteJob with a pinned, SSRF-validating transport, so there is no shared
// client to keep here.
func NewHTTPExecutor() *HTTPExecutor {
	return &HTTPExecutor{}
}

func (e *HTTPExecutor) Execute(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	return e.ExecuteJob(ctx, callbackURL, payload, "", 0, validator.Config{})
}

func (e *HTTPExecutor) ExecuteJob(ctx context.Context, callbackURL string, payload []byte, jobID string, attempt int, validatorCfg validator.Config) (int, error) {
	// Single validation: CreateValidatingTransport validates once and pins
	// the resolved IPs in the dialer (DNS-rebinding protection). A separate
	// pre-validation pass would resolve DNS twice and reopen the
	// check-time/connect-time TOCTOU gap.
	transport, validated, err := validator.CreateValidatingTransport(ctx, callbackURL, validatorCfg)
	if err != nil {
		return 0, fmt.Errorf("callback URL validation failed: %w", err)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   callbackTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return http.ErrUseLastResponse
			}
			// Only method-preserving redirects (307/308) may be followed.
			// Go silently rewrites POST→GET on 301/302/303 and drops the
			// payload: delivering nothing and recording success would be
			// worse than failing loudly (and retrying). Detect the
			// downgrade by comparing methods instead of status codes.
			if req.Method != via[0].Method {
				return fmt.Errorf("refusing redirect that changes method %s→%s", via[0].Method, req.Method)
			}
			// The SSRF check covered only the original URL, and the
			// pinned dialer only knows the original IPs: only same-host
			// redirects are safe to follow. Anything else fails closed.
			if !equalHost(req.URL.Hostname(), validated.ValidatedHost) {
				return fmt.Errorf("refusing cross-host redirect to %q", req.URL.Hostname())
			}
			if _, err := validator.ValidateCallbackURL(req.Context(), req.URL.String(), validatorCfg); err != nil {
				return err
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

	// Drain at most 1 MiB to prevent OOM; error out on oversized bodies
	// instead of truncating silently, so a 100 MB "200 OK" fails loudly
	// (and retries) rather than recording a false success.
	n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if n > maxResponseBodyBytes {
		return resp.StatusCode, fmt.Errorf(
			"callback response body exceeds %d bytes",
			maxResponseBodyBytes,
		)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf(
			"callback returned status %d",
			resp.StatusCode,
		)
	}

	return resp.StatusCode, nil
}

// equalHost compares DNS names case-insensitively, ignoring a trailing dot
// (absolute FQDN form), so redirect-target checks cannot be bypassed with
// "Example.COM." style aliases.
func equalHost(a, b string) bool {
	normalize := func(s string) string {
		return strings.ToLower(strings.TrimSuffix(s, "."))
	}
	return normalize(a) == normalize(b)
}
