package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

const callbackTimeout = 10 * time.Second

type HTTPExecutor struct {
	client *http.Client
}

func NewHTTPExecutor() *HTTPExecutor {
	return &HTTPExecutor{
		client: &http.Client{
			Timeout: callbackTimeout,
		},
	}
}

func (e *HTTPExecutor) Execute(ctx context.Context, callbackURL string, payload []byte) (int, error) {
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

	resp, err := e.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute callback: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf(
			"callback returned status %d",
			resp.StatusCode,
		)
	}

	return resp.StatusCode, nil
}
