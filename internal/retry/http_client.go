package retry

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"wsfs/internal/logging"
)

// HTTPClient wraps http.Client with retry logic for transient errors
type HTTPClient struct {
	client *http.Client
	config Config
}

// NewHTTPClient creates a new retryable HTTP client
func NewHTTPClient(timeout time.Duration, config Config) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{Timeout: timeout},
		config: config,
	}
}

// Do performs an HTTP request with retry logic for retryable status codes.
// The request body must be replayable (will be reset on retry).
// Returns the response and any error encountered.
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	var lastErr error
	var lastResp *http.Response

	// Save request body for retry (if present)
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body.Close()
	}

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate delay with Retry-After header if available
			delay := c.config.CalculateDelay(attempt-1, parseRetryAfterFromResp(lastResp))
			logging.Debugf("Retry attempt %d/%d after %v for %s %s",
				attempt, c.config.MaxRetries, delay, req.Method, req.URL.Path)

			// Wait before retry, respecting context cancellation
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(delay):
			}

			// Close previous response body to avoid leaking connections
			if lastResp != nil && lastResp.Body != nil {
				io.Copy(io.Discard, lastResp.Body)
				lastResp.Body.Close()
			}
		}

		// Reset request body for retry
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		resp, err := c.client.Do(req)
		if err != nil {
			// Network errors are retryable
			lastErr = err
			lastResp = nil
			continue
		}

		// Check if status code is retryable
		if !IsRetryableStatus(resp.StatusCode) {
			return resp, nil
		}

		// Retryable status code - save for next iteration
		lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		lastResp = resp
	}

	// All retries exhausted
	// Return last response if available (caller may want to inspect status/body)
	if lastResp != nil {
		return lastResp, lastErr
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// parseRetryAfterFromResp extracts Retry-After header from response
func parseRetryAfterFromResp(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	return ParseRetryAfter(resp.Header.Get("Retry-After"))
}
