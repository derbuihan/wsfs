package retry

import (
	"net/http"
	"testing"
	"time"
)

func TestIsRetryableStatus(t *testing.T) {
	tests := []struct {
		status   int
		expected bool
	}{
		// Non-retryable status codes
		{http.StatusOK, false},                  // 200
		{http.StatusCreated, false},             // 201
		{http.StatusNoContent, false},           // 204
		{http.StatusBadRequest, false},          // 400
		{http.StatusUnauthorized, false},        // 401
		{http.StatusForbidden, false},           // 403
		{http.StatusNotFound, false},            // 404
		{http.StatusMethodNotAllowed, false},    // 405
		{http.StatusConflict, false},            // 409
		{http.StatusUnprocessableEntity, false}, // 422

		// Retryable status codes
		{http.StatusTooManyRequests, true},     // 429
		{http.StatusInternalServerError, true}, // 500
		{http.StatusBadGateway, true},          // 502
		{http.StatusServiceUnavailable, true},  // 503
		{http.StatusGatewayTimeout, true},      // 504
	}

	for _, tt := range tests {
		got := IsRetryableStatus(tt.status)
		if got != tt.expected {
			t.Errorf("IsRetryableStatus(%d) = %v, want %v", tt.status, got, tt.expected)
		}
	}
}

func TestCalculateDelay(t *testing.T) {
	// Test without jitter for predictable results
	cfg := Config{
		InitialDelay:  1 * time.Second,
		MaxDelay:      32 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        0, // Disable jitter
	}

	// Expected delays: 1s, 2s, 4s, 8s, 16s, 32s (capped)
	expected := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second, // capped at MaxDelay
		32 * time.Second, // still capped
	}

	for attempt, want := range expected {
		got := cfg.CalculateDelay(attempt, 0)
		if got != want {
			t.Errorf("CalculateDelay(%d, 0) = %v, want %v", attempt, got, want)
		}
	}
}

func TestCalculateDelayWithRetryAfter(t *testing.T) {
	cfg := Config{
		InitialDelay:  1 * time.Second,
		MaxDelay:      32 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        0,
	}

	// Retry-After within bounds should be used
	got := cfg.CalculateDelay(0, 5*time.Second)
	if got != 5*time.Second {
		t.Errorf("CalculateDelay(0, 5s) = %v, want 5s", got)
	}

	// Retry-After exceeding MaxDelay should fall back to exponential backoff
	got = cfg.CalculateDelay(0, 60*time.Second)
	if got != 1*time.Second {
		t.Errorf("CalculateDelay(0, 60s) = %v, want 1s (fallback)", got)
	}
}

func TestCalculateDelayWithJitter(t *testing.T) {
	cfg := Config{
		InitialDelay:  1 * time.Second,
		MaxDelay:      32 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        0.2, // ±20%
	}

	// Run multiple times to verify jitter produces varying results
	delays := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		delay := cfg.CalculateDelay(0, 0)
		delays[delay] = true

		// Verify delay is within expected range: 0.8s to 1.2s
		minDelay := 800 * time.Millisecond
		maxDelay := 1200 * time.Millisecond
		if delay < minDelay || delay > maxDelay {
			t.Errorf("CalculateDelay with jitter = %v, want between %v and %v", delay, minDelay, maxDelay)
		}
	}

	// Verify that jitter produces some variance (not all same values)
	if len(delays) < 5 {
		t.Errorf("Jitter should produce varying delays, got only %d unique values", len(delays))
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		header   string
		expected time.Duration
	}{
		{"", 0},
		{"5", 5 * time.Second},
		{"120", 120 * time.Second},
		{"0", 0},     // Zero should return 0
		{"-1", 0},    // Negative should return 0
		{"invalid", 0},
		{"1.5", 0},   // Float not supported
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0}, // HTTP date not supported
	}

	for _, tt := range tests {
		got := ParseRetryAfter(tt.header)
		if got != tt.expected {
			t.Errorf("ParseRetryAfter(%q) = %v, want %v", tt.header, got, tt.expected)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxRetries != DefaultMaxRetries {
		t.Errorf("DefaultConfig().MaxRetries = %d, want %d", cfg.MaxRetries, DefaultMaxRetries)
	}
	if cfg.InitialDelay != DefaultInitialDelay {
		t.Errorf("DefaultConfig().InitialDelay = %v, want %v", cfg.InitialDelay, DefaultInitialDelay)
	}
	if cfg.MaxDelay != DefaultMaxDelay {
		t.Errorf("DefaultConfig().MaxDelay = %v, want %v", cfg.MaxDelay, DefaultMaxDelay)
	}
	if cfg.BackoffFactor != DefaultBackoffFactor {
		t.Errorf("DefaultConfig().BackoffFactor = %v, want %v", cfg.BackoffFactor, DefaultBackoffFactor)
	}
	if cfg.Jitter != DefaultJitter {
		t.Errorf("DefaultConfig().Jitter = %v, want %v", cfg.Jitter, DefaultJitter)
	}
}
