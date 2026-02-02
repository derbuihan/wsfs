package retry

import (
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// Default retry configuration constants
const (
	DefaultMaxRetries    = 5
	DefaultInitialDelay  = 1 * time.Second
	DefaultMaxDelay      = 32 * time.Second
	DefaultBackoffFactor = 2.0
	DefaultJitter        = 0.2 // ±20%
)

// Config holds retry configuration
type Config struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
	Jitter        float64
}

// DefaultConfig returns the default retry configuration
func DefaultConfig() Config {
	return Config{
		MaxRetries:    DefaultMaxRetries,
		InitialDelay:  DefaultInitialDelay,
		MaxDelay:      DefaultMaxDelay,
		BackoffFactor: DefaultBackoffFactor,
		Jitter:        DefaultJitter,
	}
}

// IsRetryableStatus returns true if the status code should trigger a retry
func IsRetryableStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests, // 429
		http.StatusInternalServerError,  // 500
		http.StatusBadGateway,           // 502
		http.StatusServiceUnavailable,   // 503
		http.StatusGatewayTimeout:       // 504
		return true
	}
	return false
}

// CalculateDelay computes the delay for the given attempt with jitter
func (c Config) CalculateDelay(attempt int, retryAfter time.Duration) time.Duration {
	// Use Retry-After header if provided and within bounds
	if retryAfter > 0 && retryAfter <= c.MaxDelay {
		return retryAfter
	}

	// Calculate exponential backoff
	delay := float64(c.InitialDelay)
	for i := 0; i < attempt; i++ {
		delay *= c.BackoffFactor
	}

	// Cap at max delay
	if delay > float64(c.MaxDelay) {
		delay = float64(c.MaxDelay)
	}

	// Apply jitter: ±Jitter%
	if c.Jitter > 0 {
		jitter := delay * c.Jitter * (2*rand.Float64() - 1)
		delay += jitter
	}

	return time.Duration(delay)
}

// ParseRetryAfter parses the Retry-After header value
// Supports integer seconds format. HTTP date format returns 0 (not supported).
func ParseRetryAfter(header string) time.Duration {
	if header == "" {
		return 0
	}

	// Try parsing as seconds (most common format)
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}

	// HTTP date format not supported - return 0
	return 0
}
