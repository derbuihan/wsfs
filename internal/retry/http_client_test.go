package retry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type errReadCloser struct{}

func (e errReadCloser) Read(p []byte) (int, error) { return 0, errors.New("read error") }
func (e errReadCloser) Close() error               { return nil }

func TestNewHTTPClient(t *testing.T) {
	client := NewHTTPClient(3*time.Second, Config{})
	if client == nil || client.client == nil {
		t.Fatal("expected client")
	}
	if client.client.Timeout != 3*time.Second {
		t.Fatalf("expected timeout 3s, got %v", client.client.Timeout)
	}
}

func TestHTTPClientDo_NonRetryable(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(2*time.Second, Config{MaxRetries: 2})
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestHTTPClientDo_RetrySuccess(t *testing.T) {
	calls := 0
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		if calls < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(2*time.Second, Config{MaxRetries: 3, InitialDelay: 0, MaxDelay: 0, BackoffFactor: 1, Jitter: 0})
	req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString("data"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	for _, body := range bodies {
		if body != "data" {
			t.Fatalf("unexpected body: %q", body)
		}
	}
}

func TestHTTPClientDo_RetryExhausted(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewHTTPClient(2*time.Second, Config{MaxRetries: 2, InitialDelay: 0, MaxDelay: 0, BackoffFactor: 1, Jitter: 0})
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	resp, err := client.Do(req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 response, got %v", resp)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestHTTPClientDo_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			cancel()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(2*time.Second, Config{MaxRetries: 1, InitialDelay: 50 * time.Millisecond, MaxDelay: 50 * time.Millisecond, BackoffFactor: 1, Jitter: 0})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	resp, err := client.Do(req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response on cancel")
	}
}

func TestHTTPClientDo_RequestBodyReadError(t *testing.T) {
	client := NewHTTPClient(2*time.Second, Config{})
	req, err := http.NewRequest(http.MethodPost, "http://example.invalid", errReadCloser{})
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	_, err = client.Do(req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "failed to read request body") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseRetryAfterFromResp(t *testing.T) {
	resp := &http.Response{Header: http.Header{"Retry-After": []string{"5"}}}
	if got := parseRetryAfterFromResp(resp); got != 5*time.Second {
		t.Fatalf("expected 5s, got %v", got)
	}
	if got := parseRetryAfterFromResp(nil); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}
