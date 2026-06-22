package client

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rbaliyan/config"
)

// BenchmarkIsNonRetryable measures the retry-classification check that runs on
// every failed RPC attempt before deciding whether to back off and retry. It
// walks an errors.Is chain and finally an errors.As, so cost depends on where a
// given error matches. Sub-benchmarks cover an early sentinel match, a late
// sentinel match, a wrapped sentinel, a typed PermissionDeniedError matched via
// errors.As, and a retryable (unmatched) error that falls through everything.
func BenchmarkIsNonRetryable(b *testing.B) {
	cases := []struct {
		name string
		err  error
	}{
		{"NotFound", config.ErrNotFound},
		{"StoreClosed", config.ErrStoreClosed},
		{"Wrapped", fmt.Errorf("call failed: %w", config.ErrInvalidKey)},
		{"PermissionDenied", &PermissionDeniedError{Message: "denied"}},
		{"Retryable", errors.New("connection reset by peer")},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = isNonRetryable(tc.err)
			}
		})
	}
}

// BenchmarkIsCircuitOpen measures the circuit-breaker gate consulted before
// every RPC. The check takes circuitMu on each call, so this quantifies the
// per-call lock overhead. The closed-circuit path is the common case; the
// open path additionally compares the open timestamp against the timeout.
func BenchmarkIsCircuitOpen(b *testing.B) {
	store, err := NewRemoteStore("passthrough:///bench", WithCircuitBreaker(5, 30*time.Second))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Closed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = store.isCircuitOpen()
		}
	})

	b.Run("Open", func(b *testing.B) {
		// Trip the breaker: threshold consecutive failures opens it.
		for i := 0; i < 5; i++ {
			store.recordFailure()
		}
		b.ReportAllocs()
		for b.Loop() {
			_ = store.isCircuitOpen()
		}
	})
}
