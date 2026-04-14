package service

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTokenBucketLimiter_AllowThenDeny(t *testing.T) {
	limiter := NewTokenBucketLimiter(
		WithRate(1),
		WithBurst(2),
		WithCleanupInterval(time.Hour),
	)
	defer limiter.Close()

	// Should allow up to burst
	if !limiter.Allow("client1") {
		t.Fatal("expected first request to be allowed")
	}
	if !limiter.Allow("client1") {
		t.Fatal("expected second request to be allowed (burst=2)")
	}

	// Third request should be denied (burst exhausted, rate too low for immediate refill)
	if limiter.Allow("client1") {
		t.Fatal("expected third request to be denied")
	}
}

func TestTokenBucketLimiter_PerClientIsolation(t *testing.T) {
	limiter := NewTokenBucketLimiter(
		WithRate(1),
		WithBurst(1),
		WithCleanupInterval(time.Hour),
	)
	defer limiter.Close()

	// Exhaust client1's bucket
	if !limiter.Allow("client1") {
		t.Fatal("expected client1 first request to be allowed")
	}
	if limiter.Allow("client1") {
		t.Fatal("expected client1 second request to be denied")
	}

	// client2 should still be allowed
	if !limiter.Allow("client2") {
		t.Fatal("expected client2 first request to be allowed")
	}
}

func TestTokenBucketLimiter_BurstAllowance(t *testing.T) {
	limiter := NewTokenBucketLimiter(
		WithRate(0.1), // Very slow refill
		WithBurst(5),
		WithCleanupInterval(time.Hour),
	)
	defer limiter.Close()

	allowed := 0
	for i := 0; i < 10; i++ {
		if limiter.Allow("client1") {
			allowed++
		}
	}

	if allowed != 5 {
		t.Fatalf("expected exactly 5 requests allowed (burst), got %d", allowed)
	}
}

func TestTokenBucketLimiter_CustomClientIdentifier(t *testing.T) {
	type ctxKey string
	limiter := NewTokenBucketLimiter(
		WithRate(1),
		WithBurst(1),
		WithCleanupInterval(time.Hour),
		WithClientIdentifier(func(ctx context.Context) string {
			if v, ok := ctx.Value(ctxKey("user")).(string); ok {
				return v
			}
			return "anonymous"
		}),
	)
	defer limiter.Close()

	ctx := context.WithValue(context.Background(), ctxKey("user"), "alice")
	clientID := limiter.ClientID(ctx)

	if clientID != "alice" {
		t.Fatalf("expected client ID 'alice', got %q", clientID)
	}

	// Exhaust alice's bucket
	limiter.Allow("alice")
	if limiter.Allow("alice") {
		t.Fatal("expected alice to be rate limited")
	}

	// bob should still be allowed
	if !limiter.Allow("bob") {
		t.Fatal("expected bob to be allowed")
	}
}

func TestTokenBucketLimiter_CleanupStaleEntries(t *testing.T) {
	limiter := NewTokenBucketLimiter(
		WithRate(10),
		WithBurst(10),
		WithCleanupInterval(50*time.Millisecond),
	)
	defer limiter.Close()

	// Create a client entry
	limiter.Allow("stale-client")

	// Verify client exists
	limiter.mu.Lock()
	if _, ok := limiter.clients["stale-client"]; !ok {
		limiter.mu.Unlock()
		t.Fatal("expected client entry to exist")
	}
	// Backdate the lastSeen to make it stale
	limiter.clients["stale-client"].lastSeen = time.Now().Add(-time.Hour)
	limiter.mu.Unlock()

	// Wait for cleanup to run
	time.Sleep(150 * time.Millisecond)

	limiter.mu.Lock()
	_, exists := limiter.clients["stale-client"]
	limiter.mu.Unlock()

	if exists {
		t.Fatal("expected stale client entry to be cleaned up")
	}
}

func TestTokenBucketLimiter_CloseStopsCleanup(t *testing.T) {
	limiter := NewTokenBucketLimiter(
		WithRate(10),
		WithBurst(10),
		WithCleanupInterval(10*time.Millisecond),
	)

	// Close should return promptly (cleanup goroutine exits)
	done := make(chan struct{})
	go func() {
		limiter.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Close() did not return within 1 second")
	}
}

func TestTokenBucketLimiter_DoubleClose(t *testing.T) {
	limiter := NewTokenBucketLimiter(
		WithRate(10),
		WithBurst(10),
		WithCleanupInterval(time.Hour),
	)

	// First close should succeed
	limiter.Close()

	// Second close should not panic
	limiter.Close()
}

func TestRateLimitInterceptor_AllowsRequest(t *testing.T) {
	limiter := &mockRateLimiter{allow: true}
	interceptor := RateLimitInterceptor(limiter)

	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil, nil, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected handler to be called")
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}
}

func TestRateLimitInterceptor_DeniesRequest(t *testing.T) {
	limiter := &mockRateLimiter{allow: false}
	interceptor := RateLimitInterceptor(limiter)

	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	}

	_, err := interceptor(context.Background(), nil, nil, handler)
	if err == nil {
		t.Fatal("expected error")
	}
	if called {
		t.Fatal("expected handler not to be called")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", st.Code())
	}
}

func TestStreamRateLimitInterceptor_DeniesRequest(t *testing.T) {
	limiter := &mockRateLimiter{allow: false}
	interceptor := StreamRateLimitInterceptor(limiter)

	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	}

	err := interceptor(nil, &mockRateLimitStream{ctx: context.Background()}, nil, handler)
	if err == nil {
		t.Fatal("expected error")
	}
	if called {
		t.Fatal("expected handler not to be called")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", st.Code())
	}
}

// mockRateLimiter is a simple mock for testing interceptors.
type mockRateLimiter struct {
	allow bool
}

func (m *mockRateLimiter) Allow(string) bool {
	return m.allow
}

// mockRateLimitStream implements grpc.ServerStream for testing.
type mockRateLimitStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockRateLimitStream) Context() context.Context {
	return m.ctx
}
