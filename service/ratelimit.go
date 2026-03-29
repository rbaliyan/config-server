package service

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// RateLimiter determines whether a request from a given client should be allowed.
type RateLimiter interface {
	Allow(clientID string) bool
}

// RateLimitOption configures the TokenBucketLimiter.
type RateLimitOption func(*rateLimitOptions)

type rateLimitOptions struct {
	rate             float64
	burst            int
	cleanupInterval  time.Duration
	clientIdentifier func(ctx context.Context) string
}

// WithRate sets the token refill rate (tokens per second).
func WithRate(r float64) RateLimitOption {
	return func(o *rateLimitOptions) {
		o.rate = r
	}
}

// WithBurst sets the maximum burst size.
func WithBurst(b int) RateLimitOption {
	return func(o *rateLimitOptions) {
		o.burst = b
	}
}

// WithCleanupInterval sets the interval for removing stale client entries.
func WithCleanupInterval(d time.Duration) RateLimitOption {
	return func(o *rateLimitOptions) {
		o.cleanupInterval = d
	}
}

// WithClientIdentifier sets a function to extract client identity from context.
func WithClientIdentifier(fn func(ctx context.Context) string) RateLimitOption {
	return func(o *rateLimitOptions) {
		o.clientIdentifier = fn
	}
}

type clientEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// TokenBucketLimiter implements RateLimiter using per-client token buckets.
type TokenBucketLimiter struct {
	mu               sync.Mutex
	clients          map[string]*clientEntry
	rate             rate.Limit
	burst            int
	cleanupInterval  time.Duration
	clientIdentifier func(ctx context.Context) string
	stopCleanup      chan struct{}
	cleanupDone      chan struct{}
}

// NewTokenBucketLimiter creates a new per-client token bucket rate limiter.
func NewTokenBucketLimiter(opts ...RateLimitOption) *TokenBucketLimiter {
	o := &rateLimitOptions{
		rate:            10,
		burst:           20,
		cleanupInterval: 5 * time.Minute,
		clientIdentifier: func(ctx context.Context) string {
			if p, ok := peer.FromContext(ctx); ok {
				return p.Addr.String()
			}
			return "unknown"
		},
	}
	for _, opt := range opts {
		opt(o)
	}

	l := &TokenBucketLimiter{
		clients:          make(map[string]*clientEntry),
		rate:             rate.Limit(o.rate),
		burst:            o.burst,
		cleanupInterval:  o.cleanupInterval,
		clientIdentifier: o.clientIdentifier,
		stopCleanup:      make(chan struct{}),
		cleanupDone:      make(chan struct{}),
	}

	go l.cleanupLoop()
	return l
}

// Allow reports whether a request from the given client should be permitted.
func (l *TokenBucketLimiter) Allow(clientID string) bool {
	l.mu.Lock()
	entry, ok := l.clients[clientID]
	if !ok {
		entry = &clientEntry{
			limiter: rate.NewLimiter(l.rate, l.burst),
		}
		l.clients[clientID] = entry
	}
	entry.lastSeen = time.Now()
	l.mu.Unlock()

	return entry.limiter.Allow()
}

// ClientID extracts the client identifier from the context using the configured function.
func (l *TokenBucketLimiter) ClientID(ctx context.Context) string {
	return l.clientIdentifier(ctx)
}

// Close stops the background cleanup goroutine.
func (l *TokenBucketLimiter) Close() {
	close(l.stopCleanup)
	<-l.cleanupDone
}

func (l *TokenBucketLimiter) cleanupLoop() {
	defer close(l.cleanupDone)
	ticker := time.NewTicker(l.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCleanup:
			return
		case <-ticker.C:
			l.cleanup()
		}
	}
}

func (l *TokenBucketLimiter) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	staleThreshold := time.Now().Add(-2 * l.cleanupInterval)
	for id, entry := range l.clients {
		if entry.lastSeen.Before(staleThreshold) {
			delete(l.clients, id)
		}
	}
}

// RateLimitInterceptor returns a unary interceptor that enforces rate limits.
func RateLimitInterceptor(limiter RateLimiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		clientID := extractClientID(ctx, limiter)
		if !limiter.Allow(clientID) {
			return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

// StreamRateLimitInterceptor returns a stream interceptor that enforces rate limits.
func StreamRateLimitInterceptor(limiter RateLimiter) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		clientID := extractClientID(ss.Context(), limiter)
		if !limiter.Allow(clientID) {
			return status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(srv, ss)
	}
}

// extractClientID gets the client ID using the limiter's ClientID method if available,
// otherwise falls back to the peer address.
func extractClientID(ctx context.Context, limiter RateLimiter) string {
	type clientIdentifier interface {
		ClientID(ctx context.Context) string
	}
	if ci, ok := limiter.(clientIdentifier); ok {
		return ci.ClientID(ctx)
	}
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}
