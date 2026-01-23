package client

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type options struct {
	// Connection options
	dialOpts       []grpc.DialOption
	connectTimeout time.Duration
	secure         bool
	tlsConfig      *tls.Config

	// Retry/resilience options
	maxRetries     int
	retryBackoff   time.Duration
	maxBackoff     time.Duration
	enableCircuit  bool
	circuitTimeout time.Duration

	// Watch options
	watchBufferSize    int
	watchReconnect     bool
	watchReconnectWait time.Duration

	// Health check options
	healthCheckInterval time.Duration
	keepaliveTime       time.Duration
	keepaliveTimeout    time.Duration

	// Observability
	onStateChange func(state ConnState)
	onWatchError  func(err error)
}

// ConnState represents the connection state.
type ConnState int

const (
	ConnStateDisconnected ConnState = iota
	ConnStateConnecting
	ConnStateConnected
	ConnStateReconnecting
	ConnStateClosed
)

func (s ConnState) String() string {
	switch s {
	case ConnStateDisconnected:
		return "disconnected"
	case ConnStateConnecting:
		return "connecting"
	case ConnStateConnected:
		return "connected"
	case ConnStateReconnecting:
		return "reconnecting"
	case ConnStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

func defaultOptions() *options {
	return &options{
		connectTimeout:      10 * time.Second,
		maxRetries:          3,
		retryBackoff:        100 * time.Millisecond,
		maxBackoff:          5 * time.Second,
		watchBufferSize:     100,
		watchReconnect:      true,
		watchReconnectWait:  time.Second,
		healthCheckInterval: 30 * time.Second,
		keepaliveTime:       30 * time.Second,
		keepaliveTimeout:    10 * time.Second,
		circuitTimeout:      30 * time.Second,
	}
}

// Option configures the RemoteStore.
type Option func(*options)

// WithDialOptions appends additional gRPC dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithConnectTimeout sets the timeout for initial connection.
func WithConnectTimeout(d time.Duration) Option {
	return func(o *options) {
		o.connectTimeout = d
	}
}

// WithTLS enables TLS with the given config.
// If config is nil, uses system default TLS config.
func WithTLS(config *tls.Config) Option {
	return func(o *options) {
		o.secure = true
		o.tlsConfig = config
	}
}

// WithInsecure explicitly enables insecure connections (no TLS).
// This should only be used for development/testing.
func WithInsecure() Option {
	return func(o *options) {
		o.secure = false
		o.tlsConfig = nil
	}
}

// WithRetry configures retry behavior for failed operations.
func WithRetry(maxRetries int, initialBackoff, maxBackoff time.Duration) Option {
	return func(o *options) {
		o.maxRetries = maxRetries
		o.retryBackoff = initialBackoff
		o.maxBackoff = maxBackoff
	}
}

// WithCircuitBreaker enables circuit breaker for resilience.
func WithCircuitBreaker(timeout time.Duration) Option {
	return func(o *options) {
		o.enableCircuit = true
		o.circuitTimeout = timeout
	}
}

// WithWatchBufferSize sets the buffer size for watch event channels.
// Default is 100. Set to 0 for unbuffered (backpressure).
func WithWatchBufferSize(size int) Option {
	return func(o *options) {
		o.watchBufferSize = size
	}
}

// WithWatchReconnect enables automatic reconnection for watches.
// Default is enabled with 1 second wait between attempts.
func WithWatchReconnect(enabled bool, waitTime time.Duration) Option {
	return func(o *options) {
		o.watchReconnect = enabled
		o.watchReconnectWait = waitTime
	}
}

// WithHealthCheck configures periodic health checks.
func WithHealthCheck(interval time.Duration) Option {
	return func(o *options) {
		o.healthCheckInterval = interval
	}
}

// WithKeepalive configures gRPC keepalive parameters.
func WithKeepalive(time, timeout time.Duration) Option {
	return func(o *options) {
		o.keepaliveTime = time
		o.keepaliveTimeout = timeout
	}
}

// WithStateCallback sets a callback for connection state changes.
func WithStateCallback(fn func(ConnState)) Option {
	return func(o *options) {
		o.onStateChange = fn
	}
}

// WithWatchErrorCallback sets a callback for watch errors.
// This is called when a watch encounters an error (before reconnection).
func WithWatchErrorCallback(fn func(error)) Option {
	return func(o *options) {
		o.onWatchError = fn
	}
}

// buildDialOpts constructs the gRPC dial options from configuration.
func (o *options) buildDialOpts() []grpc.DialOption {
	opts := make([]grpc.DialOption, 0, len(o.dialOpts)+3)

	// Credentials
	if o.secure {
		if o.tlsConfig != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.tlsConfig)))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Keepalive
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                o.keepaliveTime,
		Timeout:             o.keepaliveTimeout,
		PermitWithoutStream: true,
	}))

	// User-provided options (can override defaults)
	opts = append(opts, o.dialOpts...)

	return opts
}
