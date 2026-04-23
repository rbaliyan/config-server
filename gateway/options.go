package gateway

import (
	"crypto/tls"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	dialOpts          []grpc.DialOption
	muxOpts           []runtime.ServeMuxOption
	secure            bool
	tlsConfig         *tls.Config
	heartbeatInterval time.Duration
	// eventBufferSize is the user-requested buffer size.
	//   nil     → unset, handler applies defaultEventBufferSize
	//   *0      → explicitly disabled (resumption unavailable)
	//   *N (>0) → ring-buffer size
	eventBufferSize  *int
	dashboardEnabled bool
	dashboardPath    string // default "/dashboard"
}

// Option configures the gateway handler.
type Option func(*options)

// WithDialOptions appends additional gRPC dial options.
// Only used with NewHandler (not NewInProcessHandler).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithMuxOptions sets the ServeMux options for the gateway.
func WithMuxOptions(opts ...runtime.ServeMuxOption) Option {
	return func(o *options) {
		o.muxOpts = append(o.muxOpts, opts...)
	}
}

// WithTLS enables TLS for the gRPC connection to the backend.
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

// WithHeartbeatInterval sets the interval for SSE heartbeat comments.
// Heartbeats keep the connection alive through proxies and load balancers.
// Default is 30 seconds. Values <= 0 are ignored.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.heartbeatInterval = d
		}
	}
}

const defaultHeartbeatInterval = 30 * time.Second

const defaultEventBufferSize = 500

const defaultDashboardPath = "/dashboard"

// WithEventBufferSize sets the number of recent watch events buffered for
// Last-Event-ID resumption. Clients that reconnect with the HTTP header
// "Last-Event-ID: N" (or via the browser EventSource automatic reconnect)
// will receive every buffered event with an id strictly greater than N
// before the live stream resumes.
//
// The buffer is a per-handler ring buffer. When it overflows, the oldest
// events are evicted. If a client's Last-Event-ID is older than the oldest
// still-buffered event, only the events still in the buffer are replayed —
// older events are silently lost. Clients that cannot tolerate gaps must
// reconcile by re-reading affected keys. Malformed or empty Last-Event-ID
// values result in no replay (the client joins the live stream from the
// current point).
//
// Default is 500. Set to 0 to disable buffering entirely (in which case
// Last-Event-ID is ignored and no replay occurs). Negative values are
// ignored and leave the previously configured size in place.
func WithEventBufferSize(n int) Option {
	return func(o *options) {
		if n < 0 {
			return
		}
		v := n
		o.eventBufferSize = &v
	}
}

// WithDashboard enables the embedded web UI dashboard, mounted at /dashboard/.
func WithDashboard() Option {
	return func(o *options) {
		o.dashboardEnabled = true
	}
}

// WithDashboardPath sets the mount path for the dashboard (default "/dashboard").
// The path must start with "/" and should not end with "/".
// Values that are empty or do not start with "/" are ignored.
func WithDashboardPath(path string) Option {
	return func(o *options) {
		if path != "" && path[0] == '/' {
			o.dashboardPath = path
		}
	}
}

// resolveEventBufferSize returns the concrete ring-buffer size.
// A nil pointer means the user did not call WithEventBufferSize and
// the default applies; an explicit *0 disables the buffer.
func resolveEventBufferSize(requested *int) int {
	if requested == nil {
		return defaultEventBufferSize
	}
	return *requested
}

// buildDialOpts constructs the gRPC dial options from configuration.
func (o *options) buildDialOpts() []grpc.DialOption {
	opts := make([]grpc.DialOption, 0, len(o.dialOpts)+1)

	// Credentials
	if o.secure {
		if o.tlsConfig != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.tlsConfig)))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))) // #nosec G402
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// User-provided options (can override defaults)
	opts = append(opts, o.dialOpts...)

	return opts
}
