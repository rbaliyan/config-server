package peersync

import (
	"log/slog"
	"time"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultFailureTimeout    = 15 * time.Second
)

type options struct {
	heartbeatInterval time.Duration
	failureTimeout    time.Duration
	vnodes            int
	logger            *slog.Logger
	publishTimeout    time.Duration
}

func defaultOptions() options {
	return options{
		heartbeatInterval: defaultHeartbeatInterval,
		failureTimeout:    defaultFailureTimeout,
		vnodes:            defaultVNodes,
		logger:            slog.Default(),
		publishTimeout:    defaultHeartbeatInterval, // per-publish deadline for background loops
	}
}

// Option configures a SyncStore.
type Option func(*options)

// WithHeartbeatInterval sets how often this node broadcasts its heartbeat.
// Values ≤ 0 are ignored. Default is 5 seconds.
// The per-publish timeout for background loop goroutines is automatically
// updated to match the new interval unless overridden with WithPublishTimeout.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.heartbeatInterval = d
			o.publishTimeout = d
		}
	}
}

// WithPublishTimeout overrides the per-call deadline applied to Publish in the
// heartbeat and announce background loops. When not set (or set to ≤ 0), the
// publish timeout tracks the heartbeat interval. Call WithPublishTimeout after
// WithHeartbeatInterval if you want an independent value.
func WithPublishTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.publishTimeout = d
		}
	}
}

// WithFailureTimeout sets how long a node's heartbeat can be absent before
// the node is considered failed and removed from the ring.
// Values ≤ 0 are ignored. Default is 15 seconds (3× the default heartbeat).
func WithFailureTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.failureTimeout = d
		}
	}
}

// WithVNodes sets the number of virtual ring points per member.
// Higher values improve distribution but increase ring rebuild cost.
// Values ≤ 0 are ignored. Default is 150.
func WithVNodes(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.vnodes = n
		}
	}
}

// WithLogger sets the structured logger used for publish errors and dropped
// replication messages. Defaults to slog.Default(). Pass slog.New(slog.NewTextHandler(io.Discard, nil))
// to silence all output.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}
