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
	dialer            PeerDialer
	errHandler        func(error)
	ownershipStore    OwnershipStore
}

func defaultOptions() options {
	return options{
		heartbeatInterval: defaultHeartbeatInterval,
		failureTimeout:    defaultFailureTimeout,
		vnodes:            defaultVNodes,
		logger:            slog.Default(),
		// publishTimeout is set to heartbeatInterval lazily in New after all
		// options have been applied, so WithHeartbeatInterval does not silently
		// overwrite a WithPublishTimeout that was applied first.
	}
}

// Option configures a SyncStore.
type Option func(*options)

// WithHeartbeatInterval sets how often this node broadcasts its heartbeat.
// Values ≤ 0 are ignored. Default is 5 seconds.
//
// When no explicit WithPublishTimeout is set, the publish timeout defaults to
// the heartbeat interval. The two options are independent and may be set in
// any order.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.heartbeatInterval = d
		}
	}
}

// WithPublishTimeout overrides the per-call deadline applied to Publish in the
// heartbeat and announce background loops. When not set, the publish timeout
// defaults to the heartbeat interval. A value ≤ 0 is silently ignored (treated
// as "not set"), so passing 0 does not disable the timeout — it keeps the
// current default. The two options are independent and may be set in any order.
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

// WithLogger sets the structured logger used for non-fatal background errors:
// heartbeat publish failures, ring-change publish failures, and malformed
// inbound gossip messages. Defaults to slog.Default(). Pass
// slog.New(slog.NewTextHandler(io.Discard, nil)) to silence all log output.
// A nil value is ignored. When WithErrorHandler is set, the error handler and
// the logger are mutually exclusive for gossip errors: the handler receives
// the error and the logger is not consulted. The logger is still used for
// non-gossip warnings (e.g. the initial ring announce failure logged inside
// Connect).
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithPeerDialer sets the dialer used to forward operations to the ring owner
// when this node does not own the target namespace. Passing nil clears any
// previously configured dialer; without a dialer, non-owned operations return
// ErrNotOwner and the caller is responsible for routing.
func WithPeerDialer(d PeerDialer) Option {
	return func(o *options) {
		o.dialer = d
	}
}

// WithOwnershipStore configures a persistent store for namespace ownership
// records. When set, Connect reloads this node's owned namespaces on startup
// and Claim/Unclaim persist changes before updating the ring. Without this
// option ownership is in-memory only and is lost on restart. A nil value is
// ignored.
func WithOwnershipStore(os OwnershipStore) Option {
	return func(o *options) {
		if os != nil {
			o.ownershipStore = os
		}
	}
}

// WithErrorHandler sets a callback invoked for non-fatal gossip-layer errors:
// malformed inbound messages and transport publish failures. When set it
// replaces the default logger warning, so the caller owns all error reporting
// for these events (e.g. incrementing a metric counter). A nil value is
// ignored.
func WithErrorHandler(fn func(error)) Option {
	return func(o *options) {
		if fn != nil {
			o.errHandler = fn
		}
	}
}
