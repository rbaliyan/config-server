package peersync

import "time"

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultFailureTimeout    = 15 * time.Second
)

type options struct {
	heartbeatInterval time.Duration
	failureTimeout    time.Duration
	vnodes            int
}

func defaultOptions() options {
	return options{
		heartbeatInterval: defaultHeartbeatInterval,
		failureTimeout:    defaultFailureTimeout,
		vnodes:            defaultVNodes,
	}
}

// Option configures a SyncStore.
type Option func(*options)

// WithHeartbeatInterval sets how often this node broadcasts its heartbeat.
// Values ≤ 0 are ignored. Default is 5 seconds.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.heartbeatInterval = d
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
