package peersync

import "context"

// msgType identifies the purpose of a cluster message.
type msgType uint8

const (
	msgHeartbeat msgType = iota + 1 // heartbeatMsg payload
	msgRingChange                   // RingState payload
)

// message is the wire envelope for all cluster gossip traffic.
// JSON tags use single-character keys ("t", "p") to minimise wire size.
type message struct {
	Type    msgType `json:"t"`
	Payload []byte  `json:"p"`
}

// Transport is the pub/sub layer used for cluster gossip.
// All cluster nodes must share the same logical transport (e.g. the same
// Redis instance or channel).
type Transport interface {
	// Publish broadcasts payload to every subscriber, including the caller.
	// Loopback delivery is required: SyncStore uses the NodeID embedded in
	// each heartbeat to update its own liveness bookkeeping, so every node
	// must receive every message including its own.
	Publish(ctx context.Context, payload []byte) error

	// Subscribe registers handler for all inbound messages. SyncStore calls
	// Subscribe exactly once during Connect. Implementations must support
	// exactly one active subscription and should return an error if called
	// again. handler is invoked from a background goroutine owned by the
	// transport and must not block for extended periods. Implementations should
	// document whether they recover from handler panics; the built-in
	// RedisTransport recovers defensively so a misbehaving handler cannot crash
	// the subscriber goroutine.
	Subscribe(ctx context.Context, handler func([]byte)) error

	// Close releases transport resources.
	Close() error
}

// TransportHealthChecker is an optional interface that transports may implement
// to expose a health check. SyncStore.Health queries it when present.
type TransportHealthChecker interface {
	Health(ctx context.Context) error
}
