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

// MemberEventType classifies a cluster membership change delivered by
// MembershipTransport.
type MemberEventType uint8

const (
	// MemberJoined is fired when a node enters the cluster or updates its
	// address. Callers should call ring.Add with the new member details.
	MemberJoined MemberEventType = iota + 1
	// MemberLeft is fired when a node gracefully leaves or is declared dead
	// by the transport's own failure-detection protocol (e.g. SWIM missed
	// probes). Callers should call ring.Remove for the departing node.
	MemberLeft
)

// MemberEvent describes a single cluster membership change.
type MemberEvent struct {
	Type   MemberEventType
	Member Member
}

// MembershipTransport is an optional interface that transports may implement
// to expose native cluster membership events. When a Transport also implements
// MembershipTransport, SyncStore delegates ring membership management to the
// transport's own protocol (e.g. SWIM) instead of running its own
// heartbeat/failure-detection loops. This eliminates the duplicate liveness
// bookkeeping that would otherwise exist when the transport already has a
// built-in membership protocol.
//
// The MemberlistTransport implements this interface. The RedisTransport does
// not, so SyncStore falls back to its heartbeat/failure-detection loops when
// Redis is used as the transport.
type MembershipTransport interface {
	// SubscribeMembers registers a handler invoked whenever cluster membership
	// changes. SyncStore calls SubscribeMembers exactly once during Connect.
	// Only one active subscription is supported; a second call must return an
	// error. handler is called from the transport's internal goroutines and
	// must not block for extended periods. The transport is responsible for
	// recovering from handler panics.
	SubscribeMembers(ctx context.Context, handler func(MemberEvent)) error
}

// safeCall invokes fn(arg) and recovers from any panic so that a misbehaving
// handler cannot crash the transport's background goroutine.
func safeCall[T any](fn func(T), arg T) {
	defer func() { recover() }() //nolint:errcheck
	fn(arg)
}
