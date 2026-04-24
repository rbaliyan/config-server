package peersync

import "context"

// msgType identifies the purpose of a cluster message.
type msgType uint8

const (
	msgHeartbeat   msgType = iota + 1 // heartbeatMsg payload
	msgRingChange                      // RingState payload
	msgReplication                     // replicationMsg payload
)

// message is the wire envelope for all gossip and replication traffic.
type message struct {
	Type    msgType `json:"t"`
	Payload []byte  `json:"p"`
}

// Transport is the pub/sub layer used for gossip and replication.
// All cluster nodes must share the same logical transport (e.g. the same
// Redis instance or channel).
type Transport interface {
	// Publish broadcasts payload to every subscriber, including the caller.
	Publish(ctx context.Context, payload []byte) error

	// Subscribe registers handler for all inbound messages.
	// handler is invoked from a background goroutine owned by the transport;
	// it must not block for extended periods.
	// Subscribe must be called before the background loops start.
	Subscribe(ctx context.Context, handler func([]byte)) error

	// Close releases transport resources.
	Close() error
}

// TransportHealthChecker is an optional interface that transports may implement
// to expose a health check. SyncStore.Health queries it when present.
type TransportHealthChecker interface {
	Health(ctx context.Context) error
}
