package peersync

import "context"

// MsgType identifies the purpose of a cluster message.
type MsgType uint8

const (
	MsgHeartbeat   MsgType = iota + 1 // HeartbeatMsg payload
	MsgRingChange                      // RingState payload
	MsgReplication                     // ReplicationMsg payload
)

// Message is the wire envelope for all gossip and replication traffic.
type Message struct {
	Type    MsgType `json:"t"`
	Payload []byte  `json:"p"`
}

// Transport is the pub/sub layer used for gossip and replication.
// All cluster nodes must share the same logical transport (e.g. the same
// Redis instance or channel).
type Transport interface {
	// Publish broadcasts msg to every subscriber, including the caller.
	Publish(ctx context.Context, msg Message) error

	// Subscribe registers handler for all inbound messages.
	// handler is invoked from a background goroutine owned by the transport;
	// it must not block for extended periods.
	// Subscribe must be called before the background loops start.
	Subscribe(ctx context.Context, handler func(Message)) error

	// Close releases transport resources.
	Close() error
}
