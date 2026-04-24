package peersync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// NewMemberlistTransport creates a Transport backed by HashiCorp memberlist
// gossip. Unlike the Redis transport, no external broker is required — nodes
// discover and gossip with each other directly over TCP/UDP using the SWIM
// protocol.
//
// peerAddr is the application-layer address of this node (e.g. the gRPC
// address "host:9000") that peers use when dialling via PeerDialer. It is
// embedded in memberlist node metadata so joining peers learn the correct
// dial address; it is distinct from the memberlist gossip bind address.
//
// cfg is used to create the memberlist; its Delegate and Events fields are
// overwritten by the transport. Use memberlist.DefaultLANConfig() as a
// starting point and set BindAddr, BindPort, and Name before passing it in.
//
// After construction, call Join with the address of at least one existing
// cluster node to enter the cluster. A single-node cluster requires no Join.
//
// MemberlistTransport implements Transport, TransportHealthChecker, and
// MembershipTransport. When passed to peersync.New, SyncStore automatically
// uses native SWIM membership events instead of its own heartbeat/failure-
// detection loops, eliminating duplicate liveness bookkeeping.
func NewMemberlistTransport(cfg *memberlist.Config, peerAddr string) (*MemberlistTransport, error) {
	if cfg == nil {
		return nil, errors.New("peersync: memberlist config must not be nil")
	}
	t := &MemberlistTransport{cfg: cfg, peerAddr: peerAddr}
	cfg.Delegate = t
	cfg.Events = t
	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("peersync: memberlist create: %w", err)
	}
	t.ml = ml
	t.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return ml.NumMembers() },
		RetransmitMult: cfg.RetransmitMult,
	}
	return t, nil
}

var _ Transport              = (*MemberlistTransport)(nil)
var _ TransportHealthChecker = (*MemberlistTransport)(nil)
var _ MembershipTransport    = (*MemberlistTransport)(nil)

// MemberlistTransport is a peer-to-peer gossip Transport backed by HashiCorp
// memberlist. Create with NewMemberlistTransport.
type MemberlistTransport struct {
	cfg      *memberlist.Config
	ml       *memberlist.Memberlist
	peerAddr string

	broadcasts *memberlist.TransmitLimitedQueue

	subMu      sync.Mutex
	subscribed bool
	handler    func([]byte)

	memberMu         sync.Mutex
	memberSubscribed bool
	memberHandler    func(MemberEvent)

	closeOnce sync.Once
}

// Join connects to the cluster by contacting one or more peer addresses.
// Returns the number of nodes successfully contacted. Joining is not required
// for a single-node cluster.
func (t *MemberlistTransport) Join(peers []string) (int, error) {
	return t.ml.Join(peers)
}

// Members returns the current cluster members as seen by the local node,
// including itself.
func (t *MemberlistTransport) Members() []*memberlist.Node {
	return t.ml.Members()
}

// LocalNode returns the local memberlist node, which carries the bind address
// and port needed to share with peers for cluster joining.
func (t *MemberlistTransport) LocalNode() *memberlist.Node {
	return t.ml.LocalNode()
}

// Publish broadcasts payload to every cluster node. The local handler receives
// the payload synchronously (loopback) before it is queued for gossip delivery
// to remote nodes, satisfying the Transport loopback contract.
func (t *MemberlistTransport) Publish(_ context.Context, payload []byte) error {
	t.subMu.Lock()
	h := t.handler
	t.subMu.Unlock()
	if h != nil {
		safeCall(h, payload)
	}
	buf := make([]byte, len(payload))
	copy(buf, payload)
	t.broadcasts.QueueBroadcast(&mlBroadcast{msg: buf})
	return nil
}

// Subscribe registers handler for all inbound gossip messages. Only one active
// subscription is supported; a second call returns an error. handler is invoked
// from memberlist's internal goroutines and must not block for extended
// periods. Panics in handler are recovered defensively.
func (t *MemberlistTransport) Subscribe(_ context.Context, handler func([]byte)) error {
	t.subMu.Lock()
	defer t.subMu.Unlock()
	if t.subscribed {
		return errors.New("peersync: memberlist transport already has an active subscription")
	}
	t.subscribed = true
	t.handler = handler
	return nil
}

// SubscribeMembers registers a handler for native cluster membership events
// (join, leave, failure). SyncStore calls this during Connect when it detects
// MembershipTransport, replacing its own heartbeat/failure-detection loops
// with SWIM-protocol notifications. Only one active subscription is supported.
func (t *MemberlistTransport) SubscribeMembers(_ context.Context, handler func(MemberEvent)) error {
	t.memberMu.Lock()
	defer t.memberMu.Unlock()
	if t.memberSubscribed {
		return errors.New("peersync: memberlist transport already has an active membership subscription")
	}
	t.memberSubscribed = true
	t.memberHandler = handler
	return nil
}

// Close gracefully leaves the cluster, waiting up to 2× the configured gossip
// interval for the leave message to propagate, then shuts down the memberlist.
// Close is idempotent; subsequent calls are no-ops.
func (t *MemberlistTransport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		leaveTimeout := 2 * t.cfg.GossipInterval
		if leaveTimeout <= 0 {
			leaveTimeout = 500 * time.Millisecond
		}
		_ = t.ml.Leave(leaveTimeout) //nolint:errcheck // best-effort leave on shutdown
		err = t.ml.Shutdown()
	})
	return err
}

// Health implements TransportHealthChecker. It returns a non-nil error when
// the SWIM protocol health score is degraded (at least one missed probe cycle).
// A score of 0 means the local node is fully healthy.
func (t *MemberlistTransport) Health(_ context.Context) error {
	if score := t.ml.GetHealthScore(); score > 0 {
		return fmt.Errorf("peersync: memberlist health score degraded: %d", score)
	}
	return nil
}

// mlNodeToMember converts a memberlist.Node to a peersync.Member. The gRPC
// peer address is read from the node's metadata (set via NodeMeta); the
// memberlist gossip address is used as a fallback when metadata is absent.
func (t *MemberlistTransport) mlNodeToMember(n *memberlist.Node) Member {
	addr := string(n.Meta)
	if addr == "" {
		addr = n.Address()
	}
	return Member{ID: n.Name, Addr: addr}
}

func (t *MemberlistTransport) notifyMember(evt MemberEvent) {
	t.memberMu.Lock()
	h := t.memberHandler
	t.memberMu.Unlock()
	if h != nil {
		safeCall(h, evt)
	}
}

// -- memberlist.Delegate --

// NodeMeta embeds peerAddr so joining nodes learn the gRPC dial address.
func (t *MemberlistTransport) NodeMeta(_ int) []byte {
	return []byte(t.peerAddr)
}

// NotifyMsg is called by memberlist when a remote node delivers a gossip message.
func (t *MemberlistTransport) NotifyMsg(msg []byte) {
	t.subMu.Lock()
	h := t.handler
	t.subMu.Unlock()
	if h == nil {
		return
	}
	buf := make([]byte, len(msg))
	copy(buf, msg)
	safeCall(h, buf)
}

// GetBroadcasts returns queued broadcast messages for piggybacking onto gossip packets.
func (t *MemberlistTransport) GetBroadcasts(overhead, limit int) [][]byte {
	return t.broadcasts.GetBroadcasts(overhead, limit)
}

func (t *MemberlistTransport) LocalState(_ bool) []byte          { return nil }
func (t *MemberlistTransport) MergeRemoteState(_ []byte, _ bool) {}

// -- memberlist.EventDelegate --

// NotifyJoin is called when a new node joins or a known node is rediscovered.
// Maps to MemberJoined: ring.Add handles both new members and address updates.
func (t *MemberlistTransport) NotifyJoin(n *memberlist.Node) {
	t.notifyMember(MemberEvent{Type: MemberJoined, Member: t.mlNodeToMember(n)})
}

// NotifyLeave is called when a node gracefully leaves or is declared dead after
// missed SWIM probes. Maps to MemberLeft in both cases.
func (t *MemberlistTransport) NotifyLeave(n *memberlist.Node) {
	t.notifyMember(MemberEvent{Type: MemberLeft, Member: t.mlNodeToMember(n)})
}

// NotifyUpdate is called when a node's metadata changes (e.g. peerAddr update).
// Treated as a re-join so the ring reflects the updated address.
func (t *MemberlistTransport) NotifyUpdate(n *memberlist.Node) {
	t.notifyMember(MemberEvent{Type: MemberJoined, Member: t.mlNodeToMember(n)})
}

// mlBroadcast implements memberlist.Broadcast for gossip queue entries.
type mlBroadcast struct {
	msg []byte
}

func (b *mlBroadcast) Invalidates(_ memberlist.Broadcast) bool { return false }
func (b *mlBroadcast) Message() []byte                          { return b.msg }
func (b *mlBroadcast) Finished()                                {}
