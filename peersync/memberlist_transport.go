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
// discover and gossip with each other directly over TCP/UDP.
//
// cfg is used to create the memberlist; its Delegate field is overwritten by
// the transport. Callers should set BindAddr, BindPort, and Name (defaults to
// the hostname) before passing cfg in. Use memberlist.DefaultLANConfig() as a
// starting point for local-area-network deployments.
//
// After construction, call Join with the address of at least one existing
// cluster node to enter the cluster. A single-node cluster requires no Join.
//
// The returned *MemberlistTransport satisfies both Transport and
// TransportHealthChecker and exposes Join, Members, and LocalNode for
// cluster management.
func NewMemberlistTransport(cfg *memberlist.Config) (*MemberlistTransport, error) {
	if cfg == nil {
		return nil, errors.New("peersync: memberlist config must not be nil")
	}
	t := &MemberlistTransport{cfg: cfg}
	cfg.Delegate = t
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

// MemberlistTransport is a peer-to-peer gossip Transport backed by HashiCorp
// memberlist. Create with NewMemberlistTransport.
type MemberlistTransport struct {
	cfg        *memberlist.Config
	ml         *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	subMu      sync.Mutex
	subscribed bool
	handler    func([]byte)
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
// to remote nodes. This satisfies the Transport loopback contract.
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

// Subscribe registers handler for all inbound messages. Only one active
// subscription is supported; a second call returns an error. handler is
// invoked from memberlist's internal goroutines and must not block for
// extended periods. The MemberlistTransport recovers from handler panics
// defensively via safeCall.
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

// Close gracefully leaves the cluster, waiting up to 2× the configured gossip
// interval for the leave message to propagate, then shuts down the memberlist.
func (t *MemberlistTransport) Close() error {
	leaveTimeout := 2 * t.cfg.GossipInterval
	if leaveTimeout <= 0 {
		leaveTimeout = 500 * time.Millisecond
	}
	_ = t.ml.Leave(leaveTimeout) //nolint:errcheck // best-effort leave on shutdown
	return t.ml.Shutdown()
}

// Health implements TransportHealthChecker. It returns a non-nil error when
// the SWIM protocol health score is degraded (at least one missed probe cycle).
// A score of 0 means the local node is fully healthy from the gossip layer's
// perspective.
func (t *MemberlistTransport) Health(_ context.Context) error {
	if score := t.ml.GetHealthScore(); score > 0 {
		return fmt.Errorf("peersync: memberlist health score degraded: %d", score)
	}
	return nil
}

// -- memberlist.Delegate implementation --

// NotifyMsg is called by memberlist when a remote node delivers a message.
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

// GetBroadcasts returns queued broadcast messages for piggybacking onto
// memberlist gossip packets.
func (t *MemberlistTransport) GetBroadcasts(overhead, limit int) [][]byte {
	return t.broadcasts.GetBroadcasts(overhead, limit)
}

func (t *MemberlistTransport) NodeMeta(_ int) []byte              { return nil }
func (t *MemberlistTransport) LocalState(_ bool) []byte           { return nil }
func (t *MemberlistTransport) MergeRemoteState(_ []byte, _ bool)  {}

// mlBroadcast implements memberlist.Broadcast for gossip queue entries.
type mlBroadcast struct {
	msg []byte
}

func (b *mlBroadcast) Invalidates(_ memberlist.Broadcast) bool { return false }
func (b *mlBroadcast) Message() []byte                          { return b.msg }
func (b *mlBroadcast) Finished()                                {}
