package peersync

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/rbaliyan/config"
)

// heartbeatMsg is the payload for MsgHeartbeat.
type heartbeatMsg struct {
	NodeID string `json:"id"`
	Addr   string `json:"addr"`
	Epoch  int64  `json:"epoch"`
}

// replicationMsg carries a single Set or Delete operation to peer nodes.
type replicationMsg struct {
	NodeID    string  `json:"src"`
	Namespace string  `json:"ns"`
	Key       string  `json:"k"`
	Type      uint8   `json:"t"` // 0 = set, 1 = delete
	Data      []byte  `json:"d,omitempty"`
	Codec     string  `json:"c,omitempty"`
	Version   int64   `json:"v,omitempty"`
}

const (
	replSet    uint8 = 0
	replDelete uint8 = 1
)

type nodeState struct {
	member   Member
	lastSeen time.Time
}

// SyncStore wraps any config.Store and adds consistent-hash ownership routing,
// write replication, and gossip-based failure detection.
//
// All nodes in a cluster must share the same Transport (e.g. the same Redis
// pub/sub channel). Writes are applied locally first, then broadcast to peers
// via replication messages. Reads are always served from the local store.
type SyncStore struct {
	local     config.Store
	ring      *Ring
	self      Member
	transport Transport
	opts      options

	nodesMu sync.RWMutex
	nodes   map[string]*nodeState

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ config.Store = (*SyncStore)(nil)

// New creates a SyncStore wrapping local. self identifies this node; transport
// is the shared pub/sub channel used for gossip and replication.
func New(local config.Store, self Member, transport Transport, opts ...Option) (*SyncStore, error) {
	if local == nil {
		return nil, config.ErrStoreNotConnected
	}
	if self.ID == "" {
		return nil, config.ErrInvalidKey
	}
	if transport == nil {
		return nil, config.ErrStoreNotConnected
	}
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &SyncStore{
		local:     local,
		ring:      newRing(o.vnodes),
		self:      self,
		transport: transport,
		opts:      o,
		nodes:     make(map[string]*nodeState),
	}, nil
}

// Ring returns the underlying Ring for direct inspection or pin/unpin operations.
func (s *SyncStore) Ring() *Ring { return s.ring }

// OwnerOf returns the member ID currently responsible for namespace according
// to the ring. Returns ("", false) when the cluster has no members yet.
func (s *SyncStore) OwnerOf(namespace string) (string, bool) {
	return s.ring.OwnerOf(namespace)
}

// Pin forces namespace to always resolve to nodeID, bypassing the hash ring.
// The override is gossiped to all peers so the ring converges cluster-wide.
func (s *SyncStore) Pin(ctx context.Context, namespace, nodeID string) error {
	s.ring.Pin(namespace, nodeID)
	return s.publishRingChange(ctx)
}

// Unpin removes a manual pin and restores hash-ring routing for namespace.
// The change is gossiped to all peers.
func (s *SyncStore) Unpin(ctx context.Context, namespace string) error {
	s.ring.Unpin(namespace)
	return s.publishRingChange(ctx)
}

// Connect connects the underlying store and joins the cluster by registering
// this node on the ring and starting heartbeat and failure-detection loops.
func (s *SyncStore) Connect(ctx context.Context) error {
	if err := s.local.Connect(ctx); err != nil {
		return err
	}
	bctx, cancel := context.WithCancel(context.Background())
	s.ctx = bctx
	s.cancel = cancel

	if err := s.transport.Subscribe(bctx, s.handleMessage); err != nil {
		cancel()
		return err
	}

	// Add self to ring and announce to the cluster.
	s.ring.Add(s.self)
	if err := s.publishRingChange(ctx); err != nil {
		cancel()
		return err
	}

	s.wg.Add(2)
	go s.heartbeatLoop()
	go s.failureDetectLoop()
	return nil
}

// Close leaves the cluster, stops background loops, and closes the underlying store.
func (s *SyncStore) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.transport.Close()
	return s.local.Close(ctx)
}

// Get returns the value from the local store.
func (s *SyncStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	return s.local.Get(ctx, namespace, key)
}

// Set writes to the local store and replicates the change to all peers.
func (s *SyncStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	v, err := s.local.Set(ctx, namespace, key, value)
	if err != nil {
		return nil, err
	}
	s.publishReplication(ctx, namespace, key, v)
	return v, nil
}

// Delete removes the key from the local store and replicates the deletion to peers.
func (s *SyncStore) Delete(ctx context.Context, namespace, key string) error {
	if err := s.local.Delete(ctx, namespace, key); err != nil {
		return err
	}
	s.publishDeletion(ctx, namespace, key)
	return nil
}

// Find queries the local store.
func (s *SyncStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	return s.local.Find(ctx, namespace, filter)
}

// Watch returns a channel of change events from the local store.
// Events include both locally-originated writes and writes replicated from peers.
func (s *SyncStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return s.local.Watch(ctx, filter)
}

// Health delegates to the underlying store's HealthChecker if it implements one.
func (s *SyncStore) Health(ctx context.Context) error {
	if hc, ok := s.local.(config.HealthChecker); ok {
		return hc.Health(ctx)
	}
	return nil
}

// --- replication publish ---

func (s *SyncStore) publishReplication(ctx context.Context, namespace, key string, v config.Value) {
	data, err := v.Marshal(ctx)
	if err != nil {
		return
	}
	rm := replicationMsg{
		NodeID:    s.self.ID,
		Namespace: namespace,
		Key:       key,
		Type:      replSet,
		Data:      data,
		Codec:     v.Codec(),
		Version:   v.Metadata().Version(),
	}
	payload, err := json.Marshal(rm)
	if err != nil {
		return
	}
	_ = s.transport.Publish(ctx, Message{Type: MsgReplication, Payload: payload})
}

func (s *SyncStore) publishDeletion(ctx context.Context, namespace, key string) {
	rm := replicationMsg{
		NodeID:    s.self.ID,
		Namespace: namespace,
		Key:       key,
		Type:      replDelete,
	}
	payload, err := json.Marshal(rm)
	if err != nil {
		return
	}
	_ = s.transport.Publish(ctx, Message{Type: MsgReplication, Payload: payload})
}

func (s *SyncStore) publishRingChange(ctx context.Context) error {
	snap := s.ring.Snapshot()
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return s.transport.Publish(ctx, Message{Type: MsgRingChange, Payload: data})
}

// --- message handling ---

func (s *SyncStore) handleMessage(msg Message) {
	switch msg.Type {
	case MsgHeartbeat:
		var hb heartbeatMsg
		if err := json.Unmarshal(msg.Payload, &hb); err != nil {
			return
		}
		s.handleHeartbeat(hb)
	case MsgRingChange:
		var state RingState
		if err := json.Unmarshal(msg.Payload, &state); err != nil {
			return
		}
		s.ring.Apply(state)
	case MsgReplication:
		var rm replicationMsg
		if err := json.Unmarshal(msg.Payload, &rm); err != nil {
			return
		}
		// Skip messages we originated to avoid re-applying our own writes.
		if rm.NodeID == s.self.ID {
			return
		}
		s.applyReplication(rm)
	}
}

func (s *SyncStore) handleHeartbeat(hb heartbeatMsg) {
	s.nodesMu.Lock()
	ns, exists := s.nodes[hb.NodeID]
	if !exists {
		ns = &nodeState{member: Member{ID: hb.NodeID, Addr: hb.Addr}}
		s.nodes[hb.NodeID] = ns
	}
	wasAbsent := ns.lastSeen.IsZero()
	ns.member.Addr = hb.Addr
	ns.lastSeen = time.Now()
	s.nodesMu.Unlock()

	if wasAbsent {
		// New node seen for the first time: add to ring and broadcast updated state.
		s.ring.Add(Member{ID: hb.NodeID, Addr: hb.Addr})
		_ = s.publishRingChange(s.ctx)
	}
}

func (s *SyncStore) applyReplication(rm replicationMsg) {
	ctx := s.ctx
	if rm.Type == replDelete {
		_ = s.local.Delete(ctx, rm.Namespace, rm.Key)
		return
	}
	v, err := config.NewValueFromBytes(ctx, rm.Data, rm.Codec,
		config.WithValueMetadata(rm.Version, time.Time{}, time.Time{}),
	)
	if err != nil {
		return
	}
	_, _ = s.local.Set(ctx, rm.Namespace, rm.Key, v)
}

// --- background loops ---

func (s *SyncStore) heartbeatLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			hb := heartbeatMsg{
				NodeID: s.self.ID,
				Addr:   s.self.Addr,
				Epoch:  s.ring.Epoch(),
			}
			data, err := json.Marshal(hb)
			if err != nil {
				continue
			}
			_ = s.transport.Publish(s.ctx, Message{Type: MsgHeartbeat, Payload: data})
		}
	}
}

func (s *SyncStore) failureDetectLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.detectFailures()
		}
	}
}

func (s *SyncStore) detectFailures() {
	deadline := time.Now().Add(-s.opts.failureTimeout)

	s.nodesMu.Lock()
	var dead []string
	for id, ns := range s.nodes {
		if id == s.self.ID {
			continue
		}
		if !ns.lastSeen.IsZero() && ns.lastSeen.Before(deadline) {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		delete(s.nodes, id)
	}
	s.nodesMu.Unlock()

	if len(dead) > 0 {
		for _, id := range dead {
			s.ring.Remove(id)
		}
		_ = s.publishRingChange(s.ctx)
	}
}
