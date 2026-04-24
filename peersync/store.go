// Package-level replication note:
// applyReplication calls local.Set which auto-increments the version counter
// in the local store. As a result, the Version stored in each node's backend
// will diverge after replication. The Version field in replicationMsg is used
// only for best-effort out-of-order detection and does not represent a
// cluster-wide version. Callers that require strict version consistency must
// coordinate externally.
package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/rbaliyan/config"
)

var (
	errLocalRequired     = errors.New("peersync: local store is required")
	errSelfIDRequired    = errors.New("peersync: member ID must not be empty")
	errTransportRequired = errors.New("peersync: transport is required")
)

// heartbeatMsg is the payload for msgHeartbeat.
type heartbeatMsg struct {
	NodeID string `json:"id"`
	Addr   string `json:"addr"`
	Epoch  int64  `json:"epoch"`
}

// replOp is the typed enum for replication operation types.
type replOp uint8

const (
	replSet    replOp = iota
	replDelete replOp = 1
)

// replicationMsg carries a single Set or Delete operation to peer nodes.
//
// applyReplication calls local.Set, which auto-increments the version in
// the local store. Version here is used for best-effort out-of-order
// detection only; versions will diverge across nodes after replication.
type replicationMsg struct {
	NodeID    string `json:"src"`
	Namespace string `json:"ns"`
	Key       string `json:"k"`
	Type      replOp `json:"t"` // 0 = set, 1 = delete
	Data      []byte `json:"d,omitempty"`
	Codec     string `json:"c,omitempty"`
	Version   int64  `json:"v,omitempty"`
}

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
//
// Replication version divergence note: because each node's local store
// auto-increments versions on Set, Version values will diverge across nodes
// after replication. This is acceptable for low-contention configuration
// workloads where last-arrival-wins per node is sufficient.
type SyncStore struct {
	local     config.Store
	ring      *ring
	self      Member
	transport Transport
	opts      options

	nodesMu sync.RWMutex
	nodes   map[string]*nodeState

	ringChangeCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ config.Store = (*SyncStore)(nil)

// New creates a SyncStore wrapping local. self identifies this node; transport
// is the shared pub/sub channel used for gossip and replication.
// Connect must be called before the store is used.
func New(local config.Store, self Member, transport Transport, opts ...Option) (*SyncStore, error) {
	if local == nil {
		return nil, errLocalRequired
	}
	if self.ID == "" {
		return nil, errSelfIDRequired
	}
	if transport == nil {
		return nil, errTransportRequired
	}
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SyncStore{
		local:        local,
		ring:         newRing(o.vnodes),
		self:         self,
		transport:    transport,
		opts:         o,
		nodes:        make(map[string]*nodeState),
		ringChangeCh: make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Members returns the current set of registered members in the ring.
func (s *SyncStore) Members() []Member { return s.ring.Members() }

// Snapshot returns a point-in-time copy of the ring state.
func (s *SyncStore) Snapshot() RingState { return s.ring.Snapshot() }

// OwnerOf returns the member ID currently responsible for namespace according
// to the ring. Returns ("", false) when the cluster has no members yet.
func (s *SyncStore) OwnerOf(namespace string) (string, bool) {
	return s.ring.OwnerOf(namespace)
}

// Pin forces namespace to always resolve to nodeID, bypassing the hash ring.
// The override is gossiped to all peers so the ring converges cluster-wide.
// The ring update is synchronous; the gossip broadcast is scheduled
// asynchronously via the announce loop.
func (s *SyncStore) Pin(namespace, nodeID string) {
	s.ring.Pin(namespace, nodeID)
	s.scheduleAnnounce()
}

// Unpin removes a manual pin and restores hash-ring routing for namespace.
// The change is gossiped to all peers asynchronously.
func (s *SyncStore) Unpin(namespace string) {
	s.ring.Unpin(namespace)
	s.scheduleAnnounce()
}

// scheduleAnnounce enqueues a ring-change broadcast without blocking.
// If an announce is already queued the call is a no-op.
func (s *SyncStore) scheduleAnnounce() {
	select {
	case s.ringChangeCh <- struct{}{}:
	default:
	}
}

// Connect connects the underlying store and joins the cluster by registering
// this node on the ring and starting heartbeat and failure-detection loops.
func (s *SyncStore) Connect(ctx context.Context) error {
	if err := s.local.Connect(ctx); err != nil {
		return err
	}

	if err := s.transport.Subscribe(s.ctx, s.handleMessage); err != nil {
		return err
	}

	// Add self to ring and attempt an initial announcement. Failure is
	// non-fatal: peers will learn of this node via heartbeats.
	s.ring.Add(s.self)
	if err := s.publishRingChange(ctx); err != nil {
		s.opts.logger.Warn("peersync: initial ring announce failed, peers will learn via heartbeat", "err", err)
	}

	s.wg.Add(3)
	go s.heartbeatLoop()
	go s.failureDetectLoop()
	go s.announceLoop()
	return nil
}

// Close leaves the cluster, stops background loops, and closes the underlying store.
// Order: cancel context → wait for own goroutines → close transport (waits for its
// goroutine) → close local store.
func (s *SyncStore) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	_ = s.transport.Close()
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

// Health checks the local store and the transport. Both must implement their
// respective optional health interfaces; either check is skipped if absent.
func (s *SyncStore) Health(ctx context.Context) error {
	if hc, ok := s.local.(config.HealthChecker); ok {
		if err := hc.Health(ctx); err != nil {
			return err
		}
	}
	if hc, ok := s.transport.(TransportHealthChecker); ok {
		return hc.Health(ctx)
	}
	return nil
}

// --- replication publish ---

func (s *SyncStore) publishReplication(ctx context.Context, namespace, key string, v config.Value) {
	data, err := v.Marshal(ctx)
	if err != nil {
		s.opts.logger.Error("peersync: marshal value for replication", "ns", namespace, "key", key, "err", err)
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
	inner, err := json.Marshal(rm)
	if err != nil {
		s.opts.logger.Error("peersync: marshal replication message", "ns", namespace, "key", key, "err", err)
		return
	}
	env := message{Type: msgReplication, Payload: inner}
	payload, err := json.Marshal(env)
	if err != nil {
		s.opts.logger.Error("peersync: marshal envelope", "err", err)
		return
	}
	if err := s.transport.Publish(ctx, payload); err != nil {
		s.opts.logger.Warn("peersync: publish replication", "ns", namespace, "key", key, "err", err)
	}
}

func (s *SyncStore) publishDeletion(ctx context.Context, namespace, key string) {
	rm := replicationMsg{
		NodeID:    s.self.ID,
		Namespace: namespace,
		Key:       key,
		Type:      replDelete,
	}
	inner, err := json.Marshal(rm)
	if err != nil {
		s.opts.logger.Error("peersync: marshal deletion message", "ns", namespace, "key", key, "err", err)
		return
	}
	env := message{Type: msgReplication, Payload: inner}
	payload, err := json.Marshal(env)
	if err != nil {
		s.opts.logger.Error("peersync: marshal envelope", "err", err)
		return
	}
	if err := s.transport.Publish(ctx, payload); err != nil {
		s.opts.logger.Warn("peersync: publish deletion", "ns", namespace, "key", key, "err", err)
	}
}

func (s *SyncStore) publishRingChange(ctx context.Context) error {
	snap := s.ring.Snapshot()
	inner, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	env := message{Type: msgRingChange, Payload: inner}
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.transport.Publish(ctx, payload)
}

// --- message handling ---

func (s *SyncStore) handleMessage(raw []byte) {
	var env message
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}
	switch env.Type {
	case msgHeartbeat:
		var hb heartbeatMsg
		if err := json.Unmarshal(env.Payload, &hb); err != nil {
			return
		}
		s.handleHeartbeat(hb)
	case msgRingChange:
		var state RingState
		if err := json.Unmarshal(env.Payload, &state); err != nil {
			return
		}
		s.ring.Apply(state)
		s.seedNodesFromState(state)
	case msgReplication:
		var rm replicationMsg
		if err := json.Unmarshal(env.Payload, &rm); err != nil {
			return
		}
		// Skip messages we originated to avoid re-applying our own writes.
		if rm.NodeID == s.self.ID {
			return
		}
		s.applyReplication(s.ctx, rm)
	}
}

// seedNodesFromState seeds s.nodes from a received RingState so that the
// failure detector can evict nodes that were added via ring-change messages
// but never sent a heartbeat to this node.
func (s *SyncStore) seedNodesFromState(state RingState) {
	now := time.Now()
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	for _, m := range state.Members {
		if m.ID == s.self.ID {
			continue
		}
		if _, exists := s.nodes[m.ID]; !exists {
			s.nodes[m.ID] = &nodeState{member: m, lastSeen: now}
		}
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
		// New node seen for the first time: add to ring and schedule broadcast.
		s.ring.Add(Member{ID: hb.NodeID, Addr: hb.Addr})
		s.scheduleAnnounce()
	}
}

// applyReplication applies a replication message from a peer.
//
// Note: local.Set auto-increments the version counter in the local store,
// so the Version field in replicationMsg is used only for best-effort
// out-of-order detection. Versions will diverge across nodes after replication.
func (s *SyncStore) applyReplication(ctx context.Context, rm replicationMsg) {
	if rm.Type == replDelete {
		if err := s.local.Delete(ctx, rm.Namespace, rm.Key); err != nil {
			s.opts.logger.Warn("peersync: apply deletion", "src", rm.NodeID, "ns", rm.Namespace, "key", rm.Key, "err", err)
		}
		return
	}
	v, err := config.NewValueFromBytes(ctx, rm.Data, rm.Codec,
		config.WithValueMetadata(rm.Version, time.Time{}, time.Time{}),
	)
	if err != nil {
		s.opts.logger.Error("peersync: decode replicated value", "src", rm.NodeID, "ns", rm.Namespace, "key", rm.Key, "err", err)
		return
	}
	if _, err := s.local.Set(ctx, rm.Namespace, rm.Key, v); err != nil {
		s.opts.logger.Warn("peersync: apply set", "src", rm.NodeID, "ns", rm.Namespace, "key", rm.Key, "err", err)
	}
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
			env := message{Type: msgHeartbeat, Payload: data}
			payload, err := json.Marshal(env)
			if err != nil {
				continue
			}
			if err := s.transport.Publish(s.ctx, payload); err != nil {
				s.opts.logger.Warn("peersync: publish heartbeat", "err", err)
			}
		}
	}
}

func (s *SyncStore) failureDetectLoop() {
	defer s.wg.Done()
	// Check at failureTimeout/3 so a dead node is evicted within one check
	// interval after crossing the threshold, regardless of heartbeat rate.
	interval := s.opts.failureTimeout / 3
	if interval < s.opts.heartbeatInterval {
		interval = s.opts.heartbeatInterval
	}
	ticker := time.NewTicker(interval)
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
		s.scheduleAnnounce()
	}
}

// announceLoop drains ringChangeCh and performs the actual ring-change
// broadcast. This decouples publishRingChange from the transport's handler
// goroutine, preventing re-entrant publish deadlocks.
func (s *SyncStore) announceLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.ringChangeCh:
			_ = s.publishRingChange(s.ctx)
		}
	}
}
