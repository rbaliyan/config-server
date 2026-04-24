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
// WrittenAt is the Unix-nanosecond wall-clock time at the originating node.
// applyReplication uses it for last-write-wins (LWW) tiebreaking: a message
// is skipped if the local copy has a strictly newer UpdatedAt timestamp.
// When WrittenAt is zero (messages from older peers), the check is skipped
// and the write is applied unconditionally (safe degradation).
//
// Note: local.Set auto-increments the version in the local store so Version
// values diverge across nodes; Version is retained here for observability.
type replicationMsg struct {
	NodeID    string `json:"src"`
	Namespace string `json:"ns"`
	Key       string `json:"k"`
	Type      replOp `json:"t"` // 0 = set, 1 = delete
	Data      []byte `json:"d,omitempty"`
	Codec     string `json:"c,omitempty"`
	Version   int64  `json:"v,omitempty"`
	WrittenAt int64  `json:"wt,omitempty"` // Unix nanoseconds, for LWW tiebreak
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
	// evicted tracks nodes removed by the failure detector with their eviction
	// time. seedNodesFromState refuses to resurrect a node within 2× the
	// failure timeout after eviction, preventing gossip replay from
	// indefinitely deferring a dead node's removal.
	evicted map[string]time.Time

	// ringChangeCh has capacity 1 so that multiple rapid ring mutations
	// coalesce into a single gossip broadcast (non-blocking send drops the
	// second signal when one is already queued).
	ringChangeCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ config.Store = (*SyncStore)(nil)
var _ config.HealthChecker = (*SyncStore)(nil)

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
		evicted:      make(map[string]time.Time),
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
		WrittenAt: v.Metadata().UpdatedAt().UnixNano(),
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
		if s.ring.Apply(state) {
			// After every apply, remove greylisted nodes that the incoming
			// state may have re-introduced. This ensures evictions propagate
			// even when a peer's ring snapshot predates the eviction.
			s.scrubGreylisted()
		}
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
		// Use WithoutCancel so an in-flight apply is not abandoned when
		// the store begins shutting down; local.Close is called only after
		// transport.Close, which waits for this goroutine to exit.
		s.applyReplication(context.WithoutCancel(s.ctx), rm)
	}
}

// scrubGreylisted removes any currently-greylisted nodes from the ring after
// an Apply, preventing gossip replays from resurrecting recently evicted nodes.
func (s *SyncStore) scrubGreylisted() {
	now := time.Now()
	grace := 2 * s.opts.failureTimeout
	s.nodesMu.RLock()
	var toRemove []string
	for id, evictedAt := range s.evicted {
		if now.Sub(evictedAt) < grace {
			toRemove = append(toRemove, id)
		}
	}
	s.nodesMu.RUnlock()
	for _, id := range toRemove {
		s.ring.Remove(id)
	}
}

// seedNodesFromState seeds s.nodes from a received RingState so that the
// failure detector can evict nodes that were added via ring-change messages
// but never sent a heartbeat to this node.
//
// Nodes present in the evicted greylist are skipped for 2× the failure timeout
// to prevent gossip replays from indefinitely resurrecting dead nodes.
func (s *SyncStore) seedNodesFromState(state RingState) {
	now := time.Now()
	grace := 2 * s.opts.failureTimeout
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	for _, m := range state.Members {
		if m.ID == s.self.ID {
			continue
		}
		if evictedAt, ok := s.evicted[m.ID]; ok {
			if now.Sub(evictedAt) < grace {
				continue // still within greylist window; do not resurrect
			}
			delete(s.evicted, m.ID) // grace period expired; allow re-admission
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
// It calls s.local.Set directly — not s.Set — so the write is not re-published
// to the transport and cannot produce a replication echo.
//
// Note: local.Set auto-increments the version counter in the local store,
// so the Version field in replicationMsg is used only for best-effort
// out-of-order detection. Versions will diverge across nodes after replication.
func (s *SyncStore) applyReplication(ctx context.Context, rm replicationMsg) {
	if rm.Type == replDelete {
		err := s.local.Delete(ctx, rm.Namespace, rm.Key)
		// ErrNotFound is expected when the delete arrives after a re-create
		// or when the key never existed on this node — not a divergence.
		if err != nil && !config.IsNotFound(err) {
			s.opts.logger.Error("peersync: apply deletion — key may diverge until re-deleted",
				"src", rm.NodeID, "ns", rm.Namespace, "key", rm.Key, "err", err)
		}
		return
	}

	// LWW tiebreak: if the local copy is strictly newer, skip this write.
	// WrittenAt == 0 means the message came from an older peer; apply unconditionally.
	if rm.WrittenAt > 0 {
		writtenAt := time.Unix(0, rm.WrittenAt)
		if cur, err := s.local.Get(ctx, rm.Namespace, rm.Key); err == nil {
			if updatedAt := cur.Metadata().UpdatedAt(); !updatedAt.IsZero() && updatedAt.After(writtenAt) {
				return // local copy is newer; discard the stale replication
			}
		}
	}

	writtenAt := time.Time{}
	if rm.WrittenAt > 0 {
		writtenAt = time.Unix(0, rm.WrittenAt)
	}
	v, err := config.NewValueFromBytes(ctx, rm.Data, rm.Codec,
		config.WithValueMetadata(rm.Version, writtenAt, writtenAt),
	)
	if err != nil {
		s.opts.logger.Error("peersync: decode replicated value — message dropped",
			"src", rm.NodeID, "ns", rm.Namespace, "key", rm.Key, "err", err)
		return
	}
	if _, err := s.local.Set(ctx, rm.Namespace, rm.Key, v); err != nil {
		s.opts.logger.Error("peersync: apply set — key will diverge until re-written",
			"src", rm.NodeID, "ns", rm.Namespace, "key", rm.Key, "err", err)
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
			pubCtx, cancel := context.WithTimeout(s.ctx, s.opts.publishTimeout)
			if err := s.transport.Publish(pubCtx, payload); err != nil {
				s.opts.logger.Warn("peersync: publish heartbeat", "err", err)
			}
			cancel()
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
	now := time.Now()
	deadline := now.Add(-s.opts.failureTimeout)
	greylistCutoff := now.Add(-2 * s.opts.failureTimeout)

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
		s.evicted[id] = now
	}
	// Prune expired greylist entries to bound map growth.
	for id, t := range s.evicted {
		if t.Before(greylistCutoff) {
			delete(s.evicted, id)
		}
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
			pubCtx, cancel := context.WithTimeout(s.ctx, s.opts.publishTimeout)
			if err := s.publishRingChange(pubCtx); err != nil {
				s.opts.logger.Warn("peersync: publish ring change", "err", err)
			}
			cancel()
		}
	}
}
