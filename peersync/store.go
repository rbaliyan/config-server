// Package peersync wraps a config.Store with consistent-hash namespace
// ownership and gossip-based cluster membership.
//
// Each node in the cluster holds its own backing store (e.g. a SQLite-backed
// config-server). The ring maps each namespace to an owner; only the owner
// node stores data for that namespace. When a node receives a read or write
// for a namespace it does not own, it transparently forwards the operation to
// the owner via PeerDialer. This partitions data across nodes rather than
// replicating it.
//
// # Glossary
//
// Ring: the consistent-hash ring that maps namespace names to member nodes.
//
// Pin: a transient override that routes a specific namespace to a fixed node,
// bypassing the hash ring. Pins propagate via gossip and remain active as
// long as at least one node in the cluster is alive and broadcasting, but
// they are not written to durable storage — they are lost when all nodes
// restart. Use Pin for temporary routing changes.
//
// Claim: a persistent override backed by OwnershipStore (when configured).
// On restart, Claimed namespaces are re-pinned before the first gossip
// broadcast, so ownership survives restarts without operator intervention.
// Use Claim for stable, long-lived ownership assignments.
//
// Overrides: the set of active Pin/Claim entries on the ring. These are
// included in every ring-change gossip broadcast so all peers converge to
// the same routing table.
//
// # Alias routing
//
// The *Alias methods (SetAlias, GetAlias, DeleteAlias) route by hashing the
// full alias path string as if it were a namespace. Note that the full string
// is hashed — not just its prefix — so "app" and "app/db.host" are distinct
// ring keys and may resolve to different owner nodes. To guarantee co-location,
// keep alias paths under the same namespace prefix as their targets
// (e.g. alias "payments/db.host" resolves on the same node as namespace
// "payments").
//
// # Ownership persistence
//
// When an OwnershipStore is configured (WithOwnershipStore), each node
// persists its owned namespaces to durable storage. On restart, ownership is
// reloaded from the store and re-announced before the first gossip broadcast,
// so ownership survives restarts without operator intervention.
//
// # Dead-owner handling
//
// When a namespace's recorded owner becomes unreachable, writes return
// ErrNamespaceReadOnly. Reads fall back to the local store. Call Claim to
// transfer ownership to a live node and re-enable writes.
//
// If no PeerDialer is configured (WithPeerDialer), non-owned operations return
// ErrNotOwner so the caller can route to the correct node using OwnerOf.
//
// # Failure detection
//
// Each node publishes a heartbeat on the transport. A node absent for longer
// than the failure timeout is removed from the ring and the removal is
// gossiped to all peers.
//
// # Ring convergence
//
// Every ring-change message carries the full ring state and a monotonically
// increasing epoch. Nodes apply a received state only when its epoch exceeds
// the local epoch, so the ring converges to the globally highest-epoch view.
package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
)

var (
	errLocalRequired     = errors.New("peersync: store is required")
	errSelfIDRequired    = errors.New("peersync: member ID must not be empty")
	errTransportRequired = errors.New("peersync: transport is required")

	// ErrAlreadyConnected is returned by Connect when the store has already
	// been connected. SyncStore is single-use: Close followed by Connect is
	// not supported. Create a new SyncStore to reconnect.
	ErrAlreadyConnected = errors.New("peersync: already connected")

	// ErrNotOwner is returned by CRUD operations when this node does not own
	// the target namespace and no PeerDialer is configured. The caller should
	// call OwnerOf to find the current owner and route the request to that node.
	ErrNotOwner = errors.New("peersync: this node does not own the namespace")
)

// See also: ErrNamespaceReadOnly in ownership.go — returned for writes when the
// recorded owner is unreachable.

// heartbeatMsg is the payload for msgHeartbeat.
type heartbeatMsg struct {
	NodeID string `json:"id"`
	Addr   string `json:"addr"`
}

// nodeState tracks liveness of a peer between heartbeats.
type nodeState struct {
	member   Member
	lastSeen time.Time
}

// SyncStore wraps a config.Store and adds consistent-hash ownership routing
// and gossip-based failure detection.
//
// Each node stores data only for its owned namespaces. CRUD operations for
// non-owned namespaces are forwarded to the owner via PeerDialer when one is
// configured, or return ErrNotOwner otherwise.
//
// When an OwnershipStore is configured, owned namespaces are reloaded on
// Connect so ownership survives restarts. Namespaces whose recorded owner is
// unreachable return ErrNamespaceReadOnly for writes until Claim transfers
// ownership to a live node.
//
// All methods are safe for concurrent use. Call Close when done to stop
// background goroutines and release transport and store resources.
//
// Watch is served from the local store only; clients that need to watch a
// namespace owned by another node should connect directly to that node.
type SyncStore struct {
	local          config.Store
	ring           *ring
	self           Member
	transport      Transport
	dialer         PeerDialer
	ownershipStore OwnershipStore
	opts           options

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

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	connected  atomic.Bool
	closeOnce  sync.Once
}

var _ config.Store = (*SyncStore)(nil)
var _ config.HealthChecker = (*SyncStore)(nil)

// New creates a SyncStore wrapping store. self identifies this node; transport
// is the shared pub/sub channel used for gossip. Connect must be called before
// the store is used.
func New(store config.Store, self Member, transport Transport, opts ...Option) (*SyncStore, error) {
	if store == nil {
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
	// Derive publish timeout from heartbeat interval when not explicitly set.
	if o.publishTimeout == 0 {
		o.publishTimeout = o.heartbeatInterval
	}
	// Warn when the failure timeout is too tight relative to the heartbeat
	// interval; less than 3× means a slow heartbeat round-trip can cause
	// spurious evictions. Skip when the transport provides native membership
	// events (MembershipTransport) since the heartbeat/failure-detection loops
	// are not started in that case.
	if _, hasMembership := transport.(MembershipTransport); !hasMembership {
		if o.failureTimeout < 3*o.heartbeatInterval {
			o.logger.Warn("peersync: failureTimeout < 3×heartbeatInterval; may cause spurious evictions",
				"heartbeatInterval", o.heartbeatInterval,
				"failureTimeout", o.failureTimeout,
			)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SyncStore{
		local:          store,
		ring:           newRing(o.vnodes),
		self:           self,
		transport:      transport,
		dialer:         o.dialer,
		ownershipStore: o.ownershipStore,
		opts:           o,
		nodes:          make(map[string]*nodeState),
		evicted:        make(map[string]time.Time),
		ringChangeCh:   make(chan struct{}, 1),
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// Members returns the current set of registered members in the ring.
// The returned slice is a copy; callers may mutate it freely.
func (s *SyncStore) Members() []Member { return s.ring.Members() }

// Snapshot returns a point-in-time copy of the ring state.
// The returned value is a copy; callers may mutate it freely.
func (s *SyncStore) Snapshot() RingState { return s.ring.Snapshot() }

// OwnerOf returns the member ID recorded as responsible for namespace in the
// ring. Returns ("", false) when the cluster has no members yet.
//
// The returned ID may be a pin/claim override rather than the hash-ring
// result. It reflects the current routing table entry, not whether the node
// is currently reachable. Use HasMember to check liveness before acting on
// the result.
func (s *SyncStore) OwnerOf(namespace string) (string, bool) {
	return s.ring.OwnerOf(namespace)
}

// Pin forces namespace to always resolve to nodeID, bypassing the hash ring.
// Returns an error when nodeID is not this node and not a current ring member,
// preventing silent dead-owner state from a mistyped target. The returned
// error is not a sentinel; use string comparison only if you need to
// distinguish it (prefer HasMember to pre-validate the target instead).
// The override is gossiped to all peers; the broadcast is asynchronous.
func (s *SyncStore) Pin(namespace, nodeID string) error {
	if nodeID != s.self.ID && !s.ring.Has(nodeID) {
		return fmt.Errorf("peersync: pin target %q is not a ring member", nodeID)
	}
	s.ring.Pin(namespace, nodeID)
	s.scheduleAnnounce()
	return nil
}

// HasMember reports whether the node with the given ID is currently in the ring.
func (s *SyncStore) HasMember(id string) bool { return s.ring.Has(id) }

// Unpin removes a manual pin and restores hash-ring routing for namespace.
// The change is gossiped to all peers asynchronously.
//
// Note: Unpin only removes the in-memory ring override. If the namespace was
// registered with Claim (which also writes to OwnershipStore), calling Unpin
// instead of Unclaim leaves the DB record intact. On the next restart, Claim
// is replayed from the DB and the pin reappears. Use Unclaim to fully remove
// a persistent ownership record.
func (s *SyncStore) Unpin(namespace string) {
	s.ring.Unpin(namespace)
	s.scheduleAnnounce()
}

// Claim makes this node the persistent owner of namespace. If an
// OwnershipStore is configured the record is written to the DB before the
// ring is updated, so ownership survives a restart. The ring change is
// gossiped asynchronously.
//
// Claim overrides any existing transient Pin for the namespace.
//
// Claim should be called to take over a namespace whose previous owner has
// died (ErrNamespaceReadOnly). After a successful Claim, writes are accepted
// again and routed to this node.
func (s *SyncStore) Claim(ctx context.Context, namespace string) error {
	if s.ownershipStore != nil {
		if err := s.ownershipStore.SaveOwner(ctx, namespace, s.self.ID); err != nil {
			return fmt.Errorf("peersync: persist ownership of %q: %w", namespace, err)
		}
	}
	s.ring.Pin(namespace, s.self.ID)
	s.scheduleAnnounce()
	return nil
}

// Unclaim removes this node's persistent ownership of namespace. The DB
// record is deleted (if an OwnershipStore is configured) and hash-ring
// routing is restored. The change is gossiped asynchronously.
func (s *SyncStore) Unclaim(ctx context.Context, namespace string) error {
	if s.ownershipStore != nil {
		if err := s.ownershipStore.DeleteOwner(ctx, namespace); err != nil {
			return fmt.Errorf("peersync: remove ownership of %q: %w", namespace, err)
		}
	}
	s.ring.Unpin(namespace)
	s.scheduleAnnounce()
	return nil
}

// scheduleAnnounce enqueues a ring-change broadcast without blocking.
func (s *SyncStore) scheduleAnnounce() {
	select {
	case s.ringChangeCh <- struct{}{}:
	default:
	}
}

// Connect joins the cluster. It performs the following steps in order:
//  1. Connects the underlying local store.
//  2. Subscribes to the transport for inbound gossip.
//  3. Registers this node on the consistent-hash ring.
//  4. Reloads persisted ownership from OwnershipStore (if configured) and
//     re-pins each owned namespace before the first broadcast.
//  5. Publishes an initial ring-change announcement (failure is non-fatal).
//  6. Membership management (one of two modes):
//     a. If the transport implements MembershipTransport: subscribes to native
//        membership events and starts only the announce loop. The transport's
//        own protocol (e.g. SWIM) drives ring membership — no heartbeat or
//        failure-detection loops are started.
//     b. Otherwise: starts the heartbeat, failure-detection, and announce
//        background loops (Redis-transport mode).
//
// Connect is not idempotent: calling it more than once returns
// ErrAlreadyConnected. If Connect returns an error after step 2 or later, all
// acquired resources are released and the SyncStore must not be reused; create
// a new instance to retry.
func (s *SyncStore) Connect(ctx context.Context) error {
	if !s.connected.CompareAndSwap(false, true) {
		return ErrAlreadyConnected
	}

	if err := s.local.Connect(ctx); err != nil {
		return err
	}

	if err := s.transport.Subscribe(s.ctx, s.handleMessage); err != nil {
		s.cancel()
		_ = s.local.Close(ctx)
		return err
	}

	s.ring.Add(s.self)

	// Restore persisted ownership before the first announcement so peers
	// learn the full ownership state in the initial ring-change broadcast.
	if s.ownershipStore != nil {
		namespaces, err := s.ownershipStore.LoadOwned(ctx, s.self.ID)
		if err != nil {
			s.cancel()
			_ = s.transport.Close()
			_ = s.local.Close(ctx)
			return fmt.Errorf("peersync: load owned namespaces: %w", err)
		}
		for _, ns := range namespaces {
			s.ring.Pin(ns, s.self.ID)
		}
	}

	// Attempt an initial announcement. Failure is non-fatal: peers will learn
	// of this node via heartbeats.
	if err := s.publishRingChange(ctx); err != nil {
		s.opts.logger.Warn("peersync: initial ring announce failed, peers will learn via heartbeat", "err", err)
	}

	if mt, ok := s.transport.(MembershipTransport); ok {
		// Transport provides native membership events (e.g. SWIM via memberlist).
		// Subscribe to them and skip the heartbeat/failure-detection loops.
		if err := mt.SubscribeMembers(s.ctx, s.handleMemberEvent); err != nil {
			s.cancel()
			_ = s.transport.Close()
			_ = s.local.Close(ctx)
			return fmt.Errorf("peersync: subscribe membership events: %w", err)
		}
		s.wg.Add(1)
		go s.announceLoop()
	} else {
		s.wg.Add(3)
		go s.heartbeatLoop()
		go s.failureDetectLoop()
		go s.announceLoop()
	}
	return nil
}

// Close gracefully leaves the cluster, stops background loops, and closes the
// underlying store. Shutdown proceeds in two phases:
//
// Graceful leave (transport still open):
//  1. Remove self from the ring.
//  2. Publish a best-effort leave announcement over the (still-open) transport
//     so peers learn of the departure immediately rather than waiting out the
//     failure timeout.
//
// Shutdown (transport then store):
//  3. Cancel context — stops the heartbeat, failure-detection, and announce loops.
//  4. Close transport — drains any in-flight inbound messages before proceeding.
//  5. Wait for background goroutines.
//  6. Close the local store.
//
// Close is idempotent; subsequent calls are no-ops and return nil.
func (s *SyncStore) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		// Announce departure before stopping so peers converge quickly.
		s.ring.Remove(s.self.ID)
		leaveCtx, leaveCancel := context.WithTimeout(context.Background(), s.opts.publishTimeout)
		_ = s.publishRingChange(leaveCtx) // best-effort
		leaveCancel()

		if s.cancel != nil {
			s.cancel()
		}
		_ = s.transport.Close()
		s.wg.Wait()
		err = s.local.Close(ctx)
	})
	return err
}

// Get returns the value for key in namespace. If this node does not own
// namespace the request is forwarded to the owner via PeerDialer. When the
// recorded owner is unreachable, or when the ring has no members yet, the
// local store is queried as a read-only fallback. If a live remote owner
// exists and no PeerDialer is configured, returns ErrNotOwner.
func (s *SyncStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	store, err := s.ownerStore(namespace, false)
	if err != nil {
		return nil, err
	}
	return store.Get(ctx, namespace, key)
}

// Set writes the value to the owner of namespace. If this node is the owner
// the write goes to the local store; otherwise it is forwarded via PeerDialer.
// Returns ErrNotOwner when no PeerDialer is configured and this node is not
// the owner, or when the ring has no members yet (reads on an empty ring fall
// back to the local store, but writes always require a known owner).
// Returns ErrNamespaceReadOnly when the recorded owner is unreachable; call
// Claim to transfer ownership before writing.
func (s *SyncStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	store, err := s.ownerStore(namespace, true)
	if err != nil {
		return nil, err
	}
	return store.Set(ctx, namespace, key, value)
}

// Delete removes the key from the owner of namespace. Forwarding, ErrNotOwner,
// and ErrNamespaceReadOnly behaviour mirrors Set.
func (s *SyncStore) Delete(ctx context.Context, namespace, key string) error {
	store, err := s.ownerStore(namespace, true)
	if err != nil {
		return err
	}
	return store.Delete(ctx, namespace, key)
}

// Find queries the owner of namespace. Forwarding, empty-ring local fallback,
// and the ErrNotOwner-when-no-dialer behaviour all mirror Get.
func (s *SyncStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	store, err := s.ownerStore(namespace, false)
	if err != nil {
		return nil, err
	}
	return store.Find(ctx, namespace, filter)
}

// Watch returns a channel of change events from the local store only.
// Changes written to namespaces owned by other nodes are not visible here;
// connect directly to the owning node to watch those namespaces.
func (s *SyncStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return s.local.Watch(ctx, filter)
}

// Health checks the backing store and the transport. Returns nil when neither
// implements the health interface (always-healthy default). Both checks are
// optional and skipped when the respective type does not implement it.
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

// ownerStore returns the config.Store that should handle operations for
// namespace.
//
//   - If this node is the owner: returns the local store.
//   - If another live node is the owner and a PeerDialer is configured: dials
//     the owner and returns that store.
//   - If the owner is pinned but unreachable (dead): write=true returns
//     ErrNamespaceReadOnly; write=false falls back to the local store so
//     reads are still served from whatever data this node holds.
//   - If no dialer is configured for a live remote owner: returns ErrNotOwner.
func (s *SyncStore) ownerStore(namespace string, write bool) (config.Store, error) {
	ownerID, ok := s.ring.OwnerOf(namespace)
	if !ok {
		// Empty ring — no members at all.
		if write {
			return nil, fmt.Errorf("%w: ring has no members", ErrNotOwner)
		}
		return s.local, nil
	}
	if ownerID == s.self.ID {
		return s.local, nil
	}
	m, ok := s.ring.MemberOf(ownerID)
	if !ok {
		// Owner is recorded (pinned) but not currently alive.
		if write {
			return nil, fmt.Errorf("%w: owner %q is unreachable", ErrNamespaceReadOnly, ownerID)
		}
		return s.local, nil
	}
	if s.dialer == nil {
		return nil, fmt.Errorf("%w: %q is owned by node %q", ErrNotOwner, namespace, ownerID)
	}
	return s.dialer.Dial(m.Addr)
}

// --- gossip publish ---

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
		s.reportError(fmt.Errorf("peersync: malformed envelope: %w", err))
		return
	}
	switch env.Type {
	case msgHeartbeat:
		var hb heartbeatMsg
		if err := json.Unmarshal(env.Payload, &hb); err != nil {
			s.reportError(fmt.Errorf("peersync: malformed heartbeat: %w", err))
			return
		}
		s.handleHeartbeat(hb)
	case msgRingChange:
		var state RingState
		if err := json.Unmarshal(env.Payload, &state); err != nil {
			s.reportError(fmt.Errorf("peersync: malformed ring-change: %w", err))
			return
		}
		if s.ring.Apply(state) {
			// Remove greylisted nodes the incoming state may have re-introduced.
			s.scrubGreylisted()
		}
		s.seedNodesFromState(state)
	}
}

// reportError routes a non-fatal gossip error to the errHandler when one is
// configured, or falls back to a logger warning.
func (s *SyncStore) reportError(err error) {
	if s.opts.errHandler != nil {
		s.opts.errHandler(err)
	} else {
		s.opts.logger.Warn(err.Error())
	}
}

// scrubGreylisted removes greylisted nodes from the ring after an Apply,
// preventing gossip replays from resurrecting recently evicted nodes.
// nodesMu.Lock is held across the ring.Remove calls so that a concurrent
// seedNodesFromState cannot re-add a node between the greylist check and
// the remove. ring.mu is a separate lock so there is no deadlock risk.
func (s *SyncStore) scrubGreylisted() {
	now := time.Now()
	grace := 2 * s.opts.failureTimeout
	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	for id, evictedAt := range s.evicted {
		if now.Sub(evictedAt) < grace {
			s.ring.Remove(id)
		}
	}
}

// seedNodesFromState seeds s.nodes from a received RingState so that the
// failure detector can evict nodes added via ring-change but never heartbeated.
//
// Nodes within the eviction greylist window are skipped to prevent gossip
// replays from indefinitely resurrecting dead nodes.
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
				continue
			}
			delete(s.evicted, m.ID)
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
		s.ring.Add(Member{ID: hb.NodeID, Addr: hb.Addr})
		s.scheduleAnnounce()
	}
}

// handleMemberEvent is the MembershipTransport callback. It is the counterpart
// to handleHeartbeat/detectFailures but driven by the transport's own protocol
// rather than peersync's timer-based heartbeat loop. Only used when the
// transport implements MembershipTransport.
func (s *SyncStore) handleMemberEvent(evt MemberEvent) {
	if evt.Member.ID == s.self.ID {
		return // self is already managed by Connect / Close
	}
	switch evt.Type {
	case MemberJoined:
		s.ring.Add(evt.Member)
		s.scheduleAnnounce()
	case MemberLeft:
		s.ring.Remove(evt.Member.ID)
		s.scheduleAnnounce()
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
			hb := heartbeatMsg{NodeID: s.self.ID, Addr: s.self.Addr}
			data, err := json.Marshal(hb)
			if err != nil {
				s.reportError(fmt.Errorf("peersync: marshal heartbeat: %w", err))
				continue
			}
			env := message{Type: msgHeartbeat, Payload: data}
			payload, err := json.Marshal(env)
			if err != nil {
				s.reportError(fmt.Errorf("peersync: marshal heartbeat envelope: %w", err))
				continue
			}
			pubCtx, cancel := context.WithTimeout(s.ctx, s.opts.publishTimeout)
			if err := s.transport.Publish(pubCtx, payload); err != nil {
				s.reportError(fmt.Errorf("peersync: publish heartbeat: %w", err))
			}
			cancel()
		}
	}
}

func (s *SyncStore) failureDetectLoop() {
	defer s.wg.Done()
	// Check at failureTimeout/3 so a dead node is detected within one interval
	// after the threshold, regardless of heartbeat rate.
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
// broadcast, decoupling publishRingChange from the transport handler goroutine.
func (s *SyncStore) announceLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.ringChangeCh:
			pubCtx, cancel := context.WithTimeout(s.ctx, s.opts.publishTimeout)
			if err := s.publishRingChange(pubCtx); err != nil {
				s.reportError(fmt.Errorf("peersync: publish ring change: %w", err))
			}
			cancel()
		}
	}
}
