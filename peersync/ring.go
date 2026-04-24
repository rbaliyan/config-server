package peersync

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

const defaultVNodes = 150

// Member represents one node in the cluster.
// A zero-value Member (empty ID) is invalid; New rejects it with an error.
type Member struct {
	// ID is the unique identifier for this node.
	ID string `json:"id"`
	// Addr is the network address of this node. SyncStore passes it to
	// PeerDialer.Dial when forwarding operations to the owner of a namespace.
	Addr string `json:"addr"`
}

// RingState is a serialisable snapshot of the ring used for gossip and
// persistence. Epoch is monotonically increasing; Apply accepts a state only
// when its epoch is strictly greater than the current epoch — equal epochs are
// considered stale and dropped.
type RingState struct {
	Members []Member `json:"members"`
	// Overrides maps namespace names to member IDs for Pin/Claim entries that
	// bypass the hash ring. Included in every gossip broadcast so all peers
	// converge to the same routing table.
	Overrides map[string]string `json:"overrides,omitempty"`
	Epoch     int64             `json:"epoch"`
}

type ringPoint struct {
	hash uint32
	id   string
}

// ring is a thread-safe consistent-hash ring that maps namespace names to
// cluster members. Virtual nodes (vnodes per member) ensure even distribution.
// Manual overrides bypass the hash and pin a namespace to a specific member.
//
// Concurrency: all exported methods are safe for concurrent use. Apply and
// OwnerOf/Members/Snapshot can execute concurrently; Apply holds a write lock
// for the duration of the rebuild so OwnerOf callers see either the old or
// the new state atomically, never a partial one.
type ring struct {
	mu        sync.RWMutex
	vnodes    int
	points    []ringPoint // sorted ascending by hash
	members   map[string]Member
	overrides map[string]string // namespace → member ID
	epoch     int64
}

func newRing(vnodes int) *ring {
	if vnodes <= 0 {
		vnodes = defaultVNodes
	}
	return &ring{
		vnodes:    vnodes,
		members:   make(map[string]Member),
		overrides: make(map[string]string),
	}
}

// Add registers a member and inserts virtual points into the ring.
// If the member already exists with the same address the call is a no-op
// (epoch is not bumped), matching the idempotent behaviour of Pin/Unpin.
// If the address changed, the virtual points are rebuilt and the epoch bumps.
func (r *ring) Add(m Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.members[m.ID]; ok && existing.Addr == m.Addr {
		return
	}
	r.members[m.ID] = m
	// Remove stale virtual points for this ID before re-inserting.
	kept := r.points[:0]
	for _, p := range r.points {
		if p.id != m.ID {
			kept = append(kept, p)
		}
	}
	r.points = kept
	for i := 0; i < r.vnodes; i++ {
		h := ringHash(fmt.Sprintf("%s#%d", m.ID, i))
		r.points = append(r.points, ringPoint{h, m.ID})
	}
	r.sortLocked()
	r.epoch++
}

// Remove unregisters a member and removes its virtual points from the ring.
func (r *ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[id]; !ok {
		return
	}
	delete(r.members, id)
	kept := r.points[:0]
	for _, p := range r.points {
		if p.id != id {
			kept = append(kept, p)
		}
	}
	r.points = kept
	r.epoch++
}

// Pin forces namespace to always resolve to nodeID, overriding the hash ring.
// No-ops (and does not bump epoch) when the mapping is already set.
func (r *ring) Pin(namespace, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.overrides[namespace] == nodeID {
		return
	}
	r.overrides[namespace] = nodeID
	r.epoch++
}

// Unpin removes a manual pin and restores hash-ring routing for namespace.
// No-ops (and does not bump epoch) when no pin exists.
func (r *ring) Unpin(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.overrides[namespace]; !ok {
		return
	}
	delete(r.overrides, namespace)
	r.epoch++
}

// OwnerOf returns the member ID responsible for namespace, consulting manual
// overrides first. Returns ("", false) when the ring is empty.
func (r *ring) OwnerOf(namespace string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id, ok := r.overrides[namespace]; ok {
		return id, true
	}
	return r.lookupLocked(namespace)
}

// Has reports whether a member with the given ID is currently in the ring.
func (r *ring) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.members[id]
	return ok
}

// MemberOf returns the Member for the given ID, or (Member{}, false) if not found.
func (r *ring) MemberOf(id string) (Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.members[id]
	return m, ok
}

// Members returns the current set of registered members.
func (r *ring) Members() []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	return out
}

// Epoch returns the current ring epoch.
func (r *ring) Epoch() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.epoch
}

// Snapshot returns a point-in-time copy of ring state suitable for gossip
// or persistence. Members are sorted by ID for deterministic snapshots.
func (r *ring) Snapshot() RingState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	members := make([]Member, 0, len(r.members))
	for _, m := range r.members {
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	var overrides map[string]string
	if len(r.overrides) > 0 {
		overrides = make(map[string]string, len(r.overrides))
		for k, v := range r.overrides {
			overrides[k] = v
		}
	}
	return RingState{Members: members, Overrides: overrides, Epoch: r.epoch}
}

// Apply rebuilds the ring from state. Returns false and is a no-op when
// state.Epoch is not greater than the current epoch (stale update).
func (r *ring) Apply(state RingState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if state.Epoch <= r.epoch {
		return false
	}
	r.members = make(map[string]Member, len(state.Members))
	r.points = r.points[:0]
	for _, m := range state.Members {
		r.members[m.ID] = m
		for i := 0; i < r.vnodes; i++ {
			h := ringHash(fmt.Sprintf("%s#%d", m.ID, i))
			r.points = append(r.points, ringPoint{h, m.ID})
		}
	}
	r.sortLocked()
	r.overrides = make(map[string]string, len(state.Overrides))
	for k, v := range state.Overrides {
		r.overrides[k] = v
	}
	r.epoch = state.Epoch
	return true
}

func (r *ring) lookupLocked(namespace string) (string, bool) {
	if len(r.points) == 0 {
		return "", false
	}
	h := ringHash(namespace)
	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})
	if idx == len(r.points) {
		idx = 0 // wrap around
	}
	return r.points[idx].id, true
}

func (r *ring) sortLocked() {
	sort.Slice(r.points, func(i, j int) bool {
		return r.points[i].hash < r.points[j].hash
	})
}

func ringHash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
