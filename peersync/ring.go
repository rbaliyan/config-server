// Package peersync provides a config.Store wrapper that replicates writes
// across a cluster of nodes using a consistent-hash ring for namespace
// ownership assignment and a pluggable pub/sub transport for gossip and
// replication.
//
// Ownership model:
// The ring maps each namespace to a preferred owner using consistent hashing.
// Clients that know the ring state should direct writes to the owner for best
// consistency. Every node accepts writes regardless; changes are propagated to
// all peers via the replication channel. Last-arrival order wins per node,
// which is sufficient for low-contention configuration workloads.
//
// Manual overrides:
// Individual namespaces can be pinned to a specific node with [SyncStore.Pin],
// bypassing the hash ring. Pins are gossiped so all nodes converge on the same
// overrides. [SyncStore.Unpin] restores hash-ring routing.
//
// Failure detection:
// Each node publishes a heartbeat on the transport. A node absent for longer
// than the failure timeout is removed from the ring, and the removal is
// gossiped to all peers. The ring therefore self-heals without operator action.
//
// Ring convergence:
// Every ring-change message carries the full ring state and a monotonically
// increasing epoch. Nodes apply a received state only when its epoch exceeds
// the local epoch, so the ring converges to the globally highest-epoch view.
package peersync

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

const defaultVNodes = 150

// Member represents one node in the cluster.
type Member struct {
	// ID is the unique identifier for this node.
	ID string `json:"id"`
	// Addr is the network address clients can use to reach this node
	// (informational; peersync does not dial peers internally).
	Addr string `json:"addr"`
}

// RingState is a serialisable snapshot of the ring used for gossip and
// persistence. Epoch is monotonically increasing; higher epochs win conflicts.
type RingState struct {
	Members   []Member          `json:"members"`
	Overrides map[string]string `json:"overrides,omitempty"`
	Epoch     int64             `json:"epoch"`
}

type ringPoint struct {
	hash uint32
	id   string
}

// Ring is a thread-safe consistent-hash ring that maps namespace names to
// cluster members. Virtual nodes (vnodes per member) ensure even distribution.
// Manual overrides bypass the hash and pin a namespace to a specific member.
type Ring struct {
	mu        sync.RWMutex
	vnodes    int
	points    []ringPoint // sorted ascending by hash
	members   map[string]Member
	overrides map[string]string // namespace → member ID
	epoch     int64
}

func newRing(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = defaultVNodes
	}
	return &Ring{
		vnodes:    vnodes,
		members:   make(map[string]Member),
		overrides: make(map[string]string),
	}
}

// Add registers a member and inserts virtual points into the ring.
// If the member is already present its address is updated.
func (r *Ring) Add(m Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
func (r *Ring) Remove(id string) {
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
func (r *Ring) Pin(namespace, nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.overrides[namespace] = nodeID
	r.epoch++
}

// Unpin removes a manual pin and restores hash-ring routing for namespace.
func (r *Ring) Unpin(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.overrides, namespace)
	r.epoch++
}

// OwnerOf returns the member ID responsible for namespace, consulting manual
// overrides first. Returns ("", false) when the ring is empty.
func (r *Ring) OwnerOf(namespace string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id, ok := r.overrides[namespace]; ok {
		return id, true
	}
	return r.lookupLocked(namespace)
}

// Has reports whether a member with the given ID is currently in the ring.
func (r *Ring) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.members[id]
	return ok
}

// Members returns the current set of registered members.
func (r *Ring) Members() []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	return out
}

// Epoch returns the current ring epoch.
func (r *Ring) Epoch() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.epoch
}

// Snapshot returns a point-in-time copy of ring state suitable for gossip
// or persistence. Members are sorted by ID for deterministic snapshots.
func (r *Ring) Snapshot() RingState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	members := make([]Member, 0, len(r.members))
	for _, m := range r.members {
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	overrides := make(map[string]string, len(r.overrides))
	for k, v := range r.overrides {
		overrides[k] = v
	}
	return RingState{Members: members, Overrides: overrides, Epoch: r.epoch}
}

// Apply rebuilds the ring from state. Returns false and is a no-op when
// state.Epoch is not greater than the current epoch (stale update).
func (r *Ring) Apply(state RingState) bool {
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

func (r *Ring) lookupLocked(namespace string) (string, bool) {
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

func (r *Ring) sortLocked() {
	sort.Slice(r.points, func(i, j int) bool {
		return r.points[i].hash < r.points[j].hash
	})
}

func ringHash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
