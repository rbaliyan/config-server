package peersync

import (
	"fmt"
	"testing"
)

// benchMembers builds a slice of n members with realistic IDs and addresses.
func benchMembers(n int) []Member {
	members := make([]Member, n)
	for i := range members {
		members[i] = Member{
			ID:   fmt.Sprintf("node-%d", i),
			Addr: fmt.Sprintf("10.0.0.%d:8080", i),
		}
	}
	return members
}

// seededRing returns a ring populated with n members at the default vnode count.
func seededRing(n int) *ring {
	r := newRing(defaultVNodes)
	for _, m := range benchMembers(n) {
		r.Add(m)
	}
	return r
}

// BenchmarkRingOwnerOf measures the read path: an RLock plus a binary search
// over n*vnodes sorted points. This runs on every routed operation, so its
// cost and (lack of) allocations matter.
func BenchmarkRingOwnerOf(b *testing.B) {
	r := seededRing(10)
	namespaces := make([]string, 1024)
	for i := range namespaces {
		namespaces[i] = fmt.Sprintf("tenant-%d/service", i)
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = r.OwnerOf(namespaces[i&1023])
		i++
	}
}

// BenchmarkRingOwnerOfParallel exercises the RWMutex read path under
// contention. OwnerOf takes only an RLock, so many goroutines should proceed
// concurrently; this quantifies that the lock does not serialise readers.
func BenchmarkRingOwnerOfParallel(b *testing.B) {
	r := seededRing(10)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = r.OwnerOf(fmt.Sprintf("tenant-%d/service", i&1023))
			i++
		}
	})
}

// BenchmarkRingAdd measures the cost of registering a member. Add inserts
// vnodes (150) points, each requiring a fmt.Sprintf("%s#%d", ...) allocation,
// then re-sorts the ring. ReportAllocs surfaces the per-vnode allocations.
func BenchmarkRingAdd(b *testing.B) {
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		// Use a fresh single-member ring per iteration so we always measure
		// an insert (not the idempotent no-op path) at a stable ring size.
		r := newRing(defaultVNodes)
		r.Add(Member{
			ID:   fmt.Sprintf("node-%d", i),
			Addr: "10.0.0.1:8080",
		})
		i++
	}
}

// BenchmarkRingApply measures a full ring rebuild from a gossiped RingState.
// Apply rebuilds every member's vnodes (member_count * 150 Sprintf calls) and
// re-sorts. This runs on each accepted gossip update across the cluster.
func BenchmarkRingApply(b *testing.B) {
	members := benchMembers(10)

	b.ReportAllocs()
	epoch := int64(0)
	for b.Loop() {
		r := newRing(defaultVNodes)
		epoch++
		r.Apply(RingState{Members: members, Epoch: epoch})
	}
}
