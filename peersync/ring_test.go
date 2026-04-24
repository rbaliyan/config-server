package peersync

import (
	"fmt"
	"testing"
)

func TestRing_OwnerOf_EmptyRing(t *testing.T) {
	r := newRing(0)
	_, ok := r.OwnerOf("ns")
	if ok {
		t.Fatal("expected false for empty ring")
	}
}

func TestRing_OwnerOf_SingleMember(t *testing.T) {
	r := newRing(0)
	r.Add(Member{ID: "nodeA", Addr: "localhost:9001"})
	id, ok := r.OwnerOf("any-namespace")
	if !ok || id != "nodeA" {
		t.Fatalf("expected nodeA, got %q ok=%v", id, ok)
	}
}

func TestRing_Distribution(t *testing.T) {
	r := newRing(150)
	nodes := []string{"nodeA", "nodeB", "nodeC"}
	for _, id := range nodes {
		r.Add(Member{ID: id, Addr: id + ":9000"})
	}

	counts := make(map[string]int)
	const total = 3000
	for i := 0; i < total; i++ {
		ns := fmt.Sprintf("namespace-%d", i)
		id, ok := r.OwnerOf(ns)
		if !ok {
			t.Fatalf("OwnerOf returned false for %q", ns)
		}
		counts[id]++
	}
	// Each node should own roughly 1/3 of namespaces. Allow ±25% tolerance.
	expect := total / len(nodes)
	lo, hi := expect*75/100, expect*125/100
	for _, id := range nodes {
		c := counts[id]
		if c < lo || c > hi {
			t.Errorf("node %s owns %d/%d namespaces (expected %d–%d)", id, c, total, lo, hi)
		}
	}
}

func TestRing_Remove(t *testing.T) {
	r := newRing(0)
	r.Add(Member{ID: "nodeA"})
	r.Add(Member{ID: "nodeB"})
	r.Remove("nodeA")

	for i := 0; i < 100; i++ {
		id, ok := r.OwnerOf(fmt.Sprintf("ns-%d", i))
		if !ok {
			t.Fatal("expected owner")
		}
		if id == "nodeA" {
			t.Fatalf("namespace routed to removed node nodeA")
		}
	}
}

func TestRing_Pin_Unpin(t *testing.T) {
	r := newRing(0)
	r.Add(Member{ID: "nodeA"})
	r.Add(Member{ID: "nodeB"})

	r.Pin("payments", "nodeB")
	id, _ := r.OwnerOf("payments")
	if id != "nodeB" {
		t.Fatalf("expected pinned owner nodeB, got %q", id)
	}

	r.Unpin("payments")
	// After unpin, routing is determined by hash — just verify it returns a valid node.
	id, ok := r.OwnerOf("payments")
	if !ok || (id != "nodeA" && id != "nodeB") {
		t.Fatalf("unexpected owner after unpin: %q ok=%v", id, ok)
	}
}

func TestRing_EpochMonotonic(t *testing.T) {
	r := newRing(0)
	e0 := r.Epoch()
	r.Add(Member{ID: "n1"})
	if r.Epoch() <= e0 {
		t.Fatal("epoch did not advance after Add")
	}
	e1 := r.Epoch()
	r.Remove("n1")
	if r.Epoch() <= e1 {
		t.Fatal("epoch did not advance after Remove")
	}
	e2 := r.Epoch()
	r.Pin("ns", "n1")
	if r.Epoch() <= e2 {
		t.Fatal("epoch did not advance after Pin")
	}
}

func TestRing_Apply_RejectsStale(t *testing.T) {
	r := newRing(0)
	r.Add(Member{ID: "n1"})
	epoch := r.Epoch()

	snap := r.Snapshot()
	snap.Members = append(snap.Members, Member{ID: "n2"})
	// Apply a snapshot with the same epoch — should be rejected.
	snap.Epoch = epoch
	applied := r.Apply(snap)
	if applied {
		t.Fatal("Apply should reject snapshot with equal epoch")
	}
	if _, ok := r.members["n2"]; ok {
		t.Fatal("stale Apply must not modify the ring")
	}
}

func TestRing_Apply_AcceptsHigherEpoch(t *testing.T) {
	r := newRing(0)
	r.Add(Member{ID: "n1"})

	snap := RingState{
		Members: []Member{{ID: "n2"}, {ID: "n3"}},
		Epoch:   r.Epoch() + 10,
	}
	if !r.Apply(snap) {
		t.Fatal("Apply should accept higher-epoch snapshot")
	}
	if _, ok := r.members["n1"]; ok {
		t.Fatal("old member should be replaced after Apply")
	}
	if _, ok := r.members["n2"]; !ok {
		t.Fatal("new member n2 missing after Apply")
	}
}

func TestRing_Snapshot_IsACopy(t *testing.T) {
	r := newRing(0)
	r.Add(Member{ID: "n1"})
	snap := r.Snapshot()
	// Mutate snap — must not affect ring.
	snap.Members = append(snap.Members, Member{ID: "n2"})
	if len(r.Members()) != 1 {
		t.Fatal("snapshot mutation affected the ring")
	}
}
