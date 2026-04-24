package peersync

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// newTestMemberlistConfig returns a memberlist config suitable for unit tests:
// loopback-only, random OS-assigned port, accelerated gossip intervals, and
// all memberlist log output discarded.
func newTestMemberlistConfig(t *testing.T, name string) *memberlist.Config {
	t.Helper()
	cfg := memberlist.DefaultLANConfig()
	cfg.Name = name
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = 0
	cfg.AdvertiseAddr = "127.0.0.1"
	cfg.LogOutput = io.Discard
	// Accelerate gossip for fast test convergence.
	cfg.GossipInterval = 20 * time.Millisecond
	cfg.ProbeInterval = 100 * time.Millisecond
	cfg.RetransmitMult = 2
	return cfg
}

// newTestMemberlistTransport creates a transport for the named node. peerAddr
// is set to "<name>:9000" to give each node a distinct application address.
func newTestMemberlistTransport(t *testing.T, name string) *MemberlistTransport {
	t.Helper()
	cfg := newTestMemberlistConfig(t, name)
	tr, err := NewMemberlistTransport(cfg, name+":9000")
	if err != nil {
		t.Fatalf("NewMemberlistTransport(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestNewMemberlistTransport_NilConfig(t *testing.T) {
	_, err := NewMemberlistTransport(nil, "node1:9000")
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestNewMemberlistTransport_OK(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	if tr == nil {
		t.Fatal("expected non-nil transport")
	}
	if tr.LocalNode() == nil {
		t.Fatal("LocalNode should not be nil after construction")
	}
}

func TestMemberlistTransport_ImplementsInterfaces(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	var _ Transport             = tr
	var _ TransportHealthChecker = tr
	var _ MembershipTransport   = tr
}

func TestMemberlistTransport_PeerAddrInMeta(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	meta := tr.NodeMeta(256)
	if string(meta) != "node1:9000" {
		t.Errorf("NodeMeta = %q, want %q", meta, "node1:9000")
	}
}

func TestMemberlistTransport_SubscribeSecondCallErrors(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	ctx := context.Background()

	if err := tr.Subscribe(ctx, func([]byte) {}); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	if err := tr.Subscribe(ctx, func([]byte) {}); err == nil {
		t.Error("second Subscribe: expected error, got nil")
	}
}

func TestMemberlistTransport_SubscribeMembersSecondCallErrors(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	ctx := context.Background()

	if err := tr.SubscribeMembers(ctx, func(MemberEvent) {}); err != nil {
		t.Fatalf("first SubscribeMembers: %v", err)
	}
	if err := tr.SubscribeMembers(ctx, func(MemberEvent) {}); err == nil {
		t.Error("second SubscribeMembers: expected error, got nil")
	}
}

func TestMemberlistTransport_CloseWithoutSubscribe(t *testing.T) {
	cfg := newTestMemberlistConfig(t, "node1")
	tr, err := NewMemberlistTransport(cfg, "node1:9000")
	if err != nil {
		t.Fatalf("NewMemberlistTransport: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close without Subscribe: %v", err)
	}
}

// TestMemberlistTransport_LoopbackDelivery verifies that Publish delivers to
// the local handler immediately even with a single node (no peers).
func TestMemberlistTransport_LoopbackDelivery(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	ctx := context.Background()

	want := []byte("loopback-test")
	done := make(chan []byte, 1)

	if err := tr.Subscribe(ctx, func(msg []byte) {
		done <- msg
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := tr.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-done:
		if string(got) != string(want) {
			t.Errorf("loopback: got %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for loopback delivery")
	}
}

// TestMemberlistTransport_TwoNodeBroadcast verifies that a message published
// on node A arrives at node B via memberlist gossip.
func TestMemberlistTransport_TwoNodeBroadcast(t *testing.T) {
	ctx := context.Background()

	trA := newTestMemberlistTransport(t, "nodeA")
	trB := newTestMemberlistTransport(t, "nodeB")

	var (
		mu       sync.Mutex
		received = make(map[string][]byte)
		wg       sync.WaitGroup
	)
	wg.Add(2)
	markReceived := func(who string) func([]byte) {
		return func(msg []byte) {
			mu.Lock()
			if _, seen := received[who]; !seen {
				cp := make([]byte, len(msg))
				copy(cp, msg)
				received[who] = cp
				wg.Done()
			}
			mu.Unlock()
		}
	}

	if err := trA.Subscribe(ctx, markReceived("A")); err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	if err := trB.Subscribe(ctx, markReceived("B")); err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	addrA := fmt.Sprintf("127.0.0.1:%d", trA.LocalNode().Port)
	if _, err := trB.Join([]string{addrA}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	want := []byte("broadcast-test")
	if err := trA.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		mu.Lock()
		t.Fatalf("timeout: received on nodes: %v", received)
		mu.Unlock()
	}

	mu.Lock()
	defer mu.Unlock()
	for who, got := range received {
		if string(got) != string(want) {
			t.Errorf("node %s: got %q, want %q", who, got, want)
		}
	}
}

// TestMemberlistTransport_MembershipEvents verifies that joining a two-node
// cluster fires MemberJoined events on both nodes (for the remote peer).
func TestMemberlistTransport_MembershipEvents(t *testing.T) {
	ctx := context.Background()

	trA := newTestMemberlistTransport(t, "nodeA")
	trB := newTestMemberlistTransport(t, "nodeB")

	// Each node collects join events for the OTHER node only (self-join
	// notifications from memberlist may or may not fire; we don't depend on them).
	joinedA := make(chan Member, 4)
	joinedB := make(chan Member, 4)

	if err := trA.SubscribeMembers(ctx, func(evt MemberEvent) {
		if evt.Type == MemberJoined && evt.Member.ID != "nodeA" {
			joinedA <- evt.Member
		}
	}); err != nil {
		t.Fatalf("SubscribeMembers A: %v", err)
	}
	if err := trB.SubscribeMembers(ctx, func(evt MemberEvent) {
		if evt.Type == MemberJoined && evt.Member.ID != "nodeB" {
			joinedB <- evt.Member
		}
	}); err != nil {
		t.Fatalf("SubscribeMembers B: %v", err)
	}

	addrA := fmt.Sprintf("127.0.0.1:%d", trA.LocalNode().Port)
	if _, err := trB.Join([]string{addrA}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Both nodes should fire a MemberJoined for the other within a gossip round.
	timeout := time.After(3 * time.Second)
	for _, ch := range []chan Member{joinedA, joinedB} {
		select {
		case m := <-ch:
			// peerAddr is encoded in NodeMeta and should come through.
			if m.Addr == "" {
				t.Errorf("MemberJoined event has empty Addr: %+v", m)
			}
		case <-timeout:
			t.Fatal("timeout waiting for MemberJoined event")
		}
	}
}

// TestMemberlistTransport_MemberLeft verifies that closing a node fires a
// MemberLeft event on the remaining node.
func TestMemberlistTransport_MemberLeft(t *testing.T) {
	ctx := context.Background()

	trA := newTestMemberlistTransport(t, "nodeA")
	cfgB := newTestMemberlistConfig(t, "nodeB")
	trB, err := NewMemberlistTransport(cfgB, "nodeB:9000")
	if err != nil {
		t.Fatalf("NewMemberlistTransport B: %v", err)
	}

	leftA := make(chan Member, 1)
	if err := trA.SubscribeMembers(ctx, func(evt MemberEvent) {
		if evt.Type == MemberLeft && evt.Member.ID == "nodeB" {
			leftA <- evt.Member
		}
	}); err != nil {
		t.Fatalf("SubscribeMembers A: %v", err)
	}

	addrA := fmt.Sprintf("127.0.0.1:%d", trA.LocalNode().Port)
	if _, err := trB.Join([]string{addrA}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let membership converge

	// Close B — should trigger a graceful leave and fire MemberLeft on A.
	if err := trB.Close(); err != nil {
		t.Fatalf("Close B: %v", err)
	}

	select {
	case m := <-leftA:
		if m.ID != "nodeB" {
			t.Errorf("MemberLeft.ID = %q, want %q", m.ID, "nodeB")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for MemberLeft event")
	}
}

// TestMemberlistTransport_SyncStoreIntegration verifies the full end-to-end:
// SyncStore detects MembershipTransport and uses native SWIM events to drive
// ring membership, so nodeB's Set is forwarded to nodeA (the ring owner) once
// the cluster converges.
func TestMemberlistTransport_SyncStoreIntegration(t *testing.T) {
	ctx := context.Background()

	trA := newTestMemberlistTransport(t, "nodeA")
	trB := newTestMemberlistTransport(t, "nodeB")

	storeA := memory.NewStore()
	storeB := memory.NewStore()

	dialerB := newTestDialer()
	memberA := Member{ID: "nodeA", Addr: "nodeA:9000"}
	memberB := Member{ID: "nodeB", Addr: "nodeB:9000"}

	nodeA, err := New(storeA, memberA, trA)
	if err != nil {
		t.Fatalf("New nodeA: %v", err)
	}
	nodeB, err := New(storeB, memberB, trB, WithPeerDialer(dialerB))
	if err != nil {
		t.Fatalf("New nodeB: %v", err)
	}
	dialerB.register("nodeA:9000", nodeA)
	dialerB.register("nodeB:9000", nodeB)

	if err := nodeA.Connect(ctx); err != nil {
		t.Fatalf("Connect nodeA: %v", err)
	}
	if err := nodeB.Connect(ctx); err != nil {
		t.Fatalf("Connect nodeB: %v", err)
	}
	t.Cleanup(func() { _ = nodeA.Close(ctx); _ = nodeB.Close(ctx) })

	// B joins A's memberlist cluster — SWIM fires MemberJoined on both sides,
	// which drives ring.Add via handleMemberEvent.
	addrA := fmt.Sprintf("127.0.0.1:%d", trA.LocalNode().Port)
	if _, err := trB.Join([]string{addrA}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Wait for SWIM membership events and ring convergence.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.HasMember("nodeB") && nodeB.HasMember("nodeA") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !nodeA.HasMember("nodeB") || !nodeB.HasMember("nodeA") {
		t.Fatal("ring did not converge: nodes do not know about each other")
	}

	// Claim "ns" on nodeA so nodeB's Set is forwarded there.
	if err := nodeA.Claim(ctx, "ns"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// Wait for ring-change gossip to reach nodeB.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		owner, _ := nodeB.OwnerOf("ns")
		if owner == "nodeA" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	owner, _ := nodeB.OwnerOf("ns")
	if owner != "nodeA" {
		t.Fatalf("OwnerOf(ns) on nodeB = %q, want %q", owner, "nodeA")
	}

	// Set from nodeB — should be forwarded to nodeA's local store.
	if _, err := nodeB.Set(ctx, "ns", "key", config.NewValue("hello")); err != nil {
		t.Fatalf("Set via nodeB: %v", err)
	}
	got, err := storeA.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get from storeA: %v", err)
	}
	s, _ := got.String()
	if s != "hello" {
		t.Errorf("storeA.Get = %q, want %q", s, "hello")
	}
}

func TestMemberlistTransport_Members(t *testing.T) {
	ctx := context.Background()

	trA := newTestMemberlistTransport(t, "nodeA")
	trB := newTestMemberlistTransport(t, "nodeB")

	_ = trA.Subscribe(ctx, func([]byte) {})
	_ = trB.Subscribe(ctx, func([]byte) {})

	addrA := fmt.Sprintf("127.0.0.1:%d", trA.LocalNode().Port)
	if _, err := trB.Join([]string{addrA}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if n := len(trA.Members()); n < 2 {
		t.Errorf("trA.Members: got %d, want ≥2", n)
	}
}

func TestMemberlistTransport_Health(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	hc, ok := interface{}(tr).(TransportHealthChecker)
	if !ok {
		t.Fatal("MemberlistTransport does not implement TransportHealthChecker")
	}
	if err := hc.Health(context.Background()); err != nil {
		t.Errorf("Health on fresh node: %v", err)
	}
}

func TestMemberlistTransport_JoinLocalNode(t *testing.T) {
	tr := newTestMemberlistTransport(t, "node1")
	node := tr.LocalNode()
	if node == nil {
		t.Fatal("LocalNode returned nil")
	}
	if node.Name != "node1" {
		t.Errorf("LocalNode.Name = %q, want %q", node.Name, "node1")
	}
	if node.Port == 0 {
		t.Error("LocalNode.Port should be non-zero (OS-assigned)")
	}
}
