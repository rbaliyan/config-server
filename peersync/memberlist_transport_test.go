package peersync

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
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

func newTestMemberlistTransport(t *testing.T, name string) *MemberlistTransport {
	t.Helper()
	cfg := newTestMemberlistConfig(t, name)
	tr, err := NewMemberlistTransport(cfg)
	if err != nil {
		t.Fatalf("NewMemberlistTransport(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestNewMemberlistTransport_NilConfig(t *testing.T) {
	_, err := NewMemberlistTransport(nil)
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
	var _ Transport = tr
	var _ TransportHealthChecker = tr
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

func TestMemberlistTransport_CloseWithoutSubscribe(t *testing.T) {
	cfg := newTestMemberlistConfig(t, "node1")
	tr, err := NewMemberlistTransport(cfg)
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

	// Synchronise delivery on both ends.
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

	// B joins A's cluster.
	addrA := fmt.Sprintf("127.0.0.1:%d", trA.LocalNode().Port)
	if _, err := trB.Join([]string{addrA}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Allow one gossip interval for membership convergence.
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
	// A freshly created node should have a healthy score (0).
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
