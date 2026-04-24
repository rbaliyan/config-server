package peersync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
	_ "github.com/rbaliyan/config/codec/json"
)

// memTransport is an in-process Transport for tests.
// Publish delivers synchronously to all subscribers.
type memTransport struct {
	mu       sync.Mutex
	handlers []func(Message)
}

func (t *memTransport) Publish(_ context.Context, msg Message) error {
	t.mu.Lock()
	handlers := make([]func(Message), len(t.handlers))
	copy(handlers, t.handlers)
	t.mu.Unlock()
	for _, h := range handlers {
		h(msg)
	}
	return nil
}

func (t *memTransport) Subscribe(_ context.Context, handler func(Message)) error {
	t.mu.Lock()
	t.handlers = append(t.handlers, handler)
	t.mu.Unlock()
	return nil
}

func (t *memTransport) Close() error { return nil }

func newTestStore(t *testing.T, id string, tr Transport, opts ...Option) *SyncStore {
	t.Helper()
	local := memory.NewStore()
	s, err := New(local, Member{ID: id, Addr: id + ":9000"}, tr, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func TestSyncStore_SetReplicatesToPeer(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)

	val := config.NewValue("hello")
	if _, err := a.Set(ctx, "ns", "key1", val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Replication is synchronous via memTransport so it should already be applied.
	got, err := b.Get(ctx, "ns", "key1")
	if err != nil {
		t.Fatalf("Get on peer: %v", err)
	}
	s, _ := got.String()
	if s != "hello" {
		t.Fatalf("expected hello, got %q", s)
	}
}

func TestSyncStore_DeleteReplicatesToPeer(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)

	val := config.NewValue("to-be-deleted")
	if _, err := a.Set(ctx, "ns", "key2", val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := a.Delete(ctx, "ns", "key2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := b.Get(ctx, "ns", "key2")
	if !config.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound on peer after delete, got: %v", err)
	}
}

func TestSyncStore_NoSelfReplication(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr)

	val := config.NewValue(42)
	stored, err := a.Set(ctx, "ns", "counter", val)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	// The replication message is published and received back by nodeA,
	// but nodeA must skip it (src == self.ID). Version should remain at 1.
	got, err := a.Get(ctx, "ns", "counter")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Metadata().Version() != stored.Metadata().Version() {
		t.Fatalf("version changed after self-replication: want %d, got %d",
			stored.Metadata().Version(), got.Metadata().Version())
	}
}

func TestSyncStore_PinUnpin(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)

	_ = b // ensure nodeB is in the ring

	if err := a.Pin(ctx, "payments", "nodeB"); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	id, ok := a.OwnerOf("payments")
	if !ok || id != "nodeB" {
		t.Fatalf("Pin: expected nodeB owner, got %q ok=%v", id, ok)
	}
	// Pin is gossiped; both nodes should see it.
	id, ok = b.OwnerOf("payments")
	if !ok || id != "nodeB" {
		t.Fatalf("Pin not gossiped to nodeB: got %q ok=%v", id, ok)
	}

	if err := a.Unpin(ctx, "payments"); err != nil {
		t.Fatalf("Unpin: %v", err)
	}
	// After unpin, routing is by hash — just verify it returns a valid node.
	id, ok = a.OwnerOf("payments")
	if !ok || (id != "nodeA" && id != "nodeB") {
		t.Fatalf("unexpected owner after Unpin: %q ok=%v", id, ok)
	}
}

func TestSyncStore_FailureDetection(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr,
		WithHeartbeatInterval(20*time.Millisecond),
		WithFailureTimeout(60*time.Millisecond),
	)

	// Create nodeB with short timers but manage its lifecycle manually.
	b, _ := New(memory.NewStore(), Member{ID: "nodeB", Addr: "nodeB:9000"}, tr,
		WithHeartbeatInterval(20*time.Millisecond),
		WithFailureTimeout(60*time.Millisecond),
	)
	_ = b.Connect(ctx)

	// nodeB should be visible on nodeA's ring after its first heartbeat.
	time.Sleep(50 * time.Millisecond)
	if !a.ring.Has("nodeB") {
		t.Fatal("nodeB not seen by nodeA after heartbeat")
	}

	// Stop nodeB without sending a graceful leave.
	b.cancel()
	b.wg.Wait()

	// Wait for nodeA's failure detector to evict nodeB.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !a.ring.Has("nodeB") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("nodeB was not evicted from nodeA's ring after failure timeout")
}

func TestNew_Validation(t *testing.T) {
	tr := &memTransport{}
	local := memory.NewStore()

	if _, err := New(nil, Member{ID: "n1"}, tr); err == nil {
		t.Error("expected error for nil local store")
	}
	if _, err := New(local, Member{}, tr); err == nil {
		t.Error("expected error for empty member ID")
	}
	if _, err := New(local, Member{ID: "n1"}, nil); err == nil {
		t.Error("expected error for nil transport")
	}
}
