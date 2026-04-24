package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
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
	handlers []func([]byte)
}

func (t *memTransport) Publish(_ context.Context, payload []byte) error {
	t.mu.Lock()
	handlers := make([]func([]byte), len(t.handlers))
	copy(handlers, t.handlers)
	t.mu.Unlock()
	for _, h := range handlers {
		h(payload)
	}
	return nil
}

func (t *memTransport) Subscribe(_ context.Context, handler func([]byte)) error {
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

	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)

	_ = b // ensure nodeB is in the ring

	a.Pin("payments", "nodeB")

	// scheduleAnnounce is async; drain the channel via the announceLoop.
	time.Sleep(20 * time.Millisecond)

	id, ok := a.OwnerOf("payments")
	if !ok || id != "nodeB" {
		t.Fatalf("Pin: expected nodeB owner, got %q ok=%v", id, ok)
	}
	// Pin is gossiped; both nodes should see it.
	id, ok = b.OwnerOf("payments")
	if !ok || id != "nodeB" {
		t.Fatalf("Pin not gossiped to nodeB: got %q ok=%v", id, ok)
	}

	a.Unpin("payments")
	time.Sleep(20 * time.Millisecond)

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

// TestSyncStore_Members verifies Members() returns the current ring members.
func TestSyncStore_Members(t *testing.T) {
	tr := &memTransport{}
	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)
	_ = b

	// Give heartbeats a moment to propagate (memTransport is sync, but
	// announceLoop is async).
	time.Sleep(20 * time.Millisecond)

	members := a.Members()
	found := make(map[string]bool)
	for _, m := range members {
		found[m.ID] = true
	}
	if !found["nodeA"] {
		t.Error("nodeA missing from Members()")
	}
}

// TestSyncStore_Snapshot verifies Snapshot() is deterministic across calls.
func TestSyncStore_Snapshot(t *testing.T) {
	tr := &memTransport{}
	a := newTestStore(t, "nodeA", tr)
	newTestStore(t, "nodeB", tr)
	time.Sleep(20 * time.Millisecond)

	s1 := a.Snapshot()
	s2 := a.Snapshot()

	b1, _ := json.Marshal(s1)
	b2, _ := json.Marshal(s2)
	if string(b1) != string(b2) {
		t.Fatalf("Snapshot not deterministic: %s vs %s", b1, b2)
	}
}

// TestSyncStore_RingOnlyPeerEviction verifies that nodes added only via
// ring-change messages (never sent a heartbeat) can still be evicted.
func TestSyncStore_RingOnlyPeerEviction(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a, _ := New(memory.NewStore(), Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
		WithHeartbeatInterval(20*time.Millisecond),
		WithFailureTimeout(60*time.Millisecond),
	)
	_ = a.Connect(ctx)
	defer func() { _ = a.Close(ctx) }()

	// Manually inject a ring-change message that adds nodeC without nodeC
	// ever sending a heartbeat.
	state := RingState{
		Members: []Member{
			{ID: "nodeA", Addr: "nodeA:9000"},
			{ID: "nodeC", Addr: "nodeC:9000"},
		},
		Epoch: a.ring.Epoch() + 5,
	}
	inner, _ := json.Marshal(state)
	env := message{Type: msgRingChange, Payload: inner}
	payload, _ := json.Marshal(env)

	tr.mu.Lock()
	handlers := make([]func([]byte), len(tr.handlers))
	copy(handlers, tr.handlers)
	tr.mu.Unlock()
	for _, h := range handlers {
		h(payload)
	}

	// nodeC should now be in the ring.
	if !a.ring.Has("nodeC") {
		t.Fatal("nodeC not added via ring-change")
	}

	// Wait for failure detector to evict nodeC (it has lastSeen = now, so
	// we need to wait > failureTimeout).
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !a.ring.Has("nodeC") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("nodeC (ring-only peer) was not evicted after failure timeout")
}

// TestRing_StaleApplyRejected verifies that Apply ignores ring states whose
// epoch is not strictly greater than the current epoch.
func TestRing_StaleApplyRejected(t *testing.T) {
	r := newRing(10)
	r.Add(Member{ID: "n1", Addr: "n1:9000"})
	epoch := r.Epoch()

	// Apply a state with the same epoch — must be rejected.
	stale := RingState{
		Members: []Member{{ID: "n2", Addr: "n2:9000"}},
		Epoch:   epoch,
	}
	if r.Apply(stale) {
		t.Fatal("Apply accepted state with equal epoch (stale update)")
	}
	if r.Has("n2") {
		t.Fatal("stale Apply mutated ring members")
	}

	// Apply a state with a higher epoch — must be accepted.
	fresh := RingState{
		Members: []Member{{ID: "n1", Addr: "n1:9000"}, {ID: "n2", Addr: "n2:9000"}},
		Epoch:   epoch + 1,
	}
	if !r.Apply(fresh) {
		t.Fatal("Apply rejected state with higher epoch")
	}
	if !r.Has("n2") {
		t.Fatal("fresh Apply did not add n2 to ring")
	}
}

// TestSyncStore_EvictedNodeNotResurrected verifies that a node removed by the
// failure detector cannot be immediately re-added by a ring-change gossip
// message that still includes it (eviction greylist).
func TestSyncStore_EvictedNodeNotResurrected(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	timeout := 60 * time.Millisecond
	a, _ := New(memory.NewStore(), Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
		WithHeartbeatInterval(20*time.Millisecond),
		WithFailureTimeout(timeout),
	)
	_ = a.Connect(ctx)
	defer func() { _ = a.Close(ctx) }()

	// Inject a ring-change that adds nodeD; it never heartbeats.
	state := RingState{
		Members: []Member{
			{ID: "nodeA", Addr: "nodeA:9000"},
			{ID: "nodeD", Addr: "nodeD:9000"},
		},
		Epoch: a.ring.Epoch() + 5,
	}
	inner, _ := json.Marshal(state)
	env := message{Type: msgRingChange, Payload: inner}
	payload, _ := json.Marshal(env)
	tr.mu.Lock()
	handlers := make([]func([]byte), len(tr.handlers))
	copy(handlers, tr.handlers)
	tr.mu.Unlock()
	for _, h := range handlers {
		h(payload)
	}

	// Wait for failure detector to evict nodeD.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !a.ring.Has("nodeD") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if a.ring.Has("nodeD") {
		t.Fatal("nodeD was not evicted")
	}

	// Re-inject the same ring-change (same epoch already applied + 1 to bypass
	// epoch check). This simulates a peer that still has nodeD in its ring.
	state2 := RingState{
		Members: state.Members,
		Epoch:   a.ring.Epoch() + 1,
	}
	inner2, _ := json.Marshal(state2)
	env2 := message{Type: msgRingChange, Payload: inner2}
	payload2, _ := json.Marshal(env2)
	for _, h := range handlers {
		h(payload2)
	}

	// nodeD must NOT be resurrected within the greylist window.
	if a.ring.Has("nodeD") {
		t.Fatal("evicted nodeD was resurrected by gossip replay within greylist window")
	}
}

// TestSyncStore_CloseBeforeConnect verifies Close is safe when called on a
// store that was never Connected (ctx is initialised in New, not Connect).
func TestSyncStore_CloseBeforeConnect(t *testing.T) {
	tr := &memTransport{}
	local := memory.NewStore()
	s, err := New(local, Member{ID: "n1", Addr: "n1:9000"}, tr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close before Connect: %v", err)
	}
}

// errTransport is a Transport whose Publish always returns an error.
type errTransport struct {
	memTransport
	publishErr error
}

func (t *errTransport) Publish(_ context.Context, _ []byte) error {
	return t.publishErr
}

// TestSyncStore_PublishError verifies that transport publish errors are logged
// but do not panic or return an error from Set.
func TestSyncStore_PublishError(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tr := &errTransport{publishErr: errors.New("network down")}
	ctx := context.Background()

	local := memory.NewStore()
	s, err := New(local, Member{ID: "n1", Addr: "n1:9000"}, tr, WithLogger(discardLogger))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })

	// Set must succeed locally even if replication publish fails.
	if _, err := s.Set(ctx, "ns", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set with broken transport: %v", err)
	}
}

// healthTransport implements Transport and TransportHealthChecker.
type healthTransport struct {
	memTransport
	healthErr atomic.Pointer[error]
}

func (t *healthTransport) Health(_ context.Context) error {
	if p := t.healthErr.Load(); p != nil {
		return *p
	}
	return nil
}

// TestSyncStore_Health verifies Health checks both local store and transport.
func TestSyncStore_Health(t *testing.T) {
	tr := &healthTransport{}
	ctx := context.Background()

	s := newTestStore(t, "n1", tr)

	// Both healthy — Health must return nil.
	if err := s.Health(ctx); err != nil {
		t.Fatalf("Health healthy: %v", err)
	}

	// Transport unhealthy.
	transportErr := errors.New("redis down")
	tr.healthErr.Store(&transportErr)
	if err := s.Health(ctx); !errors.Is(err, transportErr) {
		t.Fatalf("Health with transport error: want %v, got %v", transportErr, err)
	}
}

// TestSyncStore_ConcurrentPin verifies that concurrent Pin calls on the same
// store do not race or corrupt ring state.
func TestSyncStore_ConcurrentPin(t *testing.T) {
	tr := &memTransport{}
	a := newTestStore(t, "nodeA", tr)
	newTestStore(t, "nodeB", tr)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			ns := "ns"
			if i%2 == 0 {
				a.Pin(ns, "nodeA")
			} else {
				a.Unpin(ns)
			}
		}(i)
	}
	wg.Wait()

	// Ring must still be consistent: OwnerOf returns a known node.
	id, ok := a.OwnerOf("ns")
	if ok && id != "nodeA" && id != "nodeB" {
		t.Fatalf("unexpected owner after concurrent Pin/Unpin: %q", id)
	}
}
