package peersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// testDialer routes Dial(addr) to a pre-registered config.Store.
type testDialer struct {
	mu    sync.Mutex
	peers map[string]config.Store
}

func newTestDialer() *testDialer {
	return &testDialer{peers: make(map[string]config.Store)}
}

func (d *testDialer) register(addr string, s config.Store) {
	d.mu.Lock()
	d.peers[addr] = s
	d.mu.Unlock()
}

func (d *testDialer) Dial(addr string) (config.Store, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s, ok := d.peers[addr]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("testDialer: no peer at %q", addr)
}

func newTestStore(t *testing.T, id string, tr Transport, opts ...Option) *SyncStore {
	t.Helper()
	s, err := New(memory.NewStore(), Member{ID: id, Addr: id + ":9000"}, tr, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// TestSyncStore_OwnerServesLocally verifies that the ring owner writes directly
// to its local store (single-node cluster owns all namespaces).
func TestSyncStore_OwnerServesLocally(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr)

	if _, err := a.Set(ctx, "payments", "key", config.NewValue("v")); err != nil {
		t.Fatalf("owner Set: %v", err)
	}
	got, err := a.Get(ctx, "payments", "key")
	if err != nil {
		t.Fatalf("owner Get: %v", err)
	}
	s, _ := got.String()
	if s != "v" {
		t.Fatalf("expected v, got %q", s)
	}
	if err := a.Delete(ctx, "payments", "key"); err != nil {
		t.Fatalf("owner Delete: %v", err)
	}
}

// TestSyncStore_NonOwnerRejectsWithoutDialer verifies that without a PeerDialer,
// operations routed to a live remote owner return ErrNotOwner.
func TestSyncStore_NonOwnerRejectsWithoutDialer(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr) // no dialer

	// Add nodeB as a live member and pin "payments" to it.
	// MemberOf("nodeB") will return true, triggering the no-dialer path.
	a.ring.Add(Member{ID: "nodeB", Addr: "nodeB:9000"})
	a.ring.Pin("payments", "nodeB")

	_, err := a.Set(ctx, "payments", "key", config.NewValue("v"))
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf("Set: expected ErrNotOwner, got: %v", err)
	}
	if err := a.Delete(ctx, "payments", "key"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("Delete: expected ErrNotOwner, got: %v", err)
	}
}

// TestSyncStore_NonOwnerRoutesToOwner verifies that with a PeerDialer, a
// non-owner forwards operations to the owner's store transparently.
func TestSyncStore_NonOwnerRoutesToOwner(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	dialer := newTestDialer()

	localA := memory.NewStore()
	localB := memory.NewStore()

	a, err := New(localA, Member{ID: "nodeA", Addr: "nodeA:9000"}, tr, WithPeerDialer(dialer))
	if err != nil {
		t.Fatalf("New nodeA: %v", err)
	}
	b, err := New(localB, Member{ID: "nodeB", Addr: "nodeB:9000"}, tr, WithPeerDialer(dialer))
	if err != nil {
		t.Fatalf("New nodeB: %v", err)
	}

	// Register each SyncStore so the dialer can reach it.
	// Dialer returns the SyncStore (not the raw local), so ownership is
	// re-checked at the target and the write lands in the owner's local store.
	dialer.register("nodeA:9000", a)
	dialer.register("nodeB:9000", b)

	if err := a.Connect(ctx); err != nil {
		t.Fatalf("Connect nodeA: %v", err)
	}
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect nodeB: %v", err)
	}
	t.Cleanup(func() { _ = a.Close(ctx); _ = b.Close(ctx) })

	// Wire both rings identically: nodeA owns "payments".
	a.ring.Add(Member{ID: "nodeB", Addr: "nodeB:9000"})
	b.ring.Add(Member{ID: "nodeA", Addr: "nodeA:9000"})
	a.ring.Pin("payments", "nodeA")
	b.ring.Pin("payments", "nodeA")

	// nodeB.Set("payments") must route to nodeA.
	if _, err := b.Set(ctx, "payments", "key", config.NewValue("hello")); err != nil {
		t.Fatalf("non-owner Set: %v", err)
	}

	// Value must be in nodeA's local store.
	got, err := localA.Get(ctx, "payments", "key")
	if err != nil {
		t.Fatalf("Get from owner local store: %v", err)
	}
	s, _ := got.String()
	if s != "hello" {
		t.Fatalf("expected hello in owner store, got %q", s)
	}

	// nodeB's local store must NOT have it — no replication.
	if _, err := localB.Get(ctx, "payments", "key"); !config.IsNotFound(err) {
		t.Fatalf("expected not found on non-owner local store, got: %v", err)
	}

	// nodeB.Get("payments") also routes to nodeA.
	got, err = b.Get(ctx, "payments", "key")
	if err != nil {
		t.Fatalf("non-owner Get: %v", err)
	}
	s, _ = got.String()
	if s != "hello" {
		t.Fatalf("non-owner Get: expected hello, got %q", s)
	}

	// nodeB.Delete("payments") routes to nodeA.
	if err := b.Delete(ctx, "payments", "key"); err != nil {
		t.Fatalf("non-owner Delete: %v", err)
	}
	if _, err := localA.Get(ctx, "payments", "key"); !config.IsNotFound(err) {
		t.Fatalf("expected not found in owner store after Delete, got: %v", err)
	}
}

func TestSyncStore_PinUnpin(t *testing.T) {
	tr := &memTransport{}

	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)

	// Wire both rings so each knows about the other without waiting for
	// heartbeat convergence (default interval is 5 s).
	a.ring.Add(Member{ID: "nodeB", Addr: "nodeB:9000"})
	b.ring.Add(Member{ID: "nodeA", Addr: "nodeA:9000"})

	if err := a.Pin("payments", "nodeB"); err != nil {
		t.Fatalf("Pin: %v", err)
	}

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
	if !a.HasMember("nodeB") {
		t.Fatal("nodeB not seen by nodeA after heartbeat")
	}

	// Stop nodeB without sending a graceful leave.
	b.cancel()
	b.wg.Wait()

	// Wait for nodeA's failure detector to evict nodeB.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !a.HasMember("nodeB") {
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

	if !a.HasMember("nodeC") {
		t.Fatal("nodeC not added via ring-change")
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !a.HasMember("nodeC") {
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

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !a.HasMember("nodeD") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if a.HasMember("nodeD") {
		t.Fatal("nodeD was not evicted")
	}

	// Re-inject with bumped epoch to simulate a peer that still carries nodeD.
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

	if a.HasMember("nodeD") {
		t.Fatal("evicted nodeD was resurrected by gossip replay within greylist window")
	}
}

// TestSyncStore_CloseBeforeConnect verifies Close is safe when called on a
// store that was never Connected.
func TestSyncStore_CloseBeforeConnect(t *testing.T) {
	tr := &memTransport{}
	s, err := New(memory.NewStore(), Member{ID: "n1", Addr: "n1:9000"}, tr)
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

// TestSyncStore_PublishError verifies that transport publish errors (heartbeat
// and ring-change) are logged but do not affect Set on the owning node.
func TestSyncStore_PublishError(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tr := &errTransport{publishErr: errors.New("network down")}
	ctx := context.Background()

	s, err := New(memory.NewStore(), Member{ID: "n1", Addr: "n1:9000"}, tr, WithLogger(discardLogger))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })

	// Single-node ring: n1 owns all namespaces. Set must succeed even when
	// heartbeat/ring-change publish fails.
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

	if err := s.Health(ctx); err != nil {
		t.Fatalf("Health healthy: %v", err)
	}

	transportErr := errors.New("redis down")
	tr.healthErr.Store(&transportErr)
	if err := s.Health(ctx); !errors.Is(err, transportErr) {
		t.Fatalf("Health with transport error: want %v, got %v", transportErr, err)
	}
}

// testOwnershipStore is an in-memory OwnershipStore for tests.
type testOwnershipStore struct {
	mu      sync.Mutex
	records map[string]string // namespace → nodeID
	loadErr error             // if non-nil, LoadOwned returns this error
}

func newTestOwnershipStore() *testOwnershipStore {
	return &testOwnershipStore{records: make(map[string]string)}
}

func (s *testOwnershipStore) LoadOwned(_ context.Context, nodeID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	var out []string
	for ns, id := range s.records {
		if id == nodeID {
			out = append(out, ns)
		}
	}
	return out, nil
}

func (s *testOwnershipStore) SaveOwner(_ context.Context, namespace, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[namespace] = nodeID
	return nil
}

func (s *testOwnershipStore) DeleteOwner(_ context.Context, namespace string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, namespace)
	return nil
}

// TestSyncStore_ClaimPersistsOwnership verifies that Claim writes to the
// OwnershipStore and that a new SyncStore using the same store reloads
// ownership on Connect without requiring gossip.
func TestSyncStore_ClaimPersistsOwnership(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	os := newTestOwnershipStore()

	s := newTestStore(t, "nodeA", tr, WithOwnershipStore(os))
	if err := s.Claim(ctx, "payments"); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// DB record must exist.
	if os.records["payments"] != "nodeA" {
		t.Fatalf("ownership not persisted: got %q", os.records["payments"])
	}

	// Create a second SyncStore with the same OwnershipStore (simulates restart).
	// Use a separate transport so it doesn't interfere with the first node.
	tr2 := &memTransport{}
	s2, err := New(memory.NewStore(), Member{ID: "nodeA", Addr: "nodeA:9000"}, tr2,
		WithOwnershipStore(os))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s2.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(ctx) })

	// After Connect, ownership must be restored from DB without any gossip.
	id, ok := s2.OwnerOf("payments")
	if !ok || id != "nodeA" {
		t.Fatalf("ownership not restored on reconnect: id=%q ok=%v", id, ok)
	}
}

// TestSyncStore_UnclaimRemovesOwnership verifies that Unclaim deletes the DB
// record and restores hash-ring routing.
func TestSyncStore_UnclaimRemovesOwnership(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	os := newTestOwnershipStore()

	s := newTestStore(t, "nodeA", tr, WithOwnershipStore(os))
	if err := s.Claim(ctx, "payments"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := s.Unclaim(ctx, "payments"); err != nil {
		t.Fatalf("Unclaim: %v", err)
	}

	if _, exists := os.records["payments"]; exists {
		t.Fatal("ownership record not deleted after Unclaim")
	}
}

// TestSyncStore_ReadOnlyOnDeadOwner verifies that writes return
// ErrNamespaceReadOnly when the namespace is pinned to an unreachable node,
// and that reads fall back to the local store.
func TestSyncStore_ReadOnlyOnDeadOwner(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	s := newTestStore(t, "nodeA", tr)

	// Seed local store with a value (simulates data written when nodeA was owner).
	if err := s.local.Connect(ctx); !errors.Is(err, nil) {
		// already connected via newTestStore
	}
	if _, err := s.local.Set(ctx, "payments", "tier", config.NewValue("gold")); err != nil {
		t.Fatalf("local seed: %v", err)
	}

	// Pin "payments" to a node that is not in the live ring.
	s.ring.Pin("payments", "nodeB") // nodeB is not a member

	// Write must be rejected.
	_, err := s.Set(ctx, "payments", "tier", config.NewValue("silver"))
	if !errors.Is(err, ErrNamespaceReadOnly) {
		t.Fatalf("Set: expected ErrNamespaceReadOnly, got: %v", err)
	}
	if err := s.Delete(ctx, "payments", "tier"); !errors.Is(err, ErrNamespaceReadOnly) {
		t.Fatalf("Delete: expected ErrNamespaceReadOnly, got: %v", err)
	}

	// Read must fall back to local store and return the seeded value.
	got, err := s.Get(ctx, "payments", "tier")
	if err != nil {
		t.Fatalf("Get (read-only fallback): %v", err)
	}
	v, _ := got.String()
	if v != "gold" {
		t.Fatalf("read-only fallback: expected gold, got %q", v)
	}
}

// TestSyncStore_ClaimAfterDeadOwner verifies that Claim re-enables writes
// after a namespace was in read-only state.
func TestSyncStore_ClaimAfterDeadOwner(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	s := newTestStore(t, "nodeA", tr)

	// Pin to dead node → read-only.
	s.ring.Pin("payments", "nodeB")

	if _, err := s.Set(ctx, "payments", "k", config.NewValue("v")); !errors.Is(err, ErrNamespaceReadOnly) {
		t.Fatalf("expected ErrNamespaceReadOnly before Claim, got: %v", err)
	}

	// Claim ownership back.
	if err := s.Claim(ctx, "payments"); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Write must succeed now.
	if _, err := s.Set(ctx, "payments", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set after Claim: %v", err)
	}
}

// TestSyncStore_PinRejectsUnknownTarget verifies that Pin returns an error
// when the target node is not a live ring member, preventing silent dead-owner state.
func TestSyncStore_PinRejectsUnknownTarget(t *testing.T) {
	tr := &memTransport{}
	a := newTestStore(t, "nodeA", tr)

	if err := a.Pin("payments", "ghost"); err == nil {
		t.Fatal("expected error pinning to unknown node, got nil")
	}
	// Pinning to self is always allowed.
	if err := a.Pin("payments", "nodeA"); err != nil {
		t.Fatalf("Pin to self: %v", err)
	}
}

// TestSyncStore_ConnectIdempotent verifies that a second Connect call returns
// an error without spawning duplicate goroutines.
func TestSyncStore_ConnectIdempotent(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr) // already connected

	if err := s.Connect(ctx); !errors.Is(err, ErrAlreadyConnected) {
		t.Fatalf("second Connect: expected ErrAlreadyConnected, got: %v", err)
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
				_ = a.Pin(ns, "nodeA") // nodeA is self, always valid
			} else {
				a.Unpin(ns)
			}
		}(i)
	}
	wg.Wait()

	id, ok := a.OwnerOf("ns")
	if ok && id != "nodeA" && id != "nodeB" {
		t.Fatalf("unexpected owner after concurrent Pin/Unpin: %q", id)
	}
}

// TestSyncStore_ConcurrentSetAndRingChange verifies that concurrent Set calls
// and inbound ring-change Apply do not race. The race detector is the
// primary assertion here.
func TestSyncStore_ConcurrentSetAndRingChange(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	dialer := newTestDialer()

	localA := memory.NewStore()
	a, err := New(localA, Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
		WithPeerDialer(dialer),
		WithHeartbeatInterval(time.Hour),          // disable background noise
		WithFailureTimeout(4*time.Hour),            // satisfy 3× ratio invariant
	)
	if err != nil {
		t.Fatalf("New nodeA: %v", err)
	}
	if err := a.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = a.Close(ctx) })

	// Pin "ns" to nodeA so Set goes to the local store.
	a.ring.Pin("ns", "nodeA")

	// Build two ring states to alternate between.
	snap1 := a.ring.Snapshot()
	snap1.Epoch = 100

	snap2 := snap1
	snap2.Epoch = 101

	const iters = 200
	var wg sync.WaitGroup

	// Writer goroutine: repeatedly Set a key.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iters {
			_, _ = a.Set(ctx, "ns", "k", config.NewValue("v"))
		}
	}()

	// Ring-change goroutine: rapidly apply alternating ring states to exercise
	// ownerStore resolution racing with Apply.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iters {
			if i%2 == 0 {
				a.ring.Apply(snap1)
			} else {
				a.ring.Apply(snap2)
			}
		}
	}()

	wg.Wait()
}

// TestSyncStore_ConnectLoadOwnedError verifies that a LoadOwned failure during
// Connect returns an error and does not leave background goroutines running.
func TestSyncStore_ConnectLoadOwnedError(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	os := &testOwnershipStore{loadErr: fmt.Errorf("db unavailable")}
	s, err := New(memory.NewStore(), Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
		WithOwnershipStore(os),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Connect(ctx); err == nil {
		t.Fatal("expected Connect to fail when LoadOwned errors, got nil")
	}
	// A second attempt should return ErrAlreadyConnected (connected flag is set
	// on the first attempt), confirming no partial state is exposed.
	if err := s.Connect(ctx); !errors.Is(err, ErrAlreadyConnected) {
		t.Errorf("second Connect: expected ErrAlreadyConnected, got %v", err)
	}
}

// TestSyncStore_PublishTimeoutIndependentOfHeartbeat verifies that
// WithPublishTimeout applied before WithHeartbeatInterval is not overwritten.
func TestSyncStore_PublishTimeoutIndependentOfHeartbeat(t *testing.T) {
	tr := &memTransport{}
	s, err := New(memory.NewStore(), Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
		WithPublishTimeout(500*time.Millisecond),
		WithHeartbeatInterval(10*time.Second),
		WithFailureTimeout(30*time.Second), // satisfy 3× ratio invariant
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close(context.Background())
	if s.opts.publishTimeout != 500*time.Millisecond {
		t.Errorf("publishTimeout = %v, want 500ms", s.opts.publishTimeout)
	}
	if s.opts.heartbeatInterval != 10*time.Second {
		t.Errorf("heartbeatInterval = %v, want 10s", s.opts.heartbeatInterval)
	}
}

// TestSyncStore_GracefulLeave verifies that Close removes self from the ring
// before shutting down, so peers learn of the departure without waiting for
// the failure timeout.
func TestSyncStore_GracefulLeave(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()

	a := newTestStore(t, "nodeA", tr)
	b := newTestStore(t, "nodeB", tr)

	// Align both rings to a common high-epoch state so the leave announcement
	// (which bumps the epoch by one) is guaranteed to be accepted by nodeB.
	sharedState := RingState{
		Members: []Member{
			{ID: "nodeA", Addr: "nodeA:9000"},
			{ID: "nodeB", Addr: "nodeB:9000"},
		},
		Epoch: 100,
	}
	a.ring.Apply(sharedState)
	b.ring.Apply(sharedState)

	if !b.HasMember("nodeA") {
		t.Fatal("nodeB does not have nodeA before test; ring setup failed")
	}

	// Close nodeA — should broadcast a leave with epoch 101.
	if err := a.Close(ctx); err != nil {
		t.Fatalf("Close nodeA: %v", err)
	}

	// nodeB must have received the leave announcement and removed nodeA.
	if b.HasMember("nodeA") {
		t.Error("nodeB still has nodeA in ring after graceful leave")
	}
}

// TestSyncStore_CloseIdempotent verifies that Close can be called twice
// without panicking or returning an error.
func TestSyncStore_CloseIdempotent(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr)

	if err := s.Close(ctx); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- optional interface tests ---

// TestSyncStore_Find verifies that Find on an owned namespace queries the
// local store and returns the expected page.
func TestSyncStore_Find(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr)

	if _, err := s.Set(ctx, "ns", "k1", config.NewValue("a")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	page, err := s.Find(ctx, "ns", config.NewFilter().Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(page.Results()) == 0 {
		t.Fatal("expected at least one result, got empty page")
	}
}

// TestSyncStore_Watch verifies that Watch returns a channel that receives
// change events from the local store.
func TestSyncStore_Watch(t *testing.T) {
	tr := &memTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t, "nodeA", tr)

	ch, err := s.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if ch == nil {
		t.Fatal("Watch returned nil channel")
	}
	// Trigger an event and verify it arrives.
	if _, err := s.Set(ctx, "ns", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	select {
	case e := <-ch:
		if e.Key != "k" {
			t.Errorf("event key = %q, want %q", e.Key, "k")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for watch event")
	}
}

// TestSyncStore_WithVNodes verifies that WithVNodes is applied to the ring.
func TestSyncStore_WithVNodes(t *testing.T) {
	tr := &memTransport{}
	s, err := New(memory.NewStore(), Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
		WithVNodes(10),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.ring.vnodes != 10 {
		t.Errorf("vnodes = %d, want 10", s.ring.vnodes)
	}
}

// TestSyncStore_WithErrorHandler verifies that a custom error handler is
// invoked for non-fatal gossip errors instead of the default logger.
func TestSyncStore_WithErrorHandler(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	var captured []error
	var mu sync.Mutex

	s := newTestStore(t, "nodeA", tr, WithErrorHandler(func(err error) {
		mu.Lock()
		captured = append(captured, err)
		mu.Unlock()
	}))

	// Inject a malformed envelope directly through the handler.
	s.handleMessage([]byte("not-json"))

	mu.Lock()
	n := len(captured)
	mu.Unlock()

	if n == 0 {
		t.Error("expected error handler to be called for malformed envelope, got 0 calls")
	}
	_ = s.Close(ctx)
}

// TestSyncStore_BulkStore_GetSetDeleteMany verifies BulkStore routing for the
// owner node.
func TestSyncStore_BulkStore_GetSetDeleteMany(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr)

	vals := map[string]config.Value{
		"k1": config.NewValue("v1"),
		"k2": config.NewValue("v2"),
	}
	if err := s.SetMany(ctx, "ns", vals); err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	got, err := s.GetMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("GetMany returned %d items, want 2", len(got))
	}

	n, err := s.DeleteMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if n != 2 {
		t.Errorf("DeleteMany removed %d items, want 2", n)
	}
}

// TestSyncStore_BulkStore_NonOwnerForwards verifies that BulkStore operations
// on a non-owned namespace are forwarded via PeerDialer.
func TestSyncStore_BulkStore_NonOwnerForwards(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	dialer := newTestDialer()

	localA := memory.NewStore()
	a, err := New(localA, Member{ID: "nodeA", Addr: "nodeA:9000"}, tr, WithPeerDialer(dialer))
	if err != nil {
		t.Fatalf("New nodeA: %v", err)
	}
	b, err := New(memory.NewStore(), Member{ID: "nodeB", Addr: "nodeB:9000"}, tr, WithPeerDialer(dialer))
	if err != nil {
		t.Fatalf("New nodeB: %v", err)
	}
	dialer.register("nodeA:9000", a)
	dialer.register("nodeB:9000", b)

	if err := a.Connect(ctx); err != nil {
		t.Fatalf("Connect a: %v", err)
	}
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect b: %v", err)
	}
	t.Cleanup(func() { _ = a.Close(ctx); _ = b.Close(ctx) })

	a.ring.Add(Member{ID: "nodeB", Addr: "nodeB:9000"})
	b.ring.Add(Member{ID: "nodeA", Addr: "nodeA:9000"})
	a.ring.Pin("ns", "nodeA")
	b.ring.Pin("ns", "nodeA")

	// SetMany from nodeB must land in nodeA's local store.
	vals := map[string]config.Value{"bulk": config.NewValue("yes")}
	if err := b.SetMany(ctx, "ns", vals); err != nil {
		t.Fatalf("SetMany non-owner: %v", err)
	}
	got, err := localA.Get(ctx, "ns", "bulk")
	if err != nil {
		t.Fatalf("Get from owner: %v", err)
	}
	s, _ := got.String()
	if s != "yes" {
		t.Errorf("got %q, want %q", s, "yes")
	}
}

// TestSyncStore_Stats verifies Stats is served from the local store.
func TestSyncStore_Stats(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr)

	if _, err := s.Set(ctx, "ns", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
}

// TestSyncStore_AliasStore verifies SetAlias / GetAlias / DeleteAlias /
// ListAliases routing on the owner node.
func TestSyncStore_AliasStore(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr)

	// Create an alias. SetAlias routes by alias path (treated as namespace key).
	// The alias maps "db.host" → "database/host".
	if _, err := s.Set(ctx, "database", "host", config.NewValue("localhost")); err != nil {
		t.Fatalf("Set target: %v", err)
	}
	if _, err := s.SetAlias(ctx, "db.host", "database/host"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	got, err := s.GetAlias(ctx, "db.host")
	if err != nil {
		t.Fatalf("GetAlias: %v", err)
	}
	if got == nil {
		t.Fatal("GetAlias returned nil")
	}

	aliases, err := s.ListAliases(ctx)
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	if _, ok := aliases["db.host"]; !ok {
		t.Error("ListAliases: expected db.host in result")
	}

	if err := s.DeleteAlias(ctx, "db.host"); err != nil {
		t.Fatalf("DeleteAlias: %v", err)
	}
	if _, err := s.GetAlias(ctx, "db.host"); err == nil {
		t.Error("GetAlias after DeleteAlias: expected error, got nil")
	}
}

// TestSyncStore_GetVersions verifies GetVersions routing on the owner node.
func TestSyncStore_GetVersions(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr)

	if _, err := s.Set(ctx, "ns", "k", config.NewValue("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	if _, err := s.Set(ctx, "ns", "k", config.NewValue("v2")); err != nil {
		t.Fatalf("Set v2: %v", err)
	}

	page, err := s.GetVersions(ctx, "ns", "k", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if page == nil {
		t.Fatal("GetVersions returned nil page")
	}
}

// TestSyncStore_OptionalInterfaces_NoDialerReturnsError verifies that
// BulkStore/VersionedStore/AliasStore operations return ErrNotOwner when the
// namespace is owned by a live remote node and no PeerDialer is configured.
func TestSyncStore_OptionalInterfaces_NoDialerReturnsError(t *testing.T) {
	tr := &memTransport{}
	ctx := context.Background()
	s := newTestStore(t, "nodeA", tr) // no dialer

	// Pin "ns" to a live remote node.
	s.ring.Add(Member{ID: "nodeB", Addr: "nodeB:9000"})
	s.ring.Pin("ns", "nodeB")

	if err := s.SetMany(ctx, "ns", map[string]config.Value{"k": config.NewValue("v")}); !errors.Is(err, ErrNotOwner) {
		t.Errorf("SetMany: expected ErrNotOwner, got %v", err)
	}
	if _, err := s.GetMany(ctx, "ns", []string{"k"}); err == nil {
		t.Error("GetMany: expected error for non-owned namespace with no dialer")
	}
	if _, err := s.GetVersions(ctx, "ns", "k", config.NewVersionFilter().Build()); err == nil {
		t.Error("GetVersions: expected error for non-owned namespace with no dialer")
	}
}
