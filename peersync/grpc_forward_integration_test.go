package peersync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/client"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// grpcCodeOf extracts the gRPC status code from err if it carries one. The
// forward path (client.RemoteStore) usually translates gRPC statuses into
// config sentinel errors, in which case status.FromError reports codes.Unknown;
// callers therefore treat the returned code as advisory and prefer sentinel
// checks via errors.Is.
func grpcCodeOf(err error) codes.Code {
	st, ok := status.FromError(err)
	if !ok {
		return codes.Unknown
	}
	return st.Code()
}

// isUnavailableClass reports whether err represents an "owner unreachable"
// failure, tolerant of how the forward path surfaces it: either the
// config.ErrStoreNotConnected sentinel (the client's mapping of
// codes.Unavailable) or a raw gRPC codes.Unavailable status.
func isUnavailableClass(err error) bool {
	return errors.Is(err, config.ErrStoreNotConnected) || grpcCodeOf(err) == codes.Unavailable
}

// isDeadlineClass reports whether err represents a deadline-exceeded failure,
// tolerant of either context.DeadlineExceeded (the client's mapping of
// codes.DeadlineExceeded) or a raw gRPC codes.DeadlineExceeded status.
func isDeadlineClass(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || grpcCodeOf(err) == codes.DeadlineExceeded
}

// bufconnRegistry maps a peer address string to its bufconn listener so that
// the real GRPCDialer (which calls grpc.NewClient under the hood) can be routed
// to the correct in-process node. The dialer ignores the target string passed
// to grpc.NewClient and instead resolves the original address recorded at
// Dial() time via a context dialer closure per address.
type bufconnRegistry struct {
	mu        sync.Mutex
	listeners map[string]*bufconn.Listener
}

func newBufconnRegistry() *bufconnRegistry {
	return &bufconnRegistry{listeners: make(map[string]*bufconn.Listener)}
}

func (r *bufconnRegistry) add(addr string, lis *bufconn.Listener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listeners[addr] = lis
}

func (r *bufconnRegistry) get(addr string) (*bufconn.Listener, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lis, ok := r.listeners[addr]
	return lis, ok
}

// registryDialer wraps the real client.GRPCDialer but injects, per address, a
// gRPC context dialer that connects to the in-process bufconn listener
// registered for that address. This exercises the production GRPCDialer ->
// client.NewRemoteStore -> Connect -> gRPC forwarding path end to end; only the
// physical transport is swapped for bufconn.
type registryDialer struct {
	reg *bufconnRegistry
	mu  sync.Mutex
	d   map[string]*GRPCDialer // one real GRPCDialer per address (each with its own context dialer)
}

func newRegistryDialer(reg *bufconnRegistry) *registryDialer {
	return &registryDialer{reg: reg, d: make(map[string]*GRPCDialer)}
}

func (rd *registryDialer) Dial(addr string) (config.Store, error) {
	rd.mu.Lock()
	gd, ok := rd.d[addr]
	if !ok {
		lis, found := rd.reg.get(addr)
		if !found {
			rd.mu.Unlock()
			return nil, fmt.Errorf("registryDialer: no listener for %q", addr)
		}
		// Build a genuine GRPCDialer whose connections are routed to this
		// address' bufconn listener. NewGRPCDialer/Dial is the production path.
		gd = NewGRPCDialer(
			client.WithInsecure(),
			client.WithRetry(0, 0, 0),
			client.WithDialOptions(
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return lis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			),
		)
		rd.d[addr] = gd
	}
	rd.mu.Unlock()
	return gd.Dial(addr)
}

func (rd *registryDialer) Close() error {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	for _, gd := range rd.d {
		_ = gd.Close()
	}
	return nil
}

// startNode wires a SyncStore over a fresh memory store and serves it on a
// bufconn listener via the real ConfigService, returning the node and its
// backing local store. The node uses the supplied PeerDialer for forwarding.
func startNode(t *testing.T, reg *bufconnRegistry, dialer PeerDialer, id, addr string) (*SyncStore, config.Store) {
	t.Helper()

	local := memory.NewStore()
	node, err := New(local, Member{ID: id, Addr: addr}, &memTransport{}, WithPeerDialer(dialer))
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	if err := node.Connect(context.Background()); err != nil {
		t.Fatalf("Connect(%s): %v", id, err)
	}

	// Serve the SyncStore (not the raw local store) so that a forwarded op is
	// re-checked for ownership at the target node before landing locally.
	svc, err := service.NewService(node, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService(%s): %v", id, err)
	}
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	configpb.RegisterConfigServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	reg.add(addr, lis)

	t.Cleanup(func() {
		srv.Stop()
		_ = node.Close(context.Background())
	})
	return node, local
}

// TestGRPCDialer_ForwardAcrossBufconnNodes stands up two in-process SyncStore
// nodes connected by the real GRPCDialer over bufconn. A non-owner write is
// forwarded to the owner via the GRPCDialer -> client.RemoteStore path, and the
// op is asserted to land in the owner's local store (and not the non-owner's).
func TestGRPCDialer_ForwardAcrossBufconnNodes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg := newBufconnRegistry()
	dialer := newRegistryDialer(reg)
	t.Cleanup(func() { _ = dialer.Close() })

	// Addresses carry the passthrough:/// scheme so grpc.NewClient skips DNS
	// name resolution and uses the per-address bufconn context dialer verbatim.
	// In a real deployment these would be plain host:port strings resolvable
	// via DNS; bufconn has no DNS, so passthrough is the in-process equivalent.
	const (
		addrA = "passthrough:///nodeA:9000"
		addrB = "passthrough:///nodeB:9000"
	)

	nodeA, localA := startNode(t, reg, dialer, "nodeA", addrA)
	nodeB, localB := startNode(t, reg, dialer, "nodeB", addrB)

	// Wire both rings identically: nodeA owns "payments". Each node must know
	// the other as a live member so ownerStore dials rather than returning
	// ErrNotOwner / ErrNamespaceReadOnly.
	nodeA.ring.Add(Member{ID: "nodeB", Addr: addrB})
	nodeB.ring.Add(Member{ID: "nodeA", Addr: addrA})
	nodeA.ring.Pin("payments", "nodeA")
	nodeB.ring.Pin("payments", "nodeA")

	// nodeB is NOT the owner of "payments": this Set must be serialized, dialed
	// to nodeA over bufconn through the real GRPCDialer, and re-checked for
	// ownership at nodeA before landing in nodeA's local store.
	if _, err := nodeB.Set(ctx, "payments", "db.host", config.NewValue("10.0.0.1")); err != nil {
		t.Fatalf("non-owner Set via GRPCDialer: %v", err)
	}

	// The write must be in nodeA's local store.
	got, err := localA.Get(ctx, "payments", "db.host")
	if err != nil {
		t.Fatalf("owner local Get: %v", err)
	}
	if s, _ := got.String(); s != "10.0.0.1" {
		t.Fatalf("owner local value = %q, want 10.0.0.1", s)
	}

	// And must NOT be in nodeB's local store — no replication, pure forwarding.
	if _, err := localB.Get(ctx, "payments", "db.host"); !config.IsNotFound(err) {
		t.Fatalf("non-owner local store should not hold the key, got err=%v", err)
	}

	// A forwarded read from nodeB must also resolve to nodeA's value.
	got, err = nodeB.Get(ctx, "payments", "db.host")
	if err != nil {
		t.Fatalf("non-owner Get via GRPCDialer: %v", err)
	}
	if s, _ := got.String(); s != "10.0.0.1" {
		t.Fatalf("forwarded Get value = %q, want 10.0.0.1", s)
	}

	// A forwarded delete from nodeB must remove it from nodeA's local store.
	if err := nodeB.Delete(ctx, "payments", "db.host"); err != nil {
		t.Fatalf("non-owner Delete via GRPCDialer: %v", err)
	}
	if _, err := localA.Get(ctx, "payments", "db.host"); !config.IsNotFound(err) {
		t.Fatalf("owner local store should be empty after forwarded Delete, got err=%v", err)
	}

	// Sanity: the owner serves its own namespace locally without forwarding.
	if _, err := nodeA.Set(ctx, "payments", "local.key", config.NewValue("v")); err != nil {
		t.Fatalf("owner local Set: %v", err)
	}
	if _, err := localA.Get(ctx, "payments", "local.key"); err != nil {
		t.Fatalf("owner local Get after local Set: %v", err)
	}
}

// startNodeServer is like startNode but returns the *grpc.Server so the caller
// can Stop() it mid-test to simulate an owner crash. The server and node are
// still registered for cleanup, and Stop is idempotent so an explicit mid-test
// Stop followed by the cleanup Stop is safe.
func startNodeServer(t *testing.T, reg *bufconnRegistry, dialer PeerDialer, id, addr string) (*SyncStore, config.Store, *grpc.Server) {
	t.Helper()

	local := memory.NewStore()
	node, err := New(local, Member{ID: id, Addr: addr}, &memTransport{}, WithPeerDialer(dialer))
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	if err := node.Connect(context.Background()); err != nil {
		t.Fatalf("Connect(%s): %v", id, err)
	}

	svc, err := service.NewService(node, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService(%s): %v", id, err)
	}
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	configpb.RegisterConfigServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	reg.add(addr, lis)

	t.Cleanup(func() {
		srv.Stop()
		_ = node.Close(context.Background())
	})
	return node, local, srv
}

// runtimeNode wires two nodes that both pin "payments" to nodeA so writes from
// the non-owner are forwarded over the bufconn GRPCDialer to nodeA. It returns
// the non-owner node, the owner node, and the owner's *grpc.Server.
func twoNodeForwardCluster(t *testing.T, reg *bufconnRegistry, dialer PeerDialer, addrA, addrB string) (nodeA, nodeB *SyncStore, srvA *grpc.Server) {
	t.Helper()
	nodeA, _, srvA = startNodeServer(t, reg, dialer, "nodeA", addrA)
	nodeB, _ = startNode(t, reg, dialer, "nodeB", addrB)

	nodeA.ring.Add(Member{ID: "nodeB", Addr: addrB})
	nodeB.ring.Add(Member{ID: "nodeA", Addr: addrA})
	nodeA.ring.Pin("payments", "nodeA")
	nodeB.ring.Pin("payments", "nodeA")
	return nodeA, nodeB, srvA
}

// TestGRPCDialer_ForwardToMissingOwner exercises fault mode (a): the owner's
// address is a live ring member but no gRPC server was ever started for it.
// The forwarded write must surface an error promptly (bounded context) without
// hanging, and the error must be the "owner unreachable" class.
func TestGRPCDialer_ForwardToMissingOwner(t *testing.T) {
	t.Parallel()

	reg := newBufconnRegistry()
	dialer := newRegistryDialer(reg)
	t.Cleanup(func() { _ = dialer.Close() })

	const (
		addrA = "passthrough:///nodeA:9000"
		addrB = "passthrough:///nodeB:9000"
	)

	// nodeB is the only real node; it pins "payments" to nodeA, which is a ring
	// member but has NO listener registered and NO server started.
	nodeB, _ := startNode(t, reg, dialer, "nodeB", addrB)
	nodeB.ring.Add(Member{ID: "nodeA", Addr: addrA})
	nodeB.ring.Pin("payments", "nodeA")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := nodeB.Set(ctx, "payments", "db.host", config.NewValue("10.0.0.1"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("forward to missing owner: expected an error, got nil")
		}
		// The dialer cannot resolve a listener for addrA, so the error surfaces
		// from the dial step. We only require that it is a non-nil error that
		// arrived promptly (the select below would have failed otherwise).
		t.Logf("forward to missing owner returned: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("forward to missing owner hung: no error within 2s")
	}
}

// TestGRPCDialer_ForwardToUnservedListener exercises fault mode (a) variant:
// the owner has a registered bufconn listener but no server is accepting on it
// (Serve was never called). The dial succeeds lazily but the RPC must fail with
// the unavailable class and must not hang.
func TestGRPCDialer_ForwardToUnservedListener(t *testing.T) {
	t.Parallel()

	reg := newBufconnRegistry()
	dialer := newRegistryDialer(reg)
	t.Cleanup(func() { _ = dialer.Close() })

	const (
		addrA = "passthrough:///nodeA:9000"
		addrB = "passthrough:///nodeB:9000"
	)

	// Register a listener for nodeA but never Serve it: dials connect at the
	// transport level but no gRPC server responds.
	deadLis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = deadLis.Close() })
	reg.add(addrA, deadLis)

	nodeB, _ := startNode(t, reg, dialer, "nodeB", addrB)
	nodeB.ring.Add(Member{ID: "nodeA", Addr: addrA})
	nodeB.ring.Pin("payments", "nodeA")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := nodeB.Set(ctx, "payments", "db.host", config.NewValue("10.0.0.1"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("forward to unserved listener: expected an error, got nil")
		}
		// Either the connection never becomes ready (Unavailable) or the bounded
		// context fires first (DeadlineExceeded); both are acceptable, neither
		// must hang.
		if !isUnavailableClass(err) && !isDeadlineClass(err) {
			t.Fatalf("forward to unserved listener: want Unavailable/DeadlineExceeded class, got %v (grpc code=%s)", err, grpcCodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("forward to unserved listener hung: no error within 2s")
	}
}

// TestGRPCDialer_ForwardToStoppedOwner exercises fault mode (b): the owner's
// gRPC server is Stop()'d mid-test. A subsequent forwarded write must surface
// the unavailable class (config.ErrStoreNotConnected / codes.Unavailable) and
// must not hang or corrupt state.
func TestGRPCDialer_ForwardToStoppedOwner(t *testing.T) {
	t.Parallel()

	reg := newBufconnRegistry()
	dialer := newRegistryDialer(reg)
	t.Cleanup(func() { _ = dialer.Close() })

	const (
		addrA = "passthrough:///nodeA:9000"
		addrB = "passthrough:///nodeB:9000"
	)

	nodeA, nodeB, srvA := twoNodeForwardCluster(t, reg, dialer, addrA, addrB)
	_ = nodeA

	// First, a forwarded write succeeds while the owner is alive: this primes
	// the cached GRPCDialer connection so the later failure is purely the owner
	// going away, not a cold-dial failure.
	okCtx, okCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if _, err := nodeB.Set(okCtx, "payments", "k1", config.NewValue("v1")); err != nil {
		okCancel()
		t.Fatalf("priming forward write: %v", err)
	}
	okCancel()

	// Stop the owner's server: the cached connection is now backed by a dead
	// endpoint.
	srvA.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := nodeB.Set(ctx, "payments", "k2", config.NewValue("v2"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("forward to stopped owner: expected an error, got nil")
		}
		if !isUnavailableClass(err) && !isDeadlineClass(err) {
			t.Fatalf("forward to stopped owner: want Unavailable/DeadlineExceeded class, got %v (grpc code=%s)", err, grpcCodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("forward to stopped owner hung: no error within 2s")
	}
}

// TestGRPCDialer_ForwardDeadlineExceeded exercises fault mode (c): the caller
// supplies an already-expired context. The forwarded call must surface the
// deadline-exceeded class and return immediately.
func TestGRPCDialer_ForwardDeadlineExceeded(t *testing.T) {
	t.Parallel()

	reg := newBufconnRegistry()
	dialer := newRegistryDialer(reg)
	t.Cleanup(func() { _ = dialer.Close() })

	const (
		addrA = "passthrough:///nodeA:9000"
		addrB = "passthrough:///nodeB:9000"
	)

	_, nodeB, _ := twoNodeForwardCluster(t, reg, dialer, addrA, addrB)

	// Context that is already past its deadline.
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // ensure the deadline has elapsed

	done := make(chan error, 1)
	go func() {
		_, err := nodeB.Set(ctx, "payments", "deadline.key", config.NewValue("v"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("forward with expired context: expected an error, got nil")
		}
		if !isDeadlineClass(err) {
			t.Fatalf("forward with expired context: want DeadlineExceeded class, got %v (grpc code=%s)", err, grpcCodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("forward with expired context hung: no error within 2s")
	}
}

// TestGRPCDialer_ForwardConditionalCreateCollision exercises a forwarded
// conditional write: a WriteModeCreate for a key the owner already holds must
// surface config.ErrKeyExists across the GRPCDialer forward path. This proves
// the write-mode flag is preserved over the wire and that AlreadyExists is
// mapped back to the config sentinel on the non-owner side.
func TestGRPCDialer_ForwardConditionalCreateCollision(t *testing.T) {
	t.Parallel()

	reg := newBufconnRegistry()
	dialer := newRegistryDialer(reg)
	t.Cleanup(func() { _ = dialer.Close() })

	const (
		addrA = "passthrough:///nodeA:9000"
		addrB = "passthrough:///nodeB:9000"
	)

	nodeA, nodeB, _ := twoNodeForwardCluster(t, reg, dialer, addrA, addrB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Seed the key on the owner first (a plain upsert).
	if _, err := nodeA.Set(ctx, "payments", "lock", config.NewValue("owner")); err != nil {
		t.Fatalf("seed owner key: %v", err)
	}

	// Now forward a create-only write for the same key from the non-owner. The
	// owner already holds it, so the create must collide.
	createVal := config.NewValue("intruder", config.WithValueWriteMode(config.WriteModeCreate))
	_, err := nodeB.Set(ctx, "payments", "lock", createVal)
	if err == nil {
		t.Fatal("forwarded create collision: expected ErrKeyExists, got nil")
	}
	if !config.IsKeyExists(err) {
		t.Fatalf("forwarded create collision: want config.ErrKeyExists, got %v (grpc code=%s)", err, grpcCodeOf(err))
	}

	// The owner's original value must be untouched by the failed create.
	got, err := nodeA.Get(ctx, "payments", "lock")
	if err != nil {
		t.Fatalf("owner Get after failed create: %v", err)
	}
	if s, _ := got.String(); s != "owner" {
		t.Fatalf("owner value mutated by failed create: got %q, want %q", s, "owner")
	}
}

// TestForward_GossipError_Observable verifies the gossip layer is observable:
// a malformed inbound peer message is surfaced through the configured error
// handler rather than being silently swallowed. This is the diagnostic signal
// operators rely on for the untrusted peer-to-peer wire — without it, a node
// quietly dropping bad gossip would be invisible.
func TestForward_GossipError_Observable(t *testing.T) {
	tr := &memTransport{}

	var (
		mu   sync.Mutex
		errs []error
	)
	// newTestStore Connects (subscribing to the transport) and registers cleanup.
	newTestStore(t, "nodeObs", tr, WithErrorHandler(func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}))

	// memTransport delivers synchronously, so once Publish returns the inbound
	// handler (and thus the error report) has already run — no sleep needed.
	if err := tr.Publish(context.Background(), []byte("{not a valid gossip envelope")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	mu.Lock()
	got := len(errs)
	mu.Unlock()
	if got == 0 {
		t.Fatal("malformed inbound gossip message was not surfaced via the error handler")
	}
}
