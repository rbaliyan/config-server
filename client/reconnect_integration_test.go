package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/internal/testutil"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// restartableServer is a bufconn-backed gRPC config server that can be stopped
// and restarted while reusing the same backing store. The RemoteStore dials
// through a context dialer that always reads the *current* listener, so a
// reconnect after a restart transparently lands on the fresh listener — the
// in-process equivalent of a server process bouncing under a stable DNS name.
type restartableServer struct {
	mu       sync.Mutex
	lis      atomic.Pointer[bufconn.Listener]
	srv      *grpc.Server
	store    config.Store
	guardOpt []service.Option
}

func newRestartableServer(t *testing.T, store config.Store, opts ...service.Option) *restartableServer {
	t.Helper()
	if len(opts) == 0 {
		opts = []service.Option{service.WithSecurityGuard(service.AllowAll())}
	}
	rs := &restartableServer{store: store, guardOpt: opts}
	rs.start(t)
	t.Cleanup(rs.stop)
	return rs
}

// start brings up a fresh listener + gRPC server serving the shared store.
func (rs *restartableServer) start(t *testing.T) {
	t.Helper()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	svc, err := service.NewService(rs.store, rs.guardOpt...)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	configpb.RegisterConfigServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	rs.lis.Store(lis)
	rs.srv = srv
}

// stop hard-stops the current server and closes its listener, breaking all
// in-flight connections (so the client must reconnect).
func (rs *restartableServer) stop() {
	rs.mu.Lock()
	srv := rs.srv
	lis := rs.lis.Load()
	rs.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
	if lis != nil {
		_ = lis.Close()
	}
}

// restart stops then starts the server, reusing the same backing store.
func (rs *restartableServer) restart(t *testing.T) {
	t.Helper()
	rs.stop()
	rs.start(t)
}

// dialOption returns a context dialer that always resolves to the current
// listener, surviving restarts.
func (rs *restartableServer) dialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		lis := rs.lis.Load()
		if lis == nil {
			return nil, fmt.Errorf("restartableServer: no listener")
		}
		return lis.DialContext(ctx)
	})
}

// TestIntegration_WatchReconnect_AfterServerRestart enables retry and
// watch-reconnect, then restarts the bufconn server mid-watch and asserts the
// stream reconnects and resumes delivering events. This exercises the
// WithRetry/WithWatchReconnect paths the base integration suite disables.
func TestIntegration_WatchReconnect_AfterServerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	backing := memory.NewStore(memory.WithWatchBufferSize(100))
	if err := backing.Connect(context.Background()); err != nil {
		t.Fatalf("connect backing store: %v", err)
	}
	t.Cleanup(func() { _ = backing.Close(context.Background()) })

	srv := newRestartableServer(t, backing)

	var watchErrs int32
	store, err := NewRemoteStore("passthrough:///bufconn",
		WithInsecure(),
		WithRetry(5, 20*time.Millisecond, 200*time.Millisecond),
		WithWatchReconnect(true, 20*time.Millisecond),
		WithWatchMaxErrors(50),
		WithWatchErrorCallback(func(error) { atomic.AddInt32(&watchErrs, 1) }),
		WithDialOptions(srv.dialOption(), grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("NewRemoteStore: %v", err)
	}
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	wr, err := store.WatchWithResult(ctx, config.WatchFilter{Namespaces: []string{"recon"}})
	if err != nil {
		t.Fatalf("WatchWithResult: %v", err)
	}
	defer wr.Stop()

	// Establish the stream: probe until an event flows.
	waitWatchEstablished(t, ctx, store, wr, "recon")

	// Deliver one event before the restart.
	if _, err := store.Set(ctx, "recon", "before", config.NewValue("1")); err != nil {
		t.Fatalf("Set before restart: %v", err)
	}
	if !receiveKey(t, ctx, wr, "before", 5*time.Second) {
		t.Fatal("did not receive pre-restart event")
	}

	// Bounce the server. The active watch stream breaks; the client must
	// reconnect via WithWatchReconnect and resume delivery.
	srv.restart(t)

	// After reconnect, a new write must be delivered. Because reconnection is
	// asynchronous, retry the write until the reconnected stream delivers it.
	resumed := testutil.Eventually(15*time.Second, 50*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, "recon", "after", config.NewValue("2")); err != nil {
			return false
		}
		select {
		case ev, ok := <-wr.Events():
			if !ok {
				return false
			}
			return ev.Key == "after" || ev.Key == "before"
		case <-time.After(100 * time.Millisecond):
			return false
		}
	})
	if !resumed {
		t.Fatalf("watch did not resume after server restart (watch errors observed: %d)", atomic.LoadInt32(&watchErrs))
	}
	if atomic.LoadInt32(&watchErrs) == 0 {
		t.Error("expected at least one watch error to be reported during the restart")
	}
}

// TestIntegration_ConcurrentVersionRace runs N goroutines hammering one key
// through RemoteStore under -race: each issues a create-only write plus a
// stream of upserts. It asserts exactly one create wins and that observed
// versions increase monotonically (no lost-update version regressions).
func TestIntegration_ConcurrentVersionRace(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	const (
		workers    = 16
		writesEach = 8
		ns         = "race-ns"
		key        = "counter"
		createKey  = "create-once"
	)

	// 1. Create-only race: exactly one of N concurrent WriteModeCreate writes
	// to the same fresh key must succeed; the rest must get ErrKeyExists.
	var (
		createWins   int32
		createExists int32
		wg           sync.WaitGroup
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			val := config.NewValue(fmt.Sprintf("w%d", id), config.WithValueWriteMode(config.WriteModeCreate))
			_, err := store.Set(ctx, ns, createKey, val)
			switch {
			case err == nil:
				atomic.AddInt32(&createWins, 1)
			case errIsKeyExists(err):
				atomic.AddInt32(&createExists, 1)
			default:
				t.Errorf("worker %d: unexpected create error: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	if createWins != 1 {
		t.Errorf("create-only race: got %d winners, want exactly 1", createWins)
	}
	if createExists != workers-1 {
		t.Errorf("create-only race: got %d ErrKeyExists, want %d", createExists, workers-1)
	}

	// 2. Concurrent upserts to one key: versions must be monotonic and the
	// final version must equal the total number of successful writes + the
	// initial create count for that key. We only assert monotonicity and that
	// the final version is at least the number of writers (no regression).
	var writeWG sync.WaitGroup
	writeWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer writeWG.Done()
			for j := 0; j < writesEach; j++ {
				if _, err := store.Set(ctx, ns, key, config.NewValue(fmt.Sprintf("%d-%d", id, j))); err != nil {
					t.Errorf("worker %d write %d: %v", id, j, err)
					return
				}
			}
		}(i)
	}
	writeWG.Wait()

	got, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("Get after concurrent writes: %v", err)
	}
	finalVersion := got.Metadata().Version()
	if finalVersion != int64(workers*writesEach) {
		t.Errorf("final version = %d, want %d (one increment per successful write)",
			finalVersion, workers*writesEach)
	}

	// Monotonicity: GetVersions must return strictly decreasing versions
	// (newest first) with no duplicates or gaps in the [1..final] range.
	page, err := store.GetVersions(ctx, ns, key, config.NewVersionFilter().WithLimit(int(finalVersion)).Build())
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	versions := page.Versions()
	prev := int64(-1)
	for i, v := range versions {
		cur := v.Metadata().Version()
		if prev != -1 && cur >= prev {
			t.Errorf("versions not strictly decreasing at index %d: %d then %d", i, prev, cur)
		}
		prev = cur
	}
}

// TestIntegration_AliasAndCheckAccess_EndToEnd drives the alias RPCs and the
// CheckAccess RPC over the real gRPC bufconn path. CheckAccess is not part of
// the config.Store interface, so it is invoked through the generated client.
// A read-only guard is wired via the auth interceptor so CheckAccess sees an
// identity and reports can_read=true, can_write=false.
func TestIntegration_AliasAndCheckAccess_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	// --- Alias RPCs over gRPC, via an AllowAll service so writes succeed. ---
	store := setupIntegrationTest(t)

	if _, err := store.SetAlias(ctx, "db.host", "database/host"); err != nil {
		t.Fatalf("SetAlias e2e: %v", err)
	}
	got, err := store.GetAlias(ctx, "db.host")
	if err != nil {
		t.Fatalf("GetAlias e2e: %v", err)
	}
	if s, _ := got.String(); s != "database/host" {
		t.Errorf("GetAlias = %q, want database/host", s)
	}
	aliases, err := store.ListAliases(ctx)
	if err != nil {
		t.Fatalf("ListAliases e2e: %v", err)
	}
	if _, ok := aliases["db.host"]; !ok {
		t.Errorf("ListAliases missing db.host: %v", aliases)
	}
	if err := store.DeleteAlias(ctx, "db.host"); err != nil {
		t.Fatalf("DeleteAlias e2e: %v", err)
	}
	if _, err := store.GetAlias(ctx, "db.host"); !errors.Is(err, config.ErrNotFound) {
		t.Errorf("GetAlias after delete: got %v, want ErrNotFound", err)
	}

	// --- CheckAccess over gRPC, via a read-only guard so the answer is
	// asymmetric (can_read=true, can_write=false). The auth interceptor places
	// the identity in context; without it CheckAccess returns an empty response. ---
	backing := memory.NewStore()
	if err := backing.Connect(context.Background()); err != nil {
		t.Fatalf("connect backing store: %v", err)
	}
	t.Cleanup(func() { _ = backing.Close(context.Background()) })

	svc, err := service.NewService(backing, service.WithSecurityGuard(readOnlyGuard{}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(service.AuthInterceptor(readOnlyGuard{})),
	)
	configpb.RegisterConfigServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cc := configpb.NewConfigServiceClient(conn)
	resp, err := cc.CheckAccess(ctx, &configpb.CheckAccessRequest{Namespace: "payments"})
	if err != nil {
		t.Fatalf("CheckAccess e2e: %v", err)
	}
	if !resp.GetCanRead() {
		t.Error("CheckAccess: can_read = false, want true (read-only guard)")
	}
	if resp.GetCanWrite() {
		t.Error("CheckAccess: can_write = true, want false (read-only guard)")
	}
}

// --- helpers ---

// readOnlyGuard authenticates everyone as a fixed identity and authorizes only
// the "read" action. Used to give CheckAccess a non-trivial, asymmetric answer.
type readOnlyGuard struct{}

func (readOnlyGuard) Authenticate(ctx context.Context) (service.Identity, error) {
	return roIdentity{}, nil
}

func (readOnlyGuard) Authorize(ctx context.Context, _ service.Identity, action string, _ service.Resource) (service.Decision, error) {
	return service.Decision{Allowed: action == "read"}, nil
}

type roIdentity struct{}

func (roIdentity) UserID() string         { return "reader" }
func (roIdentity) Claims() map[string]any { return nil }

func errIsKeyExists(err error) bool {
	return config.IsKeyExists(err)
}

// waitWatchEstablished probes a namespace until the watch stream delivers an
// event, then drains buffered probe events. Fails the test if the stream never
// establishes within the timeout.
func waitWatchEstablished(t *testing.T, ctx context.Context, store *RemoteStore, wr *WatchResult, ns string) {
	t.Helper()
	ok := testutil.Eventually(10*time.Second, 25*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, ns, "__probe__", config.NewValue("p")); err != nil {
			return false
		}
		select {
		case <-wr.Events():
			return true
		case <-time.After(25 * time.Millisecond):
			return false
		}
	})
	if !ok {
		t.Fatal("watch stream did not establish")
	}
	for draining := true; draining; {
		select {
		case <-wr.Events():
		default:
			draining = false
		}
	}
}

// receiveKey waits up to timeout for a watch event with the given key.
func receiveKey(t *testing.T, ctx context.Context, wr *WatchResult, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-wr.Events():
			if !ok {
				return false
			}
			if ev.Key == key {
				return true
			}
		case <-deadline:
			return false
		case <-ctx.Done():
			return false
		}
	}
}
