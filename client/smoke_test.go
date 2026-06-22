package client

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// TestSmoke_ClientCRUD is the fast labeled smoke check of the RemoteStore happy
// path over a real gRPC bufconn server: Set -> Get -> Delete -> Get(ErrNotFound).
// It reuses setupIntegrationTest (bufconn + memory store + AllowAll guard).
func TestSmoke_ClientCRUD(t *testing.T) {
	t.Parallel()

	store := setupIntegrationTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	t.Cleanup(cancel)

	const (
		ns  = "smoke-ns"
		key = "k"
	)

	if _, err := store.Set(ctx, ns, key, config.NewValue("smoke")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s, _ := got.String(); s != "smoke" {
		t.Errorf("Get = %q, want %q", s, "smoke")
	}

	if err := store.Delete(ctx, ns, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := store.Get(ctx, ns, key); !errors.Is(err, config.ErrNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
	}
}

// TestSmoke_DenyAllGuardBlocks verifies the secure production default: a Service
// built without an explicit guard uses DenyAll, so a Get over the wire that
// carries no credentials is rejected before touching the store. Existing e2e
// tests all use AllowAll, so this smokes the default-deny posture specifically.
//
// Authorization is enforced inside each RPC method (s.authorize), which calls
// guard.Authenticate first. With no identity in the request context, DenyAll's
// Authenticate returns codes.Unauthenticated (no credentials presented) — not
// PermissionDenied, which would require an authenticated-but-rejected identity.
func TestSmoke_DenyAllGuardBlocks(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	t.Cleanup(cancel)

	memStore := memory.NewStore()
	if err := memStore.Connect(ctx); err != nil {
		t.Fatalf("memStore.Connect: %v", err)
	}
	t.Cleanup(func() { _ = memStore.Close(context.Background()) })

	// No WithSecurityGuard => the default DenyAll guard is used.
	svc, err := service.NewService(memStore)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	configpb.RegisterConfigServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cli := configpb.NewConfigServiceClient(conn)
	_, err = cli.Get(ctx, &configpb.GetRequest{Namespace: "smoke-ns", Key: "k"})
	if got := status.Code(err); got != codes.Unauthenticated && got != codes.PermissionDenied {
		t.Errorf("Get with DenyAll guard: code = %v, want Unauthenticated or PermissionDenied (err: %v)", got, err)
	}
}

// TestSmoke_SnapshotETag smokes the conditional-snapshot path: an initial
// Snapshot yields an ETag; a follow-up Snapshot with WithIfNoneMatch(etag)
// must report NotModified with no entries.
func TestSmoke_SnapshotETag(t *testing.T) {
	t.Parallel()

	store := setupIntegrationTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	t.Cleanup(cancel)

	const ns = "snap-ns"
	if _, err := store.Set(ctx, ns, "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	first, err := store.Snapshot(ctx, ns)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if first.ETag == "" {
		t.Fatal("first Snapshot returned empty ETag")
	}
	if len(first.Entries) == 0 {
		t.Fatal("first Snapshot returned no entries")
	}

	second, err := store.Snapshot(ctx, ns, WithIfNoneMatch(first.ETag))
	if err != nil {
		t.Fatalf("Snapshot(IfNoneMatch): %v", err)
	}
	if !second.NotModified {
		t.Errorf("conditional Snapshot: NotModified = false, want true")
	}
	if len(second.Entries) != 0 {
		t.Errorf("conditional Snapshot: %d entries, want 0", len(second.Entries))
	}
}
