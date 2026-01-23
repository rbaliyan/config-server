package service

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupTestService(t *testing.T) (*Service, config.Store) {
	t.Helper()

	store := memory.NewStore()
	ctx := context.Background()

	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}

	t.Cleanup(func() {
		store.Close(ctx)
	})

	svc := NewService(store, WithAuthorizer(AllowAll()))
	return svc, store
}

func TestService_Get(t *testing.T) {
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Set up test data
	store.Set(ctx, "test", "key1", config.NewValue("value1"))

	// Test successful get
	resp, err := svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "key1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Entry == nil {
		t.Fatal("expected entry, got nil")
	}

	if resp.Entry.Namespace != "test" {
		t.Errorf("namespace = %q, want %q", resp.Entry.Namespace, "test")
	}

	if resp.Entry.Key != "key1" {
		t.Errorf("key = %q, want %q", resp.Entry.Key, "key1")
	}
}

func TestService_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}

	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", st.Code())
	}
}

func TestService_Set(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupTestService(t)

	resp, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "newkey",
		Value:     []byte(`"newvalue"`),
		Codec:     "json",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Entry == nil {
		t.Fatal("expected entry, got nil")
	}

	if resp.Entry.Key != "newkey" {
		t.Errorf("key = %q, want %q", resp.Entry.Key, "newkey")
	}

	// Verify it was stored
	getResp, err := svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "newkey",
	})
	if err != nil {
		t.Fatalf("failed to get stored value: %v", err)
	}

	if getResp.Entry == nil {
		t.Fatal("expected entry after set")
	}
}

func TestService_Delete(t *testing.T) {
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Set up test data
	store.Set(ctx, "test", "to-delete", config.NewValue("value"))

	// Delete it
	_, err := svc.Delete(ctx, &configpb.DeleteRequest{
		Namespace: "test",
		Key:       "to-delete",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify it's gone
	_, err = svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "to-delete",
	})
	if err == nil {
		t.Fatal("expected error after delete")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", st.Code())
	}
}

func TestService_List(t *testing.T) {
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Set up test data
	store.Set(ctx, "test", "app/name", config.NewValue("myapp"))
	store.Set(ctx, "test", "app/version", config.NewValue("1.0"))
	store.Set(ctx, "test", "db/host", config.NewValue("localhost"))

	// List with prefix
	resp, err := svc.List(ctx, &configpb.ListRequest{
		Namespace: "test",
		Prefix:    "app/",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Entries) != 2 {
		t.Errorf("expected 2 entries with app/ prefix, got %d", len(resp.Entries))
	}
}

func TestService_DenyAllAuthorizer(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Service with DenyAll (default)
	svc := NewService(store)

	_, err := svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "key",
	})
	if err == nil {
		t.Fatal("expected permission denied")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}

	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got: %v", st.Code())
	}
}
