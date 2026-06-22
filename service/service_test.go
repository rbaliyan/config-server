package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/internal/testutil"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, store
}

func TestService_Get(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Set up test data
	if _, err := store.Set(ctx, "test", "key1", config.NewValue("value1")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Set up test data
	if _, err := store.Set(ctx, "test", "to-delete", config.NewValue("value")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

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
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Set up test data
	if _, err := store.Set(ctx, "test", "app/name", config.NewValue("myapp")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}
	if _, err := store.Set(ctx, "test", "app/version", config.NewValue("1.0")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}
	if _, err := store.Set(ctx, "test", "db/host", config.NewValue("localhost")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

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

func TestAuthInterceptor_DenyAll(t *testing.T) {
	t.Parallel()
	guard := DenyAll()
	interceptor := AuthInterceptor(guard)

	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	}

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if err == nil {
		t.Fatal("expected unauthenticated error")
	}
	if called {
		t.Fatal("expected handler not to be called")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got: %v", st.Code())
	}
}

func TestService_CheckAccess(t *testing.T) {
	t.Parallel()
	svc, _ := setupTestService(t)

	// CheckAccess requires identity in context (normally placed by AuthInterceptor).
	ctx := ContextWithIdentity(context.Background(), anonymousIdentity{})

	resp, err := svc.CheckAccess(ctx, &configpb.CheckAccessRequest{
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.CanRead {
		t.Error("expected CanRead to be true with AllowAll guard")
	}
	if !resp.CanWrite {
		t.Error("expected CanWrite to be true with AllowAll guard")
	}
}

func TestService_CheckAccess_NoIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// Without identity in context, CheckAccess returns false for both.
	resp, err := svc.CheckAccess(ctx, &configpb.CheckAccessRequest{
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.CanRead {
		t.Error("expected CanRead to be false without identity in context")
	}
	if resp.CanWrite {
		t.Error("expected CanWrite to be false without identity in context")
	}
}

func TestService_Set_DefaultCodec(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// Set without specifying codec should default to json
	resp, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "nocodec",
		Value:     []byte(`"hello"`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Entry == nil {
		t.Fatal("expected entry")
	}
}

func TestService_Set_UnknownCodecPassThrough(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// Binary data that is not valid JSON — simulates client-side encrypted bytes.
	raw := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	resp, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "secret",
		Value:     raw,
		Codec:     "client:encrypted:json",
	})
	if err != nil {
		t.Fatalf("Set with unknown codec: %v", err)
	}
	if resp.Entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if resp.Entry.Codec != "client:encrypted:json" {
		t.Errorf("codec = %q, want %q", resp.Entry.Codec, "client:encrypted:json")
	}
	if string(resp.Entry.Value) != string(raw) {
		t.Errorf("value = %x, want %x", resp.Entry.Value, raw)
	}

	// Verify it can be retrieved.
	getResp, err := svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "secret",
	})
	if err != nil {
		t.Fatalf("Get after raw Set: %v", err)
	}
	if getResp.Entry.Codec != "client:encrypted:json" {
		t.Errorf("Get codec = %q, want %q", getResp.Entry.Codec, "client:encrypted:json")
	}
	if string(getResp.Entry.Value) != string(raw) {
		t.Errorf("Get value = %x, want %x", getResp.Entry.Value, raw)
	}
}

func TestService_Set_WriteModes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// Create mode
	_, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "wm-key",
		Value:     []byte(`"v1"`),
		Codec:     "json",
		WriteMode: configpb.WriteMode_WRITE_MODE_CREATE,
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Create again should fail (already exists)
	_, err = svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "wm-key",
		Value:     []byte(`"v2"`),
		Codec:     "json",
		WriteMode: configpb.WriteMode_WRITE_MODE_CREATE,
	})
	if err == nil {
		t.Fatal("expected error for duplicate create")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Errorf("expected AlreadyExists, got: %v", st.Code())
	}

	// Update mode
	_, err = svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "wm-key",
		Value:     []byte(`"v2"`),
		Codec:     "json",
		WriteMode: configpb.WriteMode_WRITE_MODE_UPDATE,
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Update nonexistent should fail
	_, err = svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "nonexistent",
		Value:     []byte(`"v"`),
		Codec:     "json",
		WriteMode: configpb.WriteMode_WRITE_MODE_UPDATE,
	})
	if err == nil {
		t.Fatal("expected error for update of nonexistent key")
	}
}

func TestService_List_Pagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	for i := 0; i < 5; i++ {
		if _, err := store.Set(ctx, "test", "key"+string(rune('A'+i)), config.NewValue(i)); err != nil {
			t.Fatalf("failed to set test data: %v", err)
		}
	}

	// List with limit
	resp, err := svc.List(ctx, &configpb.ListRequest{
		Namespace: "test",
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(resp.Entries))
	}
	if resp.NextCursor == "" {
		t.Error("expected non-empty next cursor")
	}

	// Next page
	resp2, err := svc.List(ctx, &configpb.ListRequest{
		Namespace: "test",
		Limit:     2,
		Cursor:    resp.NextCursor,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp2.Entries) != 2 {
		t.Errorf("expected 2 entries on page 2, got %d", len(resp2.Entries))
	}
}

func TestToGRPCError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"nil", nil, codes.OK},
		{"not found sentinel", config.ErrNotFound, codes.NotFound},
		{"key exists sentinel", config.ErrKeyExists, codes.AlreadyExists},
		{"invalid key sentinel", config.ErrInvalidKey, codes.InvalidArgument},
		{"invalid namespace", config.ErrInvalidNamespace, codes.InvalidArgument},
		{"invalid value", config.ErrInvalidValue, codes.InvalidArgument},
		{"type mismatch", config.ErrTypeMismatch, codes.InvalidArgument},
		{"read only", config.ErrReadOnly, codes.FailedPrecondition},
		{"not connected", config.ErrStoreNotConnected, codes.Unavailable},
		{"store closed", config.ErrStoreClosed, codes.Unavailable},
		{"watch not supported", config.ErrWatchNotSupported, codes.Unimplemented},
		{"codec not found", config.ErrCodecNotFound, codes.InvalidArgument},
		{"key not found error", &config.KeyNotFoundError{Key: "k", Namespace: "ns"}, codes.NotFound},
		{"key exists error", &config.KeyExistsError{Key: "k", Namespace: "ns"}, codes.AlreadyExists},
		{"store error", &config.StoreError{Op: "get", Backend: "test", Key: "k", Err: errors.New("fail")}, codes.Internal},
		{"unknown", errors.New("unknown"), codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := toGRPCError(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Errorf("toGRPCError(nil) = %v, want nil", got)
				}
				return
			}
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("expected gRPC status error, got: %v", got)
			}
			if st.Code() != tt.code {
				t.Errorf("toGRPCError(%v) code = %v, want %v", tt.err, st.Code(), tt.code)
			}
		})
	}
}

func TestValueToProto(t *testing.T) {
	t.Parallel()
	t.Run("nil value", func(t *testing.T) {
		t.Parallel()
		entry, err := valueToProto(context.Background(), "ns", "key", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entry.Namespace != "ns" || entry.Key != "key" {
			t.Errorf("expected ns/key, got %s/%s", entry.Namespace, entry.Key)
		}
		if len(entry.Value) != 0 {
			t.Error("expected empty value for nil")
		}
	})

	t.Run("with value", func(t *testing.T) {
		t.Parallel()
		val := config.NewValue("hello", config.WithValueType(config.TypeString))
		entry, err := valueToProto(context.Background(), "ns", "key", val)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entry.Namespace != "ns" || entry.Key != "key" {
			t.Errorf("expected ns/key, got %s/%s", entry.Namespace, entry.Key)
		}
		if len(entry.Value) == 0 {
			t.Error("expected non-empty value")
		}
		if entry.Codec != "json" {
			t.Errorf("expected json codec, got %s", entry.Codec)
		}
	})
}

func TestRecoveryInterceptor(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	interceptor := RecoveryInterceptor(logger)

	// Handler that panics
	handler := func(ctx context.Context, req any) (any, error) {
		panic("test panic")
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if resp != nil {
		t.Errorf("expected nil response from panicking handler, got %v", resp)
	}
	if err == nil {
		t.Fatal("expected error from panicking handler")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal, got: %v", st.Code())
	}
}

func TestService_Get_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// Empty namespace
	_, err := svc.Get(ctx, &configpb.GetRequest{Key: "key"})
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}

	// Empty key
	_, err = svc.Get(ctx, &configpb.GetRequest{Namespace: "ns"})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestService_Set_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.Set(ctx, &configpb.SetRequest{Key: "key", Value: []byte(`"v"`), Codec: "json"})
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestService_Delete_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.Delete(ctx, &configpb.DeleteRequest{Namespace: "ns"})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestService_List_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.List(ctx, &configpb.ListRequest{})
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestStreamRecoveryInterceptor(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	interceptor := StreamRecoveryInterceptor(logger)

	handler := func(srv any, stream grpc.ServerStream) error {
		panic("stream panic")
	}

	err := interceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/test"}, handler)
	if err == nil {
		t.Fatal("expected error from panicking stream handler")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal, got: %v", st.Code())
	}
}

func TestStreamLoggingInterceptor(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	interceptor := StreamLoggingInterceptor(logger)

	// Successful handler
	handler := func(srv any, stream grpc.ServerStream) error {
		return nil
	}
	err := interceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/test"}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Failing handler
	failHandler := func(srv any, stream grpc.ServerStream) error {
		return errors.New("stream fail")
	}
	err = interceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/test"}, failHandler)
	if err == nil {
		t.Fatal("expected error from failing stream handler")
	}
}

func TestNewService_NilStoreReturnsError(t *testing.T) {
	t.Parallel()
	svc, err := NewService(nil)
	if err == nil {
		t.Fatal("expected error for nil store, got nil")
	}
	if svc != nil {
		t.Fatal("expected nil service when store is nil")
	}
}

func TestNewService_WithOptions(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	guard := AllowAll()
	svc, err := NewService(store, WithSecurityGuard(guard))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

func TestToGRPCError_WrappedErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"wrapped not found", errors.New("wrapped: " + config.ErrNotFound.Error()), codes.Internal},
		{"key not found wrapped", &config.KeyNotFoundError{Key: "k", Namespace: "ns"}, codes.NotFound},
		{"key exists wrapped", &config.KeyExistsError{Key: "k", Namespace: "ns"}, codes.AlreadyExists},
		{"type mismatch", &config.TypeMismatchError{Key: "k", Expected: config.TypeInt, Actual: config.TypeString}, codes.InvalidArgument},
		{"invalid key", &config.InvalidKeyError{Key: "k/../x", Reason: "path traversal"}, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := toGRPCError(tt.err)
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("expected gRPC status error, got: %v", got)
			}
			if st.Code() != tt.code {
				t.Errorf("code = %v, want %v", st.Code(), tt.code)
			}
		})
	}
}

func TestService_Get_ClosedStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.Get(ctx, &configpb.GetRequest{
		Namespace: "test",
		Key:       "key",
	})
	if err == nil {
		t.Fatal("expected error for closed store")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got: %v", st.Code())
	}
}

func TestService_Set_ClosedStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.Set(ctx, &configpb.SetRequest{
		Namespace: "test",
		Key:       "key",
		Value:     []byte(`"v"`),
		Codec:     "json",
	})
	if err == nil {
		t.Fatal("expected error for closed store")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got: %v", st.Code())
	}
}

func TestService_Delete_ClosedStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.Delete(ctx, &configpb.DeleteRequest{
		Namespace: "test",
		Key:       "key",
	})
	if err == nil {
		t.Fatal("expected error for closed store")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got: %v", st.Code())
	}
}

func TestService_List_ClosedStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.List(ctx, &configpb.ListRequest{
		Namespace: "test",
	})
	if err == nil {
		t.Fatal("expected error for closed store")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got: %v", st.Code())
	}
}

func TestService_Delete_NotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.Delete(ctx, &configpb.DeleteRequest{
		Namespace: "test",
		Key:       "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for deleting nonexistent key")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", st.Code())
	}
}

func TestService_List_Empty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	resp, err := svc.List(ctx, &configpb.ListRequest{
		Namespace: "empty-ns",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(resp.Entries))
	}
}

func TestLoggingInterceptor(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	interceptor := LoggingInterceptor(logger)

	// Successful handler
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}
	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}

	// Failing handler
	failHandler := func(ctx context.Context, req any) (any, error) {
		return nil, errors.New("fail")
	}
	_, err = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, failHandler)
	if err == nil {
		t.Fatal("expected error from failing handler")
	}
}

// mockWatchServer implements configpb.ConfigService_WatchServer for testing.
type mockWatchServer struct {
	grpc.ServerStream
	ctx       context.Context
	mu        sync.Mutex
	responses []*configpb.WatchResponse
	sendErr   error
}

func (m *mockWatchServer) Context() context.Context {
	return m.ctx
}

func (m *mockWatchServer) Send(resp *configpb.WatchResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return m.sendErr
	}
	m.responses = append(m.responses, resp)
	return nil
}

// responseCount returns the number of responses received so far, safely under
// the mutex. It lets tests poll for event propagation instead of sleeping.
func (m *mockWatchServer) responseCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.responses)
}

func (m *mockWatchServer) SetHeader(metadata.MD) error  { return nil }
func (m *mockWatchServer) SendHeader(metadata.MD) error { return nil }
func (m *mockWatchServer) SetTrailer(metadata.MD)       {}
func (m *mockWatchServer) SendMsg(any) error            { return nil }
func (m *mockWatchServer) RecvMsg(any) error            { return nil }

func TestService_Watch_AllowAll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if _, err := store.Set(ctx, "test", "key1", config.NewValue("value1")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"test"},
		}, stream)
	}()

	// The watch subscription registers asynchronously inside the goroutine, and
	// memory-store events are transient (no replay), so a single write can race
	// the subscription. Re-write until the stream observes at least one event.
	testutil.WaitFor(t, 2*time.Second, 5*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, "test", "key2", config.NewValue("value2")); err != nil {
			t.Errorf("failed to set test data: %v", err)
			return true
		}
		return stream.responseCount() > 0
	}, "watch stream did not receive a Set event")

	cancel()

	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.responses) == 0 {
		t.Error("expected at least one watch response")
	}
}

func TestStreamAuthInterceptor_DenyAll(t *testing.T) {
	t.Parallel()
	guard := DenyAll()
	interceptor := StreamAuthInterceptor(guard)

	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	}

	err := interceptor(nil, &mockRateLimitStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test"}, handler)
	if err == nil {
		t.Fatal("expected unauthenticated error")
	}
	if called {
		t.Fatal("expected handler not to be called")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got: %v", st.Code())
	}
}

func TestService_Watch_NoNamespaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{}, stream)
	}()

	// Re-write until the wildcard watch observes the event (subscription
	// registration is async and events are transient).
	testutil.WaitFor(t, 2*time.Second, 5*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, "any-ns", "somekey", config.NewValue("val")); err != nil {
			t.Errorf("failed to set test data: %v", err)
			return true
		}
		return stream.responseCount() > 0
	}, "wildcard watch stream did not receive a Set event")

	cancel()

	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.responses) == 0 {
		t.Error("expected at least one watch response for wildcard watch")
	}
}

func TestAuthInterceptor_AllowAll(t *testing.T) {
	t.Parallel()
	guard := AllowAll()
	interceptor := AuthInterceptor(guard)

	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		// Verify identity was placed in context
		id, ok := IdentityFromContext(ctx)
		if !ok {
			t.Error("expected identity in context")
		}
		if id.UserID() != "anonymous" {
			t.Errorf("expected anonymous user, got %q", id.UserID())
		}
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected handler to be called")
	}
	if resp != "ok" {
		t.Errorf("unexpected response: %v", resp)
	}
}

func TestStreamAuthInterceptor_AllowAll(t *testing.T) {
	t.Parallel()
	guard := AllowAll()
	interceptor := StreamAuthInterceptor(guard)

	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		// Verify identity was placed in context
		id, ok := IdentityFromContext(stream.Context())
		if !ok {
			t.Error("expected identity in context")
		}
		if id.UserID() != "anonymous" {
			t.Errorf("expected anonymous user, got %q", id.UserID())
		}
		return nil
	}

	err := interceptor(nil, &mockRateLimitStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/test"}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected handler to be called")
	}
}

func TestService_Watch_StoreWatchError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx) // Close the store so Watch returns an error

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	stream := &mockWatchServer{ctx: ctx}

	err = svc.Watch(&configpb.WatchRequest{
		Namespaces: []string{"test"},
	}, stream)
	if err == nil {
		t.Fatal("expected error from store.Watch on closed store")
	}
}

func TestService_Watch_ChannelCloses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"test"},
		}, stream)
	}()

	// Wait until the watch subscription is established (a write propagates to
	// the stream) before closing, so we exercise the close-while-watching path
	// rather than a race where Close lands before store.Watch registers.
	testutil.WaitFor(t, 2*time.Second, 5*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, "test", "ready-probe", config.NewValue("probe")); err != nil {
			t.Errorf("failed to set probe value: %v", err)
			return true
		}
		return stream.responseCount() > 0
	}, "watch subscription was never established")

	store.Close(ctx)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Watch returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after store close")
	}
}

func TestService_Watch_SendError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendErr := errors.New("send failed")
	stream := &mockWatchServer{ctx: watchCtx, sendErr: sendErr}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"test"},
		}, stream)
	}()

	// Keep writing until Watch returns (the first event delivered to the failing
	// stream produces sendErr). A background writer avoids racing the async
	// subscription registration; it stops once Watch has returned.
	writeStop := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-writeStop:
				return
			case <-ticker.C:
				_, _ = store.Set(ctx, "test", "key1", config.NewValue("val"))
			}
		}
	}()

	select {
	case err := <-errCh:
		close(writeStop)
		<-writeDone
		if err == nil {
			t.Fatal("expected error from Send failure")
		}
		if err.Error() != sendErr.Error() {
			t.Errorf("expected send error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(writeStop)
		<-writeDone
		t.Fatal("Watch did not return after send error")
	}
}

func TestService_Watch_DeleteEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if _, err := store.Set(ctx, "test", "del-key", config.NewValue("to-delete")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"test"},
		}, stream)
	}()

	// Delete is one-shot (cannot be replayed), so first confirm the subscription
	// is established via a Set probe before deleting, then poll for the DELETE
	// event to arrive on the stream.
	testutil.WaitFor(t, 2*time.Second, 5*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, "test", "ready-probe", config.NewValue("probe")); err != nil {
			t.Errorf("failed to set probe value: %v", err)
			return true
		}
		return stream.responseCount() > 0
	}, "watch subscription was never established")

	if err := store.Delete(ctx, "test", "del-key"); err != nil {
		t.Fatalf("failed to delete test data: %v", err)
	}

	hasDelete := func() bool {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		for _, resp := range stream.responses {
			if resp.Type == configpb.ChangeType_CHANGE_TYPE_DELETE {
				return true
			}
		}
		return false
	}
	testutil.WaitFor(t, 2*time.Second, 5*time.Millisecond, hasDelete,
		"expected at least one DELETE change type in responses")

	cancel()

	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	if !hasDelete() {
		t.Error("expected at least one DELETE change type in responses")
	}
}

func TestService_Watch_MultipleNamespaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"ns1", "ns2"},
		}, stream)
	}()

	// Re-write both namespaces until the stream has observed at least one event
	// from each (>= 2 responses), tolerating async subscription registration and
	// transient (non-replayed) events.
	testutil.WaitFor(t, 2*time.Second, 5*time.Millisecond, func() bool {
		if _, err := store.Set(ctx, "ns1", "k1", config.NewValue("v1")); err != nil {
			t.Errorf("failed to set test data: %v", err)
			return true
		}
		if _, err := store.Set(ctx, "ns2", "k2", config.NewValue("v2")); err != nil {
			t.Errorf("failed to set test data: %v", err)
			return true
		}
		return stream.responseCount() >= 2
	}, "watch stream did not receive events from both namespaces")

	cancel()

	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.responses) < 2 {
		t.Errorf("expected at least 2 responses for 2 namespaces, got %d", len(stream.responses))
	}
}

// TestServiceDenyingGuard verifies that a service configured with a guard
// that denies authorization returns PermissionDenied from RPC methods.
// Auth interceptor authenticates (denyingGuard.Authenticate succeeds);
// inline authorize in the method calls guard.Authorize which denies.
func TestServiceDenyingGuard(t *testing.T) {
	t.Parallel()
	store := memory.NewStore()
	if err := store.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())

	svc, err := NewService(store, WithSecurityGuard(&denyingGuard{}))
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.Get(context.Background(), &configpb.GetRequest{
		Namespace: "ns", Key: "key",
	})
	if err == nil {
		t.Fatal("expected permission denied error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got: %v", st.Code())
	}
}

func TestIdentityFromContext(t *testing.T) {
	t.Parallel()
	// No identity in context
	_, ok := IdentityFromContext(context.Background())
	if ok {
		t.Error("expected no identity in empty context")
	}

	// With identity
	ctx := ContextWithIdentity(context.Background(), anonymousIdentity{})
	id, ok := IdentityFromContext(ctx)
	if !ok {
		t.Fatal("expected identity in context")
	}
	if id.UserID() != "anonymous" {
		t.Errorf("expected anonymous, got %q", id.UserID())
	}
}

func TestService_GetVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Create 3 versions
	store.Set(ctx, "test", "key1", config.NewValue("v1"))
	store.Set(ctx, "test", "key1", config.NewValue("v2"))
	store.Set(ctx, "test", "key1", config.NewValue("v3"))

	// List all versions
	resp, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "test",
		Key:       "key1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(resp.Entries))
	}

	// Should be newest first
	if resp.Entries[0].Version != 3 {
		t.Errorf("first entry version = %d, want 3", resp.Entries[0].Version)
	}
	if resp.Entries[2].Version != 1 {
		t.Errorf("last entry version = %d, want 1", resp.Entries[2].Version)
	}
}

func TestService_GetVersions_SpecificVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	store.Set(ctx, "test", "key1", config.NewValue("v1"))
	store.Set(ctx, "test", "key1", config.NewValue("v2"))

	resp, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "test",
		Key:       "key1",
		Version:   1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(resp.Entries))
	}
	if resp.Entries[0].Version != 1 {
		t.Errorf("version = %d, want 1", resp.Entries[0].Version)
	}
}

func TestService_GetVersions_NotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "test",
		Key:       "nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", st.Code())
	}
}

func TestService_GetVersions_VersionNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	store.Set(ctx, "test", "key1", config.NewValue("v1"))

	_, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "test",
		Key:       "key1",
		Version:   99,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent version")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", st.Code())
	}
}

func TestService_GetVersions_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// Empty namespace
	_, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{Key: "key"})
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}

	// Empty key
	_, err = svc.GetVersions(ctx, &configpb.GetVersionsRequest{Namespace: "ns"})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestService_GetVersions_Pagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	// Create 5 versions
	for i := 1; i <= 5; i++ {
		store.Set(ctx, "test", "key1", config.NewValue(i))
	}

	// Page 1
	resp1, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "test",
		Key:       "key1",
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("page 1 error: %v", err)
	}
	if len(resp1.Entries) != 2 {
		t.Fatalf("page 1: expected 2 entries, got %d", len(resp1.Entries))
	}
	if resp1.NextCursor == "" {
		t.Fatal("expected non-empty cursor after page 1")
	}

	// Page 2
	resp2, err := svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "test",
		Key:       "key1",
		Limit:     2,
		Cursor:    resp1.NextCursor,
	})
	if err != nil {
		t.Fatalf("page 2 error: %v", err)
	}
	if len(resp2.Entries) != 2 {
		t.Fatalf("page 2: expected 2 entries, got %d", len(resp2.Entries))
	}
}

func TestService_GetVersions_NotSupported(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use a store that does NOT implement VersionedStore.
	store := &unversionedStore{}
	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.GetVersions(ctx, &configpb.GetVersionsRequest{
		Namespace: "ns",
		Key:       "key",
	})
	if err == nil {
		t.Fatal("expected error for unversioned store")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented {
		t.Errorf("expected Unimplemented, got: %v", st.Code())
	}
}

func TestAuthInterceptor_DenyAll_ErrorCode(t *testing.T) {
	t.Parallel()
	guard := DenyAll()
	interceptor := AuthInterceptor(guard)

	handler := func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	}

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/config.v1.ConfigService/GetVersions"}, handler)
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got: %v", st.Code())
	}
}

func TestService_Snapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	store.Set(ctx, "test", "key1", config.NewValue("v1"))
	store.Set(ctx, "test", "key2", config.NewValue("v2"))
	store.Set(ctx, "test", "key3", config.NewValue("v3"))

	resp, err := svc.Snapshot(ctx, &configpb.SnapshotRequest{Namespace: "test"})
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if len(resp.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(resp.Entries))
	}
	if resp.Etag == "" {
		t.Fatal("expected non-empty ETag")
	}
	if resp.NotModified {
		t.Error("expected NotModified=false on first call")
	}
}

func TestService_Snapshot_ETag(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, store := setupTestService(t)

	store.Set(ctx, "test", "key1", config.NewValue("v1"))

	// Get initial ETag
	resp1, err := svc.Snapshot(ctx, &configpb.SnapshotRequest{Namespace: "test"})
	if err != nil {
		t.Fatalf("Snapshot 1 failed: %v", err)
	}

	// Same ETag should return not_modified
	resp2, err := svc.Snapshot(ctx, &configpb.SnapshotRequest{
		Namespace:   "test",
		IfNoneMatch: resp1.Etag,
	})
	if err != nil {
		t.Fatalf("Snapshot 2 failed: %v", err)
	}
	if !resp2.NotModified {
		t.Error("expected NotModified=true when ETag matches")
	}
	if len(resp2.Entries) != 0 {
		t.Errorf("expected 0 entries when not modified, got %d", len(resp2.Entries))
	}

	// Modify data, ETag should change
	store.Set(ctx, "test", "key1", config.NewValue("v2"))

	resp3, err := svc.Snapshot(ctx, &configpb.SnapshotRequest{
		Namespace:   "test",
		IfNoneMatch: resp1.Etag,
	})
	if err != nil {
		t.Fatalf("Snapshot 3 failed: %v", err)
	}
	if resp3.NotModified {
		t.Error("expected NotModified=false after data change")
	}
	if resp3.Etag == resp1.Etag {
		t.Error("expected different ETag after data change")
	}
}

func TestService_Snapshot_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	_, err := svc.Snapshot(ctx, &configpb.SnapshotRequest{})
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestService_Snapshot_Empty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupTestService(t)

	resp, err := svc.Snapshot(ctx, &configpb.SnapshotRequest{Namespace: "empty-ns"})
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(resp.Entries))
	}
	if resp.Etag == "" {
		t.Fatal("expected non-empty ETag even for empty namespace")
	}
}

// unversionedStore is a minimal config.Store that does NOT implement
// config.VersionedStore, used to test the Unimplemented error path.
type unversionedStore struct{}

func (s *unversionedStore) Connect(context.Context) error { return nil }
func (s *unversionedStore) Close(context.Context) error   { return nil }
func (s *unversionedStore) Get(context.Context, string, string) (config.Value, error) {
	return nil, nil
}
func (s *unversionedStore) Set(context.Context, string, string, config.Value) (config.Value, error) {
	return nil, nil
}
func (s *unversionedStore) Delete(context.Context, string, string) error { return nil }
func (s *unversionedStore) Find(context.Context, string, config.Filter) (config.Page, error) {
	return nil, nil
}
func (s *unversionedStore) Watch(context.Context, config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, nil
}

// denyingGuard authenticates successfully but denies all actions.
type denyingGuard struct{}

func (denyingGuard) Authenticate(context.Context) (Identity, error) {
	return anonymousIdentity{}, nil
}

func (denyingGuard) Authorize(context.Context, Identity, string, Resource) (Decision, error) {
	return Decision{Allowed: false, Reason: "denied by test guard"}, nil
}
