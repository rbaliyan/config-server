package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
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

	svc := NewService(store, WithAuthorizer(AllowAll()))
	return svc, store
}

func TestService_Get(t *testing.T) {
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

func TestService_DenyAllAuthorizer(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
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

func TestService_CheckAccess(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupTestService(t)

	resp, err := svc.CheckAccess(ctx, &configpb.CheckAccessRequest{
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.CanRead {
		t.Error("expected CanRead to be true with AllowAll authorizer")
	}
	if !resp.CanWrite {
		t.Error("expected CanWrite to be true with AllowAll authorizer")
	}
}

func TestService_CheckAccess_DenyAll(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	svc := NewService(store) // DenyAll is default

	resp, err := svc.CheckAccess(ctx, &configpb.CheckAccessRequest{
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.CanRead {
		t.Error("expected CanRead to be false with DenyAll authorizer")
	}
	if resp.CanWrite {
		t.Error("expected CanWrite to be false with DenyAll authorizer")
	}
}

func TestService_Set_DefaultCodec(t *testing.T) {
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

func TestService_Set_WriteModes(t *testing.T) {
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
	t.Run("nil value", func(t *testing.T) {
		entry, err := valueToProto("ns", "key", nil)
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
		val := config.NewValue("hello", config.WithValueType(config.TypeString))
		entry, err := valueToProto("ns", "key", val)
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

func TestNewService_NilStorePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil store")
		}
	}()
	NewService(nil)
}

func TestNewService_WithOptions(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	auth := AllowAll()
	svc := NewService(store, WithAuthorizer(auth))
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

func TestToGRPCError_WrappedErrors(t *testing.T) {
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
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

	_, err := svc.Get(ctx, &configpb.GetRequest{
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
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

	_, err := svc.Set(ctx, &configpb.SetRequest{
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
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

	_, err := svc.Delete(ctx, &configpb.DeleteRequest{
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
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

	_, err := svc.List(ctx, &configpb.ListRequest{
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

func (m *mockWatchServer) SetHeader(metadata.MD) error  { return nil }
func (m *mockWatchServer) SendHeader(metadata.MD) error { return nil }
func (m *mockWatchServer) SetTrailer(metadata.MD)       {}
func (m *mockWatchServer) SendMsg(any) error            { return nil }
func (m *mockWatchServer) RecvMsg(any) error            { return nil }

func TestService_Watch_AllowAll(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

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

	time.Sleep(50 * time.Millisecond)

	if _, err := store.Set(ctx, "test", "key2", config.NewValue("value2")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	cancel()

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.responses) == 0 {
		t.Error("expected at least one watch response")
	}
}

func TestService_Watch_DenyAll(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store)

	stream := &mockWatchServer{ctx: ctx}

	err := svc.Watch(&configpb.WatchRequest{
		Namespaces: []string{"test"},
	}, stream)
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

func TestService_Watch_NoNamespaces(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{}, stream)
	}()

	time.Sleep(50 * time.Millisecond)

	if _, err := store.Set(ctx, "any-ns", "somekey", config.NewValue("val")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	cancel()

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.responses) == 0 {
		t.Error("expected at least one watch response for wildcard watch")
	}
}

func TestService_Watch_NoNamespaces_DenyAll(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store)

	stream := &mockWatchServer{ctx: ctx}

	err := svc.Watch(&configpb.WatchRequest{}, stream)
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

func TestService_Watch_StoreWatchError(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	store.Close(ctx) // Close the store so Watch returns an error

	svc := NewService(store, WithAuthorizer(AllowAll()))

	stream := &mockWatchServer{ctx: ctx}

	err := svc.Watch(&configpb.WatchRequest{
		Namespaces: []string{"test"},
	}, stream)
	if err == nil {
		t.Fatal("expected error from store.Watch on closed store")
	}
}

func TestService_Watch_ChannelCloses(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}

	svc := NewService(store, WithAuthorizer(AllowAll()))

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"test"},
		}, stream)
	}()

	time.Sleep(50 * time.Millisecond)

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
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

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

	time.Sleep(50 * time.Millisecond)

	if _, err := store.Set(ctx, "test", "key1", config.NewValue("val")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error from Send failure")
		}
		if err.Error() != sendErr.Error() {
			t.Errorf("expected send error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after send error")
	}
}

func TestService_Watch_DeleteEvent(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

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

	time.Sleep(50 * time.Millisecond)

	if err := store.Delete(ctx, "test", "del-key"); err != nil {
		t.Fatalf("failed to delete test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	cancel()

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

	foundDelete := false
	for _, resp := range stream.responses {
		if resp.Type == configpb.ChangeType_CHANGE_TYPE_DELETE {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Error("expected at least one DELETE change type in responses")
	}
}

func TestService_Watch_MultipleNamespaces(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc := NewService(store, WithAuthorizer(AllowAll()))

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := &mockWatchServer{ctx: watchCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Watch(&configpb.WatchRequest{
			Namespaces: []string{"ns1", "ns2"},
		}, stream)
	}()

	time.Sleep(50 * time.Millisecond)

	if _, err := store.Set(ctx, "ns1", "k1", config.NewValue("v1")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}
	if _, err := store.Set(ctx, "ns2", "k2", config.NewValue("v2")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	cancel()

	err := <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.responses) < 2 {
		t.Errorf("expected at least 2 responses for 2 namespaces, got %d", len(stream.responses))
	}
}

func TestService_Watch_PartialNamespaceDenied(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	auth := &namespaceAuthorizer{allowed: map[string]bool{"allowed": true}}
	svc := NewService(store, WithAuthorizer(auth))

	stream := &mockWatchServer{ctx: ctx}

	err := svc.Watch(&configpb.WatchRequest{
		Namespaces: []string{"allowed", "denied"},
	}, stream)
	if err == nil {
		t.Fatal("expected error when one namespace is denied")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got: %v", st.Code())
	}
}

type namespaceAuthorizer struct {
	allowed map[string]bool
}

func (a *namespaceAuthorizer) Authorize(_ context.Context, req AuthRequest) error {
	if req.Namespace == "" {
		return status.Errorf(codes.PermissionDenied, "wildcard not allowed")
	}
	if a.allowed[req.Namespace] {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "namespace %q not allowed", req.Namespace)
}
