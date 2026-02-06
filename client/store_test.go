package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRemoteStore_NotConnected(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	ctx := context.Background()

	// All operations should fail when not connected
	t.Run("Get", func(t *testing.T) {
		_, err := store.Get(ctx, "ns", "key")
		if !errors.Is(err, config.ErrStoreNotConnected) {
			t.Errorf("Get() error = %v, want ErrStoreNotConnected", err)
		}
	})

	t.Run("Set", func(t *testing.T) {
		_, err := store.Set(ctx, "ns", "key", config.NewValue("test"))
		if !errors.Is(err, config.ErrStoreNotConnected) {
			t.Errorf("Set() error = %v, want ErrStoreNotConnected", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Delete(ctx, "ns", "key")
		if !errors.Is(err, config.ErrStoreNotConnected) {
			t.Errorf("Delete() error = %v, want ErrStoreNotConnected", err)
		}
	})

	t.Run("Find", func(t *testing.T) {
		_, err := store.Find(ctx, "ns", config.NewFilter().Build())
		if !errors.Is(err, config.ErrStoreNotConnected) {
			t.Errorf("Find() error = %v, want ErrStoreNotConnected", err)
		}
	})

	t.Run("Watch", func(t *testing.T) {
		_, err := store.Watch(ctx, config.WatchFilter{})
		if !errors.Is(err, config.ErrStoreNotConnected) {
			t.Errorf("Watch() error = %v, want ErrStoreNotConnected", err)
		}
	})
}

func TestRemoteStore_CloseWithoutConnect(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	// Close without Connect should not error
	if err := store.Close(context.Background()); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestRemoteStore_CloseTwice(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	// Close twice should be safe (idempotent)
	store.Close(context.Background())
	if err := store.Close(context.Background()); err != nil {
		t.Errorf("Close() second call error = %v, want nil", err)
	}
}

func TestRemoteStore_OperationsAfterClose(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	store.Close(context.Background())

	ctx := context.Background()

	// Operations should fail with ErrStoreClosed
	_, err = store.Get(ctx, "ns", "key")
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Get() after close error = %v, want ErrStoreClosed", err)
	}
}

func TestRemoteStore_ConcurrentAccess(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 100)

	// Concurrent operations should not panic
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Get(ctx, "ns", "key")
			if err != nil && !errors.Is(err, config.ErrStoreNotConnected) {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRemoteStore_ConcurrentConnectClose(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent Connect and Close should not panic or race
	for i := 0; i < 5; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			store.Connect(ctx) // Ignore error - no server
		}()
		go func() {
			defer wg.Done()
			store.Close(ctx)
		}()
	}

	wg.Wait()
}

func TestRemoteStore_InterfaceCompliance(t *testing.T) {
	var store config.Store = &RemoteStore{}
	if store == nil {
		t.Error("RemoteStore should implement config.Store")
	}
}

func TestRemoteStore_State(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	// Initial state should be disconnected
	if store.State() != ConnStateDisconnected {
		t.Errorf("Initial state = %v, want Disconnected", store.State())
	}

	// After close, state should be closed
	store.Close(context.Background())
	if store.State() != ConnStateClosed {
		t.Errorf("After close state = %v, want Closed", store.State())
	}
}

func TestRemoteStore_StateCallback(t *testing.T) {
	states := make([]ConnState, 0)
	var mu sync.Mutex

	store, err := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithStateCallback(func(state ConnState) {
			mu.Lock()
			states = append(states, state)
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	store.Close(context.Background())

	mu.Lock()
	defer mu.Unlock()

	if len(states) == 0 {
		t.Error("State callback should have been called")
	}

	// Last state should be closed
	if states[len(states)-1] != ConnStateClosed {
		t.Errorf("Last state = %v, want Closed", states[len(states)-1])
	}
}

func TestRemoteStore_Ready(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	// Not ready when not connected
	if store.Ready() {
		t.Error("Ready() should be false when not connected")
	}
}

func TestWatchResult(t *testing.T) {
	ch := make(chan config.ChangeEvent)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	close(doneCh)

	result := &WatchResult{
		Events: ch,
		Err: func() error {
			<-doneCh
			select {
			case err := <-errCh:
				return err
			default:
				return nil
			}
		},
		Stop: func() {},
	}

	if result.Events == nil {
		t.Error("Events channel should not be nil")
	}

	if result.Err == nil {
		t.Error("Err function should not be nil")
	}

	if result.Stop == nil {
		t.Error("Stop function should not be nil")
	}

	// Test normal closure (no error)
	if err := result.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
}

func TestWatchResult_WithError(t *testing.T) {
	ch := make(chan config.ChangeEvent)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	testErr := errors.New("test error")

	errCh <- testErr
	close(doneCh)

	result := &WatchResult{
		Events: ch,
		Err: func() error {
			<-doneCh
			select {
			case err := <-errCh:
				return err
			default:
				return nil
			}
		},
		Stop: func() {},
	}

	if err := result.Err(); err != testErr {
		t.Errorf("Err() = %v, want %v", err, testErr)
	}
}

func TestConnState_String(t *testing.T) {
	tests := []struct {
		state ConnState
		want  string
	}{
		{ConnStateDisconnected, "disconnected"},
		{ConnStateConnecting, "connecting"},
		{ConnStateConnected, "connected"},
		{ConnStateClosed, "closed"},
		{ConnState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("ConnState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestOptions(t *testing.T) {
	t.Run("WithRetry", func(t *testing.T) {
		store, _ := NewRemoteStore("localhost:9999",
			WithInsecure(),
			WithRetry(5, 200*time.Millisecond, 10*time.Second),
		)
		if store.opts.maxRetries != 5 {
			t.Errorf("maxRetries = %d, want 5", store.opts.maxRetries)
		}
	})

	t.Run("WithCircuitBreaker", func(t *testing.T) {
		store, _ := NewRemoteStore("localhost:9999",
			WithInsecure(),
			WithCircuitBreaker(5, time.Minute),
		)
		if !store.opts.enableCircuit {
			t.Error("enableCircuit should be true")
		}
		if store.opts.circuitThreshold != 5 {
			t.Errorf("circuitThreshold = %d, want 5", store.opts.circuitThreshold)
		}
	})

	t.Run("WithWatchBufferSize", func(t *testing.T) {
		store, _ := NewRemoteStore("localhost:9999",
			WithInsecure(),
			WithWatchBufferSize(50),
		)
		if store.opts.watchBufferSize != 50 {
			t.Errorf("watchBufferSize = %d, want 50", store.opts.watchBufferSize)
		}
	})

	t.Run("WithWatchReconnect", func(t *testing.T) {
		store, _ := NewRemoteStore("localhost:9999",
			WithInsecure(),
			WithWatchReconnect(false, 0),
		)
		if store.opts.watchReconnect {
			t.Error("watchReconnect should be false")
		}
	})
}

func TestCircuitBreaker(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithCircuitBreaker(5, 100*time.Millisecond),
	)

	// Initially circuit should be closed
	if store.isCircuitOpen() {
		t.Error("Circuit should be closed initially")
	}

	// Record 5 failures to open circuit
	for i := 0; i < 5; i++ {
		store.recordFailure()
	}

	if !store.isCircuitOpen() {
		t.Error("Circuit should be open after 5 failures")
	}

	// Wait for circuit to half-open
	time.Sleep(150 * time.Millisecond)

	if store.isCircuitOpen() {
		t.Error("Circuit should be half-open after timeout")
	}

	// Success should reset circuit
	store.recordSuccess()
	store.recordFailure()
	if store.isCircuitOpen() {
		t.Error("Circuit should be closed after success reset")
	}
}

func TestCircuitBreaker_CustomThreshold(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithCircuitBreaker(3, 100*time.Millisecond),
	)

	// 2 failures should not open circuit with threshold 3
	for i := 0; i < 2; i++ {
		store.recordFailure()
	}
	if store.isCircuitOpen() {
		t.Error("Circuit should be closed after 2 failures with threshold 3")
	}

	// 3rd failure should open it
	store.recordFailure()
	if !store.isCircuitOpen() {
		t.Error("Circuit should be open after 3 failures with threshold 3")
	}
}

func TestProtoToValue(t *testing.T) {
	t.Run("nil entry", func(t *testing.T) {
		val := protoToValue(nil)
		if val != nil {
			t.Error("expected nil for nil entry")
		}
	})

	t.Run("with value and metadata", func(t *testing.T) {
		now := timestamppb.Now()
		entry := &configpb.Entry{
			Namespace: "ns",
			Key:       "key",
			Value:     []byte(`"hello"`),
			Codec:     "json",
			Version:   3,
			Type:      int32(config.TypeString),
			CreatedAt: now,
			UpdatedAt: now,
		}
		val := protoToValue(entry)
		if val == nil {
			t.Fatal("expected non-nil value")
		}
		s, err := val.String()
		if err != nil {
			t.Fatalf("String() error: %v", err)
		}
		if s != "hello" {
			t.Errorf("String() = %q, want %q", s, "hello")
		}
		if val.Metadata().Version() != 3 {
			t.Errorf("Version() = %d, want 3", val.Metadata().Version())
		}
	})

	t.Run("without metadata", func(t *testing.T) {
		entry := &configpb.Entry{
			Namespace: "ns",
			Key:       "key",
			Value:     []byte(`42`),
			Codec:     "json",
		}
		val := protoToValue(entry)
		if val == nil {
			t.Fatal("expected non-nil value")
		}
	})

	t.Run("invalid codec fallback", func(t *testing.T) {
		entry := &configpb.Entry{
			Namespace: "ns",
			Key:       "key",
			Value:     []byte("not-valid-json"),
			Codec:     "json",
		}
		val := protoToValue(entry)
		if val == nil {
			t.Fatal("expected non-nil value even with invalid data (fallback)")
		}
	})
}

func TestRemoteStore_WatchMaxErrors(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithWatchMaxErrors(3),
	)
	if store.opts.watchMaxErrors != 3 {
		t.Errorf("watchMaxErrors = %d, want 3", store.opts.watchMaxErrors)
	}
}

func TestIsNonRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{config.ErrNotFound, true},
		{config.ErrKeyExists, true},
		{config.ErrInvalidKey, true},
		{config.ErrInvalidNamespace, true},
		{config.ErrInvalidValue, true},
		{config.ErrReadOnly, true},
		{&PermissionDeniedError{}, true},
		{config.ErrStoreNotConnected, false},
		{errors.New("random error"), false},
		{fmt.Errorf("wrapped: %w", config.ErrNotFound), true},
		{fmt.Errorf("wrapped: %w", config.ErrKeyExists), true},
		{fmt.Errorf("wrapped: %w", config.ErrInvalidKey), true},
		{fmt.Errorf("wrapped: %w", config.ErrInvalidNamespace), true},
		{fmt.Errorf("wrapped: %w", config.ErrInvalidValue), true},
		{fmt.Errorf("wrapped: %w", config.ErrReadOnly), true},
		{fmt.Errorf("wrapped: %w", &PermissionDeniedError{Message: "denied"}), true},
		{fmt.Errorf("wrapped: %w", errors.New("transient")), false},
	}

	for _, tt := range tests {
		got := isNonRetryable(tt.err)
		if got != tt.want {
			t.Errorf("isNonRetryable(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestFromGRPCError_AllCodes(t *testing.T) {
	tests := []struct {
		name     string
		code     codes.Code
		msg      string
		want     error
		wantType any
	}{
		{"OK", codes.OK, "ok", nil, nil},
		{"NotFound", codes.NotFound, "not found", config.ErrNotFound, nil},
		{"AlreadyExists", codes.AlreadyExists, "exists", config.ErrKeyExists, nil},
		{"InvalidArgument", codes.InvalidArgument, "bad arg", config.ErrInvalidValue, nil},
		{"FailedPrecondition", codes.FailedPrecondition, "read only", config.ErrReadOnly, nil},
		{"Unavailable", codes.Unavailable, "down", config.ErrStoreNotConnected, nil},
		{"Unimplemented", codes.Unimplemented, "nope", config.ErrWatchNotSupported, nil},
		{"Canceled", codes.Canceled, "canceled", context.Canceled, nil},
		{"DeadlineExceeded", codes.DeadlineExceeded, "timeout", context.DeadlineExceeded, nil},
		{"OutOfRange", codes.OutOfRange, "out of range", config.ErrInvalidValue, nil},
		{"PermissionDenied", codes.PermissionDenied, "denied", ErrPermissionDenied, &PermissionDeniedError{}},
		{"Unauthenticated", codes.Unauthenticated, "unauth", ErrPermissionDenied, &PermissionDeniedError{}},
		{"Internal", codes.Internal, "internal", nil, &RemoteError{}},
		{"Unknown", codes.Unknown, "unknown", nil, &RemoteError{}},
		{"DataLoss", codes.DataLoss, "data lost", nil, &RemoteError{}},
		{"ResourceExhausted", codes.ResourceExhausted, "throttled", nil, &RemoteError{}},
		{"Aborted", codes.Aborted, "conflict", nil, &RemoteError{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fromGRPCError(status.Error(tt.code, tt.msg))
			if tt.want != nil {
				if !errors.Is(err, tt.want) {
					t.Errorf("fromGRPCError(code=%v) = %v, want %v", tt.code, err, tt.want)
				}
			} else if tt.wantType == nil && err != nil {
				t.Errorf("fromGRPCError(code=%v) = %v, want nil", tt.code, err)
			}
			if tt.wantType != nil {
				switch tt.wantType.(type) {
				case *PermissionDeniedError:
					var permErr *PermissionDeniedError
					if !errors.As(err, &permErr) {
						t.Errorf("fromGRPCError(code=%v) type = %T, want *PermissionDeniedError", tt.code, err)
					} else if permErr.Message != tt.msg {
						t.Errorf("PermissionDeniedError.Message = %q, want %q", permErr.Message, tt.msg)
					}
				case *RemoteError:
					var remoteErr *RemoteError
					if !errors.As(err, &remoteErr) {
						t.Errorf("fromGRPCError(code=%v) type = %T, want *RemoteError", tt.code, err)
					} else {
						if remoteErr.Code != tt.code {
							t.Errorf("RemoteError.Code = %v, want %v", remoteErr.Code, tt.code)
						}
						if remoteErr.Message != tt.msg {
							t.Errorf("RemoteError.Message = %q, want %q", remoteErr.Message, tt.msg)
						}
					}
				}
			}
		})
	}

	t.Run("nil error", func(t *testing.T) {
		if err := fromGRPCError(nil); err != nil {
			t.Errorf("fromGRPCError(nil) = %v, want nil", err)
		}
	})

	t.Run("non-grpc error passthrough", func(t *testing.T) {
		orig := errors.New("plain error")
		if got := fromGRPCError(orig); got != orig {
			t.Errorf("fromGRPCError(plain) = %v, want same error", got)
		}
	})

	t.Run("default code", func(t *testing.T) {
		err := fromGRPCError(status.Error(codes.Code(999), "weird"))
		var remoteErr *RemoteError
		if !errors.As(err, &remoteErr) {
			t.Fatalf("fromGRPCError(unknown code) type = %T, want *RemoteError", err)
		}
		if remoteErr.Code != codes.Code(999) {
			t.Errorf("RemoteError.Code = %v, want 999", remoteErr.Code)
		}
	})
}

func TestConnState_ReservedValue(t *testing.T) {
	reserved := ConnState(3)
	if got := reserved.String(); got != "unknown" {
		t.Errorf("ConnState(3).String() = %q, want %q", got, "unknown")
	}
}

func TestBuildDialOpts_Insecure(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithInsecure())
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}
	opts := store.opts.buildDialOpts()
	if len(opts) < 2 {
		t.Fatalf("buildDialOpts() returned %d opts, want at least 2 (credentials + keepalive)", len(opts))
	}
}

func TestBuildDialOpts_TLS(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999", WithTLS(nil))
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}
	if !store.opts.secure {
		t.Error("secure should be true with WithTLS")
	}
	opts := store.opts.buildDialOpts()
	if len(opts) < 2 {
		t.Fatalf("buildDialOpts() returned %d opts, want at least 2", len(opts))
	}
}

func TestBuildDialOpts_CustomTLS(t *testing.T) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
	store, err := NewRemoteStore("localhost:9999", WithTLS(tlsCfg))
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}
	if !store.opts.secure {
		t.Error("secure should be true")
	}
	if store.opts.tlsConfig != tlsCfg {
		t.Error("tlsConfig should be the provided config")
	}
	opts := store.opts.buildDialOpts()
	if len(opts) < 2 {
		t.Fatalf("buildDialOpts() returned %d opts, want at least 2", len(opts))
	}
}

func TestBuildDialOpts_WithKeepalive(t *testing.T) {
	store, err := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithKeepalive(15*time.Second, 5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}
	if store.opts.keepaliveTime != 15*time.Second {
		t.Errorf("keepaliveTime = %v, want 15s", store.opts.keepaliveTime)
	}
	if store.opts.keepaliveTimeout != 5*time.Second {
		t.Errorf("keepaliveTimeout = %v, want 5s", store.opts.keepaliveTimeout)
	}
	opts := store.opts.buildDialOpts()
	if len(opts) < 2 {
		t.Fatalf("buildDialOpts() returned %d opts, want at least 2", len(opts))
	}
}

func TestBuildDialOpts_WithDialOptions(t *testing.T) {
	customOpt := grpc.WithAuthority("custom-authority")
	store, err := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithDialOptions(customOpt),
	)
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}
	opts := store.opts.buildDialOpts()
	// credentials + keepalive + user option = at least 3
	if len(opts) < 3 {
		t.Fatalf("buildDialOpts() returned %d opts, want at least 3", len(opts))
	}
}

func TestCircuitBreaker_Disabled(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999", WithInsecure())

	// Circuit breaker is disabled by default
	if store.opts.enableCircuit {
		t.Fatal("circuit should be disabled by default")
	}

	// isCircuitOpen should always return false when disabled
	for i := 0; i < 100; i++ {
		store.recordFailure()
	}
	if store.isCircuitOpen() {
		t.Error("isCircuitOpen() should return false when circuit breaker is disabled")
	}

	// recordSuccess should be a no-op when disabled
	store.recordSuccess()
	if store.isCircuitOpen() {
		t.Error("circuit should remain closed when disabled")
	}
}

func TestCircuitBreaker_ResetCircuit(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithCircuitBreaker(3, time.Minute),
	)

	// Open the circuit
	for i := 0; i < 3; i++ {
		store.recordFailure()
	}
	if !store.isCircuitOpen() {
		t.Fatal("circuit should be open")
	}

	// resetCircuit should close it immediately
	store.resetCircuit()
	if store.isCircuitOpen() {
		t.Error("circuit should be closed after resetCircuit()")
	}

	// Verify failure counter was also reset: one failure should not open it
	store.recordFailure()
	if store.isCircuitOpen() {
		t.Error("circuit should not open after single failure post-reset")
	}
}

func TestCircuitBreaker_RecordSuccessResetsCount(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithCircuitBreaker(3, time.Minute),
	)

	// Accumulate 2 failures (below threshold)
	store.recordFailure()
	store.recordFailure()

	// Success resets the counter
	store.recordSuccess()

	// Now 2 more failures should not open circuit (counter was reset)
	store.recordFailure()
	store.recordFailure()
	if store.isCircuitOpen() {
		t.Error("circuit should not be open: success should have reset consecutive failures")
	}

	// Third failure (since last success) should open it
	store.recordFailure()
	if !store.isCircuitOpen() {
		t.Error("circuit should be open after 3 consecutive failures")
	}
}

func TestCircuitBreaker_GetClientReturnsError(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithCircuitBreaker(1, time.Minute),
	)

	// Connect so client is set
	store.Connect(context.Background())

	// Open the circuit
	store.recordFailure()

	_, err := store.getClient()
	if err == nil {
		t.Fatal("getClient() should return error when circuit is open")
	}
	var remoteErr *RemoteError
	if !errors.As(err, &remoteErr) {
		t.Errorf("getClient() error type = %T, want *RemoteError", err)
	}
}

func TestProtoToValue_OnlyCreatedAt(t *testing.T) {
	created := timestamppb.New(time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))
	entry := &configpb.Entry{
		Namespace: "ns",
		Key:       "key",
		Value:     []byte(`"test"`),
		Codec:     "json",
		Version:   1,
		CreatedAt: created,
	}
	val := protoToValue(entry)
	if val == nil {
		t.Fatal("expected non-nil value")
	}
	meta := val.Metadata()
	if meta.Version() != 1 {
		t.Errorf("Version() = %d, want 1", meta.Version())
	}
	if !meta.CreatedAt().Equal(created.AsTime()) {
		t.Errorf("CreatedAt() = %v, want %v", meta.CreatedAt(), created.AsTime())
	}
	if !meta.UpdatedAt().IsZero() {
		t.Errorf("UpdatedAt() = %v, want zero", meta.UpdatedAt())
	}
}

func TestProtoToValue_OnlyUpdatedAt(t *testing.T) {
	updated := timestamppb.New(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))
	entry := &configpb.Entry{
		Namespace: "ns",
		Key:       "key",
		Value:     []byte(`true`),
		Codec:     "json",
		Version:   2,
		UpdatedAt: updated,
	}
	val := protoToValue(entry)
	if val == nil {
		t.Fatal("expected non-nil value")
	}
	meta := val.Metadata()
	if !meta.CreatedAt().IsZero() {
		t.Errorf("CreatedAt() = %v, want zero", meta.CreatedAt())
	}
	if !meta.UpdatedAt().Equal(updated.AsTime()) {
		t.Errorf("UpdatedAt() = %v, want %v", meta.UpdatedAt(), updated.AsTime())
	}
}

func TestProtoToValue_TypeWithoutVersionOrTimestamps(t *testing.T) {
	entry := &configpb.Entry{
		Namespace: "ns",
		Key:       "key",
		Value:     []byte(`100`),
		Codec:     "json",
		Type:      int32(config.TypeInt),
	}
	val := protoToValue(entry)
	if val == nil {
		t.Fatal("expected non-nil value")
	}
	if val.Type() != config.TypeInt {
		t.Errorf("Type() = %v, want TypeInt", val.Type())
	}
}

func TestProtoToValue_EmptyValueBytes(t *testing.T) {
	entry := &configpb.Entry{
		Namespace: "ns",
		Key:       "key",
		Value:     []byte{},
		Codec:     "json",
	}
	val := protoToValue(entry)
	// Even with empty bytes, protoToValue should not panic.
	// It may return a value via fallback or via decoding.
	// The key property is that it does not return nil (entry is non-nil).
	if val == nil {
		t.Fatal("expected non-nil value for non-nil entry")
	}
}

func TestProtoToValue_VersionOnlyNoTimestamps(t *testing.T) {
	entry := &configpb.Entry{
		Namespace: "ns",
		Key:       "key",
		Value:     []byte(`"data"`),
		Codec:     "json",
		Version:   5,
	}
	val := protoToValue(entry)
	if val == nil {
		t.Fatal("expected non-nil value")
	}
	if val.Metadata().Version() != 5 {
		t.Errorf("Version() = %d, want 5", val.Metadata().Version())
	}
}

func TestOptions_WithWatchReconnectWaitTime(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithWatchReconnect(true, 3*time.Second),
	)
	if !store.opts.watchReconnect {
		t.Error("watchReconnect should be true")
	}
	if store.opts.watchReconnectWait != 3*time.Second {
		t.Errorf("watchReconnectWait = %v, want 3s", store.opts.watchReconnectWait)
	}
}

func TestOptions_WithWatchErrorCallback(t *testing.T) {
	called := false
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithWatchErrorCallback(func(err error) {
			called = true
		}),
	)
	if store.opts.onWatchError == nil {
		t.Fatal("onWatchError should be set")
	}
	store.opts.onWatchError(errors.New("test"))
	if !called {
		t.Error("watch error callback was not called")
	}
}

func TestOptions_WithKeepalive(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithKeepalive(20*time.Second, 8*time.Second),
	)
	if store.opts.keepaliveTime != 20*time.Second {
		t.Errorf("keepaliveTime = %v, want 20s", store.opts.keepaliveTime)
	}
	if store.opts.keepaliveTimeout != 8*time.Second {
		t.Errorf("keepaliveTimeout = %v, want 8s", store.opts.keepaliveTimeout)
	}
}

func TestOptions_WithStateCallback(t *testing.T) {
	var received ConnState
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithStateCallback(func(state ConnState) {
			received = state
		}),
	)
	if store.opts.onStateChange == nil {
		t.Fatal("onStateChange should be set")
	}
	store.opts.onStateChange(ConnStateConnected)
	if received != ConnStateConnected {
		t.Errorf("state callback received %v, want Connected", received)
	}
}

func TestOptions_WithWatchMaxErrors(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithWatchMaxErrors(7),
	)
	if store.opts.watchMaxErrors != 7 {
		t.Errorf("watchMaxErrors = %d, want 7", store.opts.watchMaxErrors)
	}
}

func TestOptions_WithWatchBufferSizeZero(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithWatchBufferSize(0),
	)
	if store.opts.watchBufferSize != 0 {
		t.Errorf("watchBufferSize = %d, want 0", store.opts.watchBufferSize)
	}
}

func TestOptions_DefaultValues(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999", WithInsecure())
	if store.opts.maxRetries != 3 {
		t.Errorf("default maxRetries = %d, want 3", store.opts.maxRetries)
	}
	if store.opts.retryBackoff != 100*time.Millisecond {
		t.Errorf("default retryBackoff = %v, want 100ms", store.opts.retryBackoff)
	}
	if store.opts.maxBackoff != 5*time.Second {
		t.Errorf("default maxBackoff = %v, want 5s", store.opts.maxBackoff)
	}
	if store.opts.watchBufferSize != 100 {
		t.Errorf("default watchBufferSize = %d, want 100", store.opts.watchBufferSize)
	}
	if !store.opts.watchReconnect {
		t.Error("default watchReconnect should be true")
	}
	if store.opts.watchReconnectWait != time.Second {
		t.Errorf("default watchReconnectWait = %v, want 1s", store.opts.watchReconnectWait)
	}
	if store.opts.watchMaxErrors != 10 {
		t.Errorf("default watchMaxErrors = %d, want 10", store.opts.watchMaxErrors)
	}
	if store.opts.keepaliveTime != 30*time.Second {
		t.Errorf("default keepaliveTime = %v, want 30s", store.opts.keepaliveTime)
	}
	if store.opts.keepaliveTimeout != 10*time.Second {
		t.Errorf("default keepaliveTimeout = %v, want 10s", store.opts.keepaliveTimeout)
	}
	if store.opts.enableCircuit {
		t.Error("default enableCircuit should be false")
	}
}

func TestOptions_WithTLS_NilConfig(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999", WithTLS(nil))
	if !store.opts.secure {
		t.Error("secure should be true")
	}
	if store.opts.tlsConfig != nil {
		t.Error("tlsConfig should be nil when WithTLS(nil)")
	}
}

func TestOptions_WithDialOptions(t *testing.T) {
	opt1 := grpc.WithAuthority("a")
	opt2 := grpc.WithAuthority("b")
	store, _ := NewRemoteStore("localhost:9999",
		WithInsecure(),
		WithDialOptions(opt1, opt2),
	)
	if len(store.opts.dialOpts) != 2 {
		t.Errorf("dialOpts length = %d, want 2", len(store.opts.dialOpts))
	}
}

func TestConnectAfterClose(t *testing.T) {
	store, _ := NewRemoteStore("localhost:9999", WithInsecure())
	store.Close(context.Background())

	err := store.Connect(context.Background())
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Connect() after Close() = %v, want ErrStoreClosed", err)
	}
}
