package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
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
		{ConnStateReconnecting, "reconnecting"},
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
	t.Run("WithConnectTimeout", func(t *testing.T) {
		store, _ := NewRemoteStore("localhost:9999",
			WithInsecure(),
			WithConnectTimeout(5*time.Second),
		)
		if store.opts.connectTimeout != 5*time.Second {
			t.Errorf("connectTimeout = %v, want 5s", store.opts.connectTimeout)
		}
	})

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
			WithCircuitBreaker(time.Minute),
		)
		if !store.opts.enableCircuit {
			t.Error("enableCircuit should be true")
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
		WithCircuitBreaker(100*time.Millisecond),
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

func TestIsNonRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{config.ErrNotFound, true},
		{config.ErrKeyExists, true},
		{config.ErrInvalidKey, true},
		{config.ErrInvalidValue, true},
		{config.ErrReadOnly, true},
		{&PermissionDeniedError{}, true},
		{config.ErrStoreNotConnected, false},
		{errors.New("random error"), false},
	}

	for _, tt := range tests {
		got := isNonRetryable(tt.err)
		if got != tt.want {
			t.Errorf("isNonRetryable(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
