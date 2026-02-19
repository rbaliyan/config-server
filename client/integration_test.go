package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// setupIntegrationTest starts an in-process gRPC server backed by a memory store
// and returns a connected RemoteStore. The server and store are cleaned up when
// the test finishes.
func setupIntegrationTest(t *testing.T) *RemoteStore {
	t.Helper()

	// Create and connect the memory store.
	memStore := memory.NewStore(memory.WithWatchBufferSize(100))
	ctx := context.Background()
	if err := memStore.Connect(ctx); err != nil {
		t.Fatalf("failed to connect memory store: %v", err)
	}

	// Create the gRPC service with AllowAll authorizer for testing.
	svc, err := service.NewService(memStore, service.WithAuthorizer(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// Create a bufconn listener for in-process communication.
	lis := bufconn.Listen(bufSize)

	// Register and start the gRPC server.
	srv := grpc.NewServer()
	configpb.RegisterConfigServiceServer(srv, svc)

	go func() {
		// Server.Serve returns after GracefulStop; not an error.
		_ = srv.Serve(lis)
	}()

	// Build the RemoteStore with a custom dialer that uses bufconn.
	store, err := NewRemoteStore("passthrough:///bufconn",
		WithInsecure(),
		WithRetry(0, 0, 0), // No retries for deterministic tests.
		WithWatchReconnect(false, 0),
		WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}), grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("NewRemoteStore failed: %v", err)
	}

	if err := store.Connect(ctx); err != nil {
		t.Fatalf("RemoteStore.Connect failed: %v", err)
	}

	t.Cleanup(func() {
		store.Close(context.Background())
		srv.GracefulStop()
		memStore.Close(context.Background())
	})

	return store
}

func TestIntegration_CRUD(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	// Set a value.
	val := config.NewValue("hello-world")
	result, err := store.Set(ctx, "test-ns", "greeting", val)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if result == nil {
		t.Fatal("Set returned nil value")
	}

	// Get it back.
	got, err := store.Get(ctx, "test-ns", "greeting")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	str, err := got.String()
	if err != nil {
		t.Fatalf("Value.String() failed: %v", err)
	}
	if str != "hello-world" {
		t.Errorf("Get returned %q, want %q", str, "hello-world")
	}

	// Verify metadata round-trip: version should be 1 for a new key.
	if got.Metadata().Version() != 1 {
		t.Errorf("Version = %d, want 1", got.Metadata().Version())
	}

	// Delete it.
	if err := store.Delete(ctx, "test-ns", "greeting"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone.
	_, err = store.Get(ctx, "test-ns", "greeting")
	if !errors.Is(err, config.ErrNotFound) {
		t.Errorf("Get after Delete: got err = %v, want ErrNotFound", err)
	}
}

func TestIntegration_Find(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	// Seed multiple values with a common prefix.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("app/setting-%d", i)
		_, err := store.Set(ctx, "find-ns", key, config.NewValue(i))
		if err != nil {
			t.Fatalf("Set(%q) failed: %v", key, err)
		}
	}

	// Also set a value outside the prefix.
	_, err := store.Set(ctx, "find-ns", "other/key", config.NewValue("x"))
	if err != nil {
		t.Fatalf("Set(other/key) failed: %v", err)
	}

	// Find with prefix filter.
	filter := config.NewFilter().WithPrefix("app/").Build()
	page, err := store.Find(ctx, "find-ns", filter)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(page.Results()) != 5 {
		t.Errorf("Find with prefix 'app/' returned %d results, want 5", len(page.Results()))
	}

	// Test pagination: limit to 2 per page.
	filter = config.NewFilter().WithPrefix("app/").WithLimit(2).Build()
	page, err = store.Find(ctx, "find-ns", filter)
	if err != nil {
		t.Fatalf("Find (page 1) failed: %v", err)
	}
	if len(page.Results()) != 2 {
		t.Errorf("Find page 1 returned %d results, want 2", len(page.Results()))
	}
	if page.NextCursor() == "" {
		t.Error("expected non-empty cursor for page 1")
	}

	// Fetch page 2 using cursor.
	filter = config.NewFilter().WithPrefix("app/").WithLimit(2).WithCursor(page.NextCursor()).Build()
	page2, err := store.Find(ctx, "find-ns", filter)
	if err != nil {
		t.Fatalf("Find (page 2) failed: %v", err)
	}
	if len(page2.Results()) != 2 {
		t.Errorf("Find page 2 returned %d results, want 2", len(page2.Results()))
	}
}

func TestIntegration_Watch(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start watching before making changes.
	watchResult, err := store.WatchWithResult(ctx, config.WatchFilter{
		Namespaces: []string{"watch-ns"},
	})
	if err != nil {
		t.Fatalf("WatchWithResult failed: %v", err)
	}
	defer watchResult.Stop()

	// Give the watch stream time to establish.
	time.Sleep(100 * time.Millisecond)

	// Make a change.
	_, err = store.Set(ctx, "watch-ns", "live-key", config.NewValue("live-value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for the event.
	select {
	case event, ok := <-watchResult.Events:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		if event.Key != "live-key" {
			t.Errorf("event Key = %q, want %q", event.Key, "live-key")
		}
		if event.Namespace != "watch-ns" {
			t.Errorf("event Namespace = %q, want %q", event.Namespace, "watch-ns")
		}
		if event.Type != config.ChangeTypeSet {
			t.Errorf("event Type = %v, want ChangeTypeSet", event.Type)
		}
		if event.Value == nil {
			t.Error("event Value should not be nil for a Set event")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for watch event")
	}

	// Now test delete event.
	if err := store.Delete(ctx, "watch-ns", "live-key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	select {
	case event, ok := <-watchResult.Events:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		if event.Key != "live-key" {
			t.Errorf("delete event Key = %q, want %q", event.Key, "live-key")
		}
		if event.Type != config.ChangeTypeDelete {
			t.Errorf("delete event Type = %v, want ChangeTypeDelete", event.Type)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delete watch event")
	}
}

func TestIntegration_ConditionalWrites(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	// WriteModeCreate: first write should succeed.
	val := config.NewValue("first", config.WithValueWriteMode(config.WriteModeCreate))
	_, err := store.Set(ctx, "cond-ns", "unique-key", val)
	if err != nil {
		t.Fatalf("Create (first) failed: %v", err)
	}

	// WriteModeCreate: second write to same key should fail with ErrKeyExists.
	val2 := config.NewValue("second", config.WithValueWriteMode(config.WriteModeCreate))
	_, err = store.Set(ctx, "cond-ns", "unique-key", val2)
	if !errors.Is(err, config.ErrKeyExists) {
		t.Errorf("Create (duplicate) err = %v, want ErrKeyExists", err)
	}

	// WriteModeUpdate: updating existing key should succeed.
	val3 := config.NewValue("updated", config.WithValueWriteMode(config.WriteModeUpdate))
	_, err = store.Set(ctx, "cond-ns", "unique-key", val3)
	if err != nil {
		t.Fatalf("Update (existing) failed: %v", err)
	}

	// Verify the value was updated.
	got, err := store.Get(ctx, "cond-ns", "unique-key")
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	str, err := got.String()
	if err != nil {
		t.Fatalf("Value.String() failed: %v", err)
	}
	if str != "updated" {
		t.Errorf("Value after update = %q, want %q", str, "updated")
	}

	// WriteModeUpdate: updating nonexistent key should fail with ErrNotFound.
	val4 := config.NewValue("nope", config.WithValueWriteMode(config.WriteModeUpdate))
	_, err = store.Set(ctx, "cond-ns", "nonexistent-key", val4)
	if !errors.Is(err, config.ErrNotFound) {
		t.Errorf("Update (nonexistent) err = %v, want ErrNotFound", err)
	}
}

func TestIntegration_ErrorMapping(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	t.Run("GetNotFound", func(t *testing.T) {
		_, err := store.Get(ctx, "err-ns", "missing")
		if !errors.Is(err, config.ErrNotFound) {
			t.Errorf("Get(missing) err = %v, want ErrNotFound", err)
		}
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		err := store.Delete(ctx, "err-ns", "missing")
		if !errors.Is(err, config.ErrNotFound) {
			t.Errorf("Delete(missing) err = %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		val := config.NewValue("x", config.WithValueWriteMode(config.WriteModeCreate))
		_, err := store.Set(ctx, "err-ns", "dup-key", val)
		if err != nil {
			t.Fatalf("first Create failed: %v", err)
		}

		_, err = store.Set(ctx, "err-ns", "dup-key", val)
		if !errors.Is(err, config.ErrKeyExists) {
			t.Errorf("duplicate Create err = %v, want ErrKeyExists", err)
		}
	})

	t.Run("UpdateNonexistent", func(t *testing.T) {
		val := config.NewValue("x", config.WithValueWriteMode(config.WriteModeUpdate))
		_, err := store.Set(ctx, "err-ns", "ghost", val)
		if !errors.Is(err, config.ErrNotFound) {
			t.Errorf("Update(ghost) err = %v, want ErrNotFound", err)
		}
	})
}

func TestIntegration_TypeRoundTrip(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	tests := []struct {
		name   string
		key    string
		input  any
		verify func(t *testing.T, v config.Value)
	}{
		{
			name:  "string",
			key:   "str",
			input: "hello",
			verify: func(t *testing.T, v config.Value) {
				s, err := v.String()
				if err != nil {
					t.Fatalf("String() error: %v", err)
				}
				if s != "hello" {
					t.Errorf("String() = %q, want %q", s, "hello")
				}
			},
		},
		{
			name:  "int",
			key:   "num",
			input: 42,
			verify: func(t *testing.T, v config.Value) {
				n, err := v.Int64()
				if err != nil {
					t.Fatalf("Int64() error: %v", err)
				}
				if n != 42 {
					t.Errorf("Int64() = %d, want 42", n)
				}
			},
		},
		{
			name:  "bool",
			key:   "flag",
			input: true,
			verify: func(t *testing.T, v config.Value) {
				b, err := v.Bool()
				if err != nil {
					t.Fatalf("Bool() error: %v", err)
				}
				if !b {
					t.Error("Bool() = false, want true")
				}
			},
		},
		{
			name:  "float",
			key:   "pi",
			input: 3.14,
			verify: func(t *testing.T, v config.Value) {
				f, err := v.Float64()
				if err != nil {
					t.Fatalf("Float64() error: %v", err)
				}
				if f != 3.14 {
					t.Errorf("Float64() = %f, want 3.14", f)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.Set(ctx, "types-ns", tt.key, config.NewValue(tt.input))
			if err != nil {
				t.Fatalf("Set failed: %v", err)
			}

			got, err := store.Get(ctx, "types-ns", tt.key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			tt.verify(t, got)
		})
	}
}

func TestIntegration_VersionIncrement(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	// First set: version should be 1.
	_, err := store.Set(ctx, "ver-ns", "counter", config.NewValue(1))
	if err != nil {
		t.Fatalf("Set #1 failed: %v", err)
	}

	v1, err := store.Get(ctx, "ver-ns", "counter")
	if err != nil {
		t.Fatalf("Get #1 failed: %v", err)
	}
	if v1.Metadata().Version() != 1 {
		t.Errorf("Version after first set = %d, want 1", v1.Metadata().Version())
	}

	// Second set: version should be 2.
	_, err = store.Set(ctx, "ver-ns", "counter", config.NewValue(2))
	if err != nil {
		t.Fatalf("Set #2 failed: %v", err)
	}

	v2, err := store.Get(ctx, "ver-ns", "counter")
	if err != nil {
		t.Fatalf("Get #2 failed: %v", err)
	}
	if v2.Metadata().Version() != 2 {
		t.Errorf("Version after second set = %d, want 2", v2.Metadata().Version())
	}
}
