package gateway

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// newRemoteTestClient stands up the gRPC ConfigService over a bufconn listener
// backed by an in-memory store, and returns a client wired to it plus the
// store so tests can seed/mutate data directly. The store is connected and all
// resources are cleaned up via t.Cleanup.
func newRemoteTestClient(t *testing.T) (configpb.ConfigServiceClient, config.Store) {
	t.Helper()

	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect store: %v", err)
	}
	t.Cleanup(func() { store.Close(ctx) })

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	configpb.RegisterConfigServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return configpb.NewConfigServiceClient(conn), store
}

func TestRemoteDiffHandler(t *testing.T) {
	t.Parallel()

	client, store := newRemoteTestClient(t)

	// Seed two versions of one key via the store (memory store is versioned).
	ctx := context.Background()
	if _, err := store.Set(ctx, "ns", "key", config.NewValue("v1-value")); err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	if _, err := store.Set(ctx, "ns", "key", config.NewValue("v2-value")); err != nil {
		t.Fatalf("seed v2: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /v1/namespaces/{namespace}/keys/{key}/diff", newRemoteDiffHandler(client))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tests := []struct {
		name        string
		path        string
		wantStatus  int
		wantChanged *bool // only checked when wantStatus == 200
	}{
		{
			name:        "valid diff changed",
			path:        "/v1/namespaces/ns/keys/key/diff?v1=1&v2=2",
			wantStatus:  http.StatusOK,
			wantChanged: boolPtr(true),
		},
		{
			name:        "valid diff unchanged (same version)",
			path:        "/v1/namespaces/ns/keys/key/diff?v1=2&v2=2",
			wantStatus:  http.StatusOK,
			wantChanged: boolPtr(false),
		},
		{
			name:       "missing v1",
			path:       "/v1/namespaces/ns/keys/key/diff?v2=2",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing v2",
			path:       "/v1/namespaces/ns/keys/key/diff?v1=1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-numeric v2",
			path:       "/v1/namespaces/ns/keys/key/diff?v1=1&v2=abc",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "version not found",
			path:       "/v1/namespaces/ns/keys/key/diff?v1=1&v2=99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "key not found",
			path:       "/v1/namespaces/ns/keys/missing/diff?v1=1&v2=2",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, err := http.Get(srv.URL + tt.path)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus != http.StatusOK {
				return
			}

			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}

			var body diffResponse
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Namespace != "ns" {
				t.Errorf("namespace = %q, want ns", body.Namespace)
			}
			if body.Key != "key" {
				t.Errorf("key = %q, want key", body.Key)
			}
			if tt.wantChanged != nil && body.Changed != *tt.wantChanged {
				t.Errorf("changed = %v, want %v", body.Changed, *tt.wantChanged)
			}
		})
	}
}

func TestRemoteSSEHandler(t *testing.T) {
	t.Parallel()

	client, store := newRemoteTestClient(t)

	sseHandler := newRemoteSSEHandler(client, 30*time.Second, newEventBuffer(defaultEventBufferSize))
	mux := http.NewServeMux()
	mux.Handle("GET /v1/watch", sseHandler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/watch?namespaces=test", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET stream: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Read the stream incrementally in a goroutine, appending to a buffer the
	// test goroutine polls with a bounded deadline (no fixed sleep).
	type result struct {
		mu  sync.Mutex
		buf strings.Builder
	}
	res := &result{}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		chunk := make([]byte, 256)
		for {
			n, err := resp.Body.Read(chunk)
			if n > 0 {
				res.mu.Lock()
				res.buf.Write(chunk[:n])
				res.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	bodyNow := func() string {
		res.mu.Lock()
		defer res.mu.Unlock()
		return res.buf.String()
	}

	// Wait for the SSE preamble (connected comment) before writing the key,
	// guaranteeing the watch is registered server-side.
	testutil.WaitFor(t, 15*time.Second, 10*time.Millisecond, func() bool {
		return strings.Contains(bodyNow(), ": connected")
	}, "remote SSE preamble")

	if _, err := store.Set(context.Background(), "test", "remote-key", config.NewValue("remote-val")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Wait for a data: line carrying the SET event.
	testutil.WaitFor(t, 15*time.Second, 10*time.Millisecond, func() bool {
		return strings.Contains(bodyNow(), "event: set") &&
			strings.Contains(bodyNow(), `"key":"remote-key"`)
	}, "remote SSE SET event")

	// Tear down: cancel the request context and drain the reader.
	cancel()
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not exit after cancel")
	}

	body := bodyNow()
	evt, ok := findSSEEvent(t, body, "SET")
	if !ok {
		t.Fatalf("expected SET event in remote SSE output, got:\n%s", body)
	}
	if evt.Key != "remote-key" {
		t.Errorf("key = %q, want remote-key", evt.Key)
	}
	if evt.Namespace != "test" {
		t.Errorf("namespace = %q, want test", evt.Namespace)
	}
}
