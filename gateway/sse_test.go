package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

// syncRecorder is a thread-safe wrapper around httptest.ResponseRecorder.
// It synchronizes writes (from the handler goroutine) with reads (from the
// test goroutine) to avoid data races when polling the body.
type syncRecorder struct {
	mu  sync.Mutex
	rec *httptest.ResponseRecorder
}

func newSyncRecorder() *syncRecorder {
	return &syncRecorder{rec: httptest.NewRecorder()}
}

func (sr *syncRecorder) Header() http.Header { return sr.rec.Header() }

func (sr *syncRecorder) Write(b []byte) (int, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.rec.Write(b)
}

func (sr *syncRecorder) WriteHeader(code int) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.rec.WriteHeader(code)
}

func (sr *syncRecorder) Flush() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.rec.Flush()
}

func (sr *syncRecorder) bodyString() string {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.rec.Body.String()
}

// result returns a snapshot of the recorder's state. Call only after the
// handler goroutine has finished to avoid races.
func (sr *syncRecorder) result() *httptest.ResponseRecorder {
	return sr.rec
}

// waitForBody polls the recorder body until pred returns true or timeout expires.
func waitForBody(t *testing.T, rec *syncRecorder, timeout time.Duration, pred func(string) bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred(rec.bodyString()) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// findSSEEvent scans SSE body text and returns the first sseEvent with the given type.
func findSSEEvent(t *testing.T, body, eventType string) (sseEvent, bool) {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		var evt sseEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			continue
		}
		if evt.Type == eventType {
			return evt, true
		}
	}
	return sseEvent{}, false
}

func setupSSETest(t *testing.T, opts ...service.Option) (*Handler, config.Store) {
	t.Helper()

	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	t.Cleanup(func() { store.Close(ctx) })

	if len(opts) == 0 {
		opts = []service.Option{service.WithAuthorizer(service.AllowAll())}
	}
	svc := service.NewService(store, opts...)

	handler, err := NewInProcessHandler(ctx, svc, WithHeartbeatInterval(30*time.Second))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	return handler, store
}

func TestSSEWatch_InProcess(t *testing.T) {
	handler, store := setupSSETest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	// Wait for the preamble before triggering a change.
	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble")
	}

	store.Set(context.Background(), "test", "app/timeout", config.NewValue("30"))

	// Wait for the SET event to appear.
	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, "event: set")
	}) {
		t.Fatal("timed out waiting for SET event")
	}

	cancel()
	<-done

	body := rec.bodyString()

	// Parse SSE events — find the first SET event data line.
	evt, ok := findSSEEvent(t, body, "SET")
	if !ok {
		t.Fatal("expected at least one SET event in SSE output")
	}
	if evt.Namespace != "test" {
		t.Errorf("namespace = %q, want test", evt.Namespace)
	}
	if evt.Key != "app/timeout" {
		t.Errorf("key = %q, want app/timeout", evt.Key)
	}
}

func TestSSEWatch_DeleteEvent(t *testing.T) {
	handler, store := setupSSETest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-create a key to delete.
	store.Set(context.Background(), "test", "old-key", config.NewValue("val"))

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble")
	}

	store.Delete(context.Background(), "test", "old-key")

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, "event: delete")
	}) {
		t.Fatal("timed out waiting for DELETE event")
	}

	cancel()
	<-done

	body := rec.bodyString()

	evt, ok := findSSEEvent(t, body, "DELETE")
	if !ok {
		t.Fatal("expected a DELETE event in SSE output")
	}
	if evt.Key != "old-key" {
		t.Errorf("key = %q, want old-key", evt.Key)
	}
}

func TestSSEWatch_Heartbeat(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	// Use a very short heartbeat interval for testing.
	handler, err := NewInProcessHandler(ctx, svc, WithHeartbeatInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(reqCtx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": heartbeat")
	}) {
		t.Fatal("timed out waiting for heartbeat comment")
	}

	cancel()
	<-done
}

func TestSSEWatch_ClientDisconnect(t *testing.T) {
	handler, _ := setupSSETest(t)

	ctx, cancel := context.WithCancel(context.Background())

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble")
	}

	// Simulate client disconnect.
	cancel()

	select {
	case <-done:
		// Clean shutdown.
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after client disconnect")
	}
}

func TestSSEWatch_Authorization(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	// DenyAll authorizer.
	svc := service.NewService(store)

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=secret", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	<-done

	body := rec.Body.String()

	// The in-process handler writes SSE headers before calling svc.Watch,
	// so we get an error event instead of an HTTP error code.
	if !strings.Contains(body, "event: error") {
		t.Errorf("expected SSE error event for denied access, got:\n%s", body)
	}
}

func TestSSEWatch_MethodNotAllowed(t *testing.T) {
	handler, _ := setupSSETest(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/watch", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /v1/watch status = %d, want 405", rec.Code)
	}
}

func TestSSEWatch_QueryParams(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet,
		"/v1/watch?namespaces=ns1&namespaces=ns2&prefixes=app/&prefixes=db/", nil)

	req := parseWatchQuery(r)

	if len(req.Namespaces) != 2 || req.Namespaces[0] != "ns1" || req.Namespaces[1] != "ns2" {
		t.Errorf("namespaces = %v, want [ns1 ns2]", req.Namespaces)
	}
	if len(req.Prefixes) != 2 || req.Prefixes[0] != "app/" || req.Prefixes[1] != "db/" {
		t.Errorf("prefixes = %v, want [app/ db/]", req.Prefixes)
	}
}

func TestSSEWatch_Headers(t *testing.T) {
	handler, _ := setupSSETest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble")
	}
	cancel()
	<-done

	result := rec.result()
	if ct := result.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if cc := result.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", cc)
	}
	if xab := result.Header().Get("X-Accel-Buffering"); xab != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", xab)
	}
}

// nonFlushWriter is an http.ResponseWriter that does not implement http.Flusher.
type nonFlushWriter struct {
	code int
	body strings.Builder
	h    http.Header
}

func (w *nonFlushWriter) Header() http.Header        { return w.h }
func (w *nonFlushWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *nonFlushWriter) WriteHeader(code int)        { w.code = code }

func TestSSEWatch_NoFlusher(t *testing.T) {
	handler, _ := setupSSETest(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	w := &nonFlushWriter{code: http.StatusOK, h: make(http.Header)}

	handler.ServeHTTP(w, req)

	if w.code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.code)
	}
	if !strings.Contains(w.body.String(), "streaming not supported") {
		t.Errorf("body = %q, want 'streaming not supported'", w.body.String())
	}
}

func TestSSEWatch_Preamble(t *testing.T) {
	handler, _ := setupSSETest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(ctx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble")
	}
	cancel()
	<-done

	body := rec.bodyString()

	if !strings.Contains(body, "retry: 5000") {
		t.Errorf("expected retry field in preamble, got:\n%s", body)
	}
	if !strings.Contains(body, ": connected") {
		t.Errorf("expected connected comment in preamble, got:\n%s", body)
	}

	// Verify preamble comes before any event data.
	retryIdx := strings.Index(body, "retry: 5000")
	connIdx := strings.Index(body, ": connected")
	if retryIdx > connIdx {
		t.Errorf("retry field should come before connected comment")
	}
}

func TestSSEWatch_ExistingRoutes_StillWork(t *testing.T) {
	handler, store := setupSSETest(t)

	// Seed data.
	store.Set(context.Background(), "test", "key1", config.NewValue("val1"))

	// Existing gRPC-Gateway routes should still work.
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces/test/keys/key1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET key status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestWithHeartbeatInterval(t *testing.T) {
	t.Run("positive", func(t *testing.T) {
		o := &options{}
		WithHeartbeatInterval(10 * time.Second)(o)
		if o.heartbeatInterval != 10*time.Second {
			t.Errorf("heartbeatInterval = %v, want 10s", o.heartbeatInterval)
		}
	})

	t.Run("zero_ignored", func(t *testing.T) {
		o := &options{heartbeatInterval: 5 * time.Second}
		WithHeartbeatInterval(0)(o)
		if o.heartbeatInterval != 5*time.Second {
			t.Errorf("heartbeatInterval = %v, want 5s (unchanged)", o.heartbeatInterval)
		}
	})

	t.Run("negative_ignored", func(t *testing.T) {
		o := &options{heartbeatInterval: 5 * time.Second}
		WithHeartbeatInterval(-1 * time.Second)(o)
		if o.heartbeatInterval != 5*time.Second {
			t.Errorf("heartbeatInterval = %v, want 5s (unchanged)", o.heartbeatInterval)
		}
	})
}

func TestHandler_Close(t *testing.T) {
	ctx := context.Background()

	handler, err := NewHandler(ctx, "localhost:19999", WithInsecure())
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}

	// Close should succeed.
	if err := handler.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should be safe (no-op via sync.Once).
	if err := handler.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}
}

func TestHandler_Close_InProcess(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	// Close on in-process handler is a no-op but should not fail.
	if err := handler.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should be safe.
	if err := handler.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}
}

// setupBufconn creates a gRPC server on a bufconn listener and returns the
// listener and a cleanup function.
func setupBufconn(t *testing.T, store config.Store) *bufconn.Listener {
	t.Helper()

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	configpb.RegisterConfigServiceServer(grpcServer, svc)

	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	return lis
}

func TestSSEWatch_Remote(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	lis := setupBufconn(t, store)

	handler, err := NewHandler(ctx, "passthrough:///bufnet",
		WithInsecure(),
		WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})),
	)
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	defer handler.Close()

	// Make SSE watch request.
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req = req.WithContext(reqCtx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	// Wait for preamble before triggering a change.
	if !waitForBody(t, rec, 5*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for remote SSE preamble")
	}

	store.Set(context.Background(), "test", "remote-key", config.NewValue("remote-val"))

	if !waitForBody(t, rec, 5*time.Second, func(s string) bool {
		return strings.Contains(s, "event: set")
	}) {
		t.Fatal("timed out waiting for remote SET event")
	}

	reqCancel()
	<-done

	body := rec.bodyString()

	if !strings.Contains(body, "retry: 5000") {
		t.Errorf("expected retry field, got:\n%s", body)
	}

	// Verify SET event received through remote relay.
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

func TestSSEWatch_Remote_AuthDenied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	// Use DenyAll authorizer on the server.
	svc := service.NewService(store)
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	configpb.RegisterConfigServiceServer(grpcServer, svc)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	handler, err := NewHandler(ctx, "passthrough:///bufnet",
		WithInsecure(),
		WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})),
	)
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	defer handler.Close()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=secret", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	<-done

	// In gRPC server-streaming RPCs, client.Watch() returns a stream
	// immediately. The auth error arrives on stream.Recv() after SSE
	// headers are already committed, so we get 200 + SSE error event.
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Errorf("expected SSE error event in body: %s", body)
	}
	if !strings.Contains(body, "no authorizer configured") {
		t.Errorf("expected auth denial message in body: %s", body)
	}
}

func TestSSEWatch_Remote_ClosedConn(t *testing.T) {
	// Exercise the writeHTTPError path: when the gRPC connection is closed,
	// client.Watch() itself returns an error before SSE headers are sent.

	// Create a connection to a non-existent server that will fail immediately.
	conn, err := grpc.NewClient("passthrough:///closed",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			// Return a connection that's immediately closed.
			server, client := net.Pipe()
			server.Close()
			return client, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	client := configpb.NewConfigServiceClient(conn)
	sseHandler := newRemoteSSEHandler(client, 30*time.Second)
	handler := composeHandlers(http.NewServeMux(), sseHandler)

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after closed connection")
	}

	// With a broken connection, we may get either:
	// - An HTTP error (if client.Watch() fails) with non-200 status
	// - An SSE error event (if client.Watch() succeeds but Recv() fails)
	// Either way, the response should contain error information.
	body := rec.Body.String()
	if rec.Code == http.StatusOK {
		// SSE error event path — error came through Recv().
		if !strings.Contains(body, "event: error") {
			t.Errorf("expected SSE error event in response, got:\n%s", body)
		}
	}
	// Non-200 means writeHTTPError path was taken — valid either way.
}

func TestWriteHTTPError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"PermissionDenied", status.Error(codes.PermissionDenied, "forbidden"), http.StatusForbidden},
		{"Unauthenticated", status.Error(codes.Unauthenticated, "no creds"), http.StatusUnauthorized},
		{"InvalidArgument", status.Error(codes.InvalidArgument, "bad input"), http.StatusBadRequest},
		{"Unimplemented", status.Error(codes.Unimplemented, "not impl"), http.StatusNotImplemented},
		{"Unavailable", status.Error(codes.Unavailable, "down"), http.StatusServiceUnavailable},
		{"Internal", status.Error(codes.Internal, "oops"), http.StatusInternalServerError},
		{"non-gRPC error", fmt.Errorf("plain error"), http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeHTTPError(rec, tt.err)
			if rec.Code != tt.wantCode {
				t.Errorf("writeHTTPError(%v) status = %d, want %d", tt.err, rec.Code, tt.wantCode)
			}
		})
	}
}

func TestMustJSON(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		data := mustJSON(map[string]string{"key": "val"})
		if string(data) != `{"key":"val"}` {
			t.Errorf("mustJSON = %s, want %s", data, `{"key":"val"}`)
		}
	})

	t.Run("fallback", func(t *testing.T) {
		// Channels cannot be marshaled to JSON.
		data := mustJSON(make(chan int))
		if string(data) != `{"error":"internal error"}` {
			t.Errorf("mustJSON fallback = %s, want %s", data, `{"error":"internal error"}`)
		}
	})
}

func TestResponseToSSEEvent(t *testing.T) {
	t.Run("nil_entry", func(t *testing.T) {
		resp := &configpb.WatchResponse{
			Type: configpb.ChangeType_CHANGE_TYPE_SET,
		}
		evt := responseToSSEEvent(resp)
		if evt.Type != "SET" {
			t.Errorf("Type = %q, want SET", evt.Type)
		}
		if evt.Namespace != "" || evt.Key != "" {
			t.Errorf("expected empty namespace/key for nil entry, got ns=%q key=%q", evt.Namespace, evt.Key)
		}
	})

	t.Run("unknown_type", func(t *testing.T) {
		resp := &configpb.WatchResponse{
			Type: configpb.ChangeType_CHANGE_TYPE_UNSPECIFIED,
		}
		evt := responseToSSEEvent(resp)
		if evt.Type != "UNKNOWN" {
			t.Errorf("Type = %q, want UNKNOWN", evt.Type)
		}
	})
}
