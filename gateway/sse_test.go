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
	"github.com/rbaliyan/config-server/internal/testutil"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

func (sr *syncRecorder) Header() http.Header {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.rec.Header()
}

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
	return testutil.Eventually(timeout, 10*time.Millisecond, func() bool {
		return pred(rec.bodyString())
	})
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
		opts = []service.Option{service.WithSecurityGuard(service.AllowAll())}
	}
	svc, err := service.NewService(store, opts...)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	handler, err := NewInProcessHandler(ctx, svc, WithHeartbeatInterval(30*time.Second))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	return handler, store
}

func TestSSEWatch_InProcess(t *testing.T) {
	t.Parallel()
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

	if _, err := store.Set(context.Background(), "test", "app/timeout", config.NewValue("30")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

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

// sseFrame is one parsed SSE event frame: its id, event type, and the decoded
// data payload.
type sseFrame struct {
	id    string
	event string
	data  sseEvent
}

// parseSSEFrames parses an SSE body into ordered frames. It pairs each
// "id:"/"event:"/"data:" block separated by blank lines. Comment lines
// (": ...") and the "retry:" preamble are skipped.
func parseSSEFrames(t *testing.T, body string) []sseFrame {
	t.Helper()
	var frames []sseFrame
	var cur sseFrame
	haveData := false
	flush := func() {
		if haveData {
			frames = append(frames, cur)
		}
		cur = sseFrame{}
		haveData = false
	}
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "id: "):
			cur.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			cur.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			payload := strings.TrimPrefix(line, "data: ")
			var evt sseEvent
			if err := json.Unmarshal([]byte(payload), &evt); err == nil {
				cur.data = evt
				haveData = true
			}
		}
	}
	flush()
	return frames
}

// TestSSEWatch_LastEventIDReplay_AcrossReconnect verifies that after a client
// disconnects and reconnects carrying Last-Event-ID, the eventbuffer ring
// replays exactly the events the client missed — no gaps, no duplicates —
// before the live stream resumes. Both connections share the handler's single
// event buffer, mirroring a real reconnect against one server.
func TestSSEWatch_LastEventIDReplay_AcrossReconnect(t *testing.T) {
	t.Parallel()
	handler, store := setupSSETest(t)

	// --- First connection: receive a few live events, record the last id. ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1 := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=replay", nil).WithContext(ctx1)
	rec1 := newSyncRecorder()
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		handler.ServeHTTP(rec1, req1)
	}()

	if !waitForBody(t, rec1, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("conn1: timed out waiting for preamble")
	}

	// Produce 5 events on the first connection so all five land in the shared
	// ring buffer. The ring only records events observed by an active stream,
	// so the producing connection must be live while they are emitted.
	for i := 1; i <= 5; i++ {
		if _, err := store.Set(context.Background(), "replay", fmt.Sprintf("k%d", i), config.NewValue(i)); err != nil {
			t.Fatalf("Set k%d: %v", i, err)
		}
	}
	// Wait until conn1 has delivered all 5 SET events.
	if !waitForBody(t, rec1, 3*time.Second, func(s string) bool {
		return strings.Count(s, "event: set") >= 5
	}) {
		t.Fatalf("conn1: timed out waiting for 5 SET events, got:\n%s", rec1.bodyString())
	}

	cancel1()
	<-done1

	frames1 := parseSSEFrames(t, rec1.bodyString())
	var sets1 []sseFrame
	for _, f := range frames1 {
		if f.event == "set" {
			sets1 = append(sets1, f)
		}
	}
	if len(sets1) < 5 {
		t.Fatalf("conn1: expected >=5 SET frames, got %d", len(sets1))
	}
	// Simulate a client that processed through the 3rd event, then dropped the
	// connection (it "missed" k4 and k5 even though they are in the ring).
	lastID := sets1[2].id // resume after the 3rd event
	if lastID == "" {
		t.Fatal("conn1: 3rd SET frame has empty id")
	}

	// --- Reconnect carrying Last-Event-ID; the ring should replay k4, k5. ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	req2 := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=replay", nil).WithContext(ctx2)
	req2.Header.Set("Last-Event-ID", lastID)
	rec2 := newSyncRecorder()
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		handler.ServeHTTP(rec2, req2)
	}()

	// The replayed events are written right after the preamble, so wait until
	// both missed keys appear.
	if !waitForBody(t, rec2, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"key":"k4"`) && strings.Contains(s, `"key":"k5"`)
	}) {
		t.Fatalf("conn2: timed out waiting for replayed events, got:\n%s", rec2.bodyString())
	}

	cancel2()
	<-done2

	// --- Assert: exactly the missed events were replayed, in order, no dups. ---
	frames2 := parseSSEFrames(t, rec2.bodyString())
	var replayedKeys []string
	var replayedIDs []string
	for _, f := range frames2 {
		if f.event != "set" {
			continue
		}
		replayedKeys = append(replayedKeys, f.data.Key)
		replayedIDs = append(replayedIDs, f.id)
	}

	if len(replayedKeys) != 2 {
		t.Fatalf("conn2: expected exactly 2 replayed SET events, got %d: %v", len(replayedKeys), replayedKeys)
	}
	if replayedKeys[0] != "k4" || replayedKeys[1] != "k5" {
		t.Errorf("conn2: replayed keys = %v, want [k4 k5]", replayedKeys)
	}

	// No duplicate ids, and every replayed id must be strictly greater than the
	// resume point (no missed events, no re-delivery of already-seen events).
	seen := map[string]bool{}
	for _, id := range replayedIDs {
		if seen[id] {
			t.Errorf("conn2: duplicate replayed event id %q", id)
		}
		seen[id] = true
		if id <= lastID {
			t.Errorf("conn2: replayed id %q <= resume id %q (event should not be re-delivered)", id, lastID)
		}
	}
}

// TestSSEWatch_LastEventIDReplay_EmptyAndMalformed verifies that a reconnect
// with no Last-Event-ID (or a malformed one) replays nothing — the ring is not
// dumped wholesale, avoiding a race with the live stream.
func TestSSEWatch_LastEventIDReplay_EmptyAndMalformed(t *testing.T) {
	t.Parallel()
	handler, store := setupSSETest(t)

	// Seed events into the ring via a throwaway connection.
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1 := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=norep", nil).WithContext(ctx1)
	rec1 := newSyncRecorder()
	done1 := make(chan struct{})
	go func() { defer close(done1); handler.ServeHTTP(rec1, req1) }()
	if !waitForBody(t, rec1, 2*time.Second, func(s string) bool { return strings.Contains(s, ": connected") }) {
		t.Fatal("seed conn: timed out waiting for preamble")
	}
	if _, err := store.Set(context.Background(), "norep", "seed", config.NewValue("v")); err != nil {
		t.Fatalf("Set seed: %v", err)
	}
	if !waitForBody(t, rec1, 2*time.Second, func(s string) bool { return strings.Contains(s, "event: set") }) {
		t.Fatal("seed conn: timed out waiting for SET")
	}
	cancel1()
	<-done1

	for _, lastID := range []string{"", "not-a-number"} {
		ctx2, cancel2 := context.WithCancel(context.Background())
		req2 := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=norep", nil).WithContext(ctx2)
		if lastID != "" {
			req2.Header.Set("Last-Event-ID", lastID)
		}
		rec2 := newSyncRecorder()
		done2 := make(chan struct{})
		go func() { defer close(done2); handler.ServeHTTP(rec2, req2) }()
		if !waitForBody(t, rec2, 2*time.Second, func(s string) bool { return strings.Contains(s, ": connected") }) {
			t.Fatalf("reconnect (lastID=%q): timed out waiting for preamble", lastID)
		}
		// Replay (if any) is written synchronously between the preamble and the
		// first live event. Emit a fresh live event and wait for it to arrive:
		// once it does, replay is provably complete, so any erroneously dumped
		// "seed" frame would already be present. This is a positive observable
		// signal — no fixed sleep, no weakened assertion.
		liveKey := "live-" + strings.ReplaceAll(lastID, "-", "")
		if liveKey == "live-" {
			liveKey = "live-empty"
		}
		if _, err := store.Set(context.Background(), "norep", liveKey, config.NewValue("v")); err != nil {
			t.Fatalf("Set live event: %v", err)
		}
		if !waitForBody(t, rec2, 2*time.Second, func(s string) bool {
			return strings.Contains(s, `"key":"`+liveKey+`"`)
		}) {
			t.Fatalf("reconnect (lastID=%q): timed out waiting for live event", lastID)
		}
		cancel2()
		<-done2

		// The seeded "seed" key must NOT have been replayed; the only SET frame
		// should be the live event produced after reconnect.
		frames := parseSSEFrames(t, rec2.bodyString())
		for _, f := range frames {
			if f.event != "set" {
				continue
			}
			if f.data.Key != liveKey {
				t.Errorf("lastID=%q: unexpected replayed SET event %q (ring should not be dumped)", lastID, f.data.Key)
			}
		}
	}
}

func TestSSEWatch_DeleteEvent(t *testing.T) {
	t.Parallel()
	handler, store := setupSSETest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-create a key to delete.
	if _, err := store.Set(context.Background(), "test", "old-key", config.NewValue("val")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

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

	if err := store.Delete(context.Background(), "test", "old-key"); err != nil {
		t.Fatalf("failed to delete test data: %v", err)
	}

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
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

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
	t.Parallel()
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

func TestSSEWatch_AuthMiddleware(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	// Wrap handler with AuthMiddleware using DenyAll guard.
	wrapped := AuthMiddleware(service.DenyAll())(handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=secret", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestSSEWatch_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	handler, _ := setupSSETest(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/watch", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /v1/watch status = %d, want 405", rec.Code)
	}
}

func TestSSEWatch_QueryParams(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet,
		"/v1/watch?namespaces=ns1&namespaces=ns2&prefixes=app/&prefixes=db/", nil)

	req, _, err := parseWatchQuery(r)
	if err != nil {
		t.Fatalf("parseWatchQuery failed: %v", err)
	}

	if len(req.Namespaces) != 2 || req.Namespaces[0] != "ns1" || req.Namespaces[1] != "ns2" {
		t.Errorf("namespaces = %v, want [ns1 ns2]", req.Namespaces)
	}
	if len(req.Prefixes) != 2 || req.Prefixes[0] != "app/" || req.Prefixes[1] != "db/" {
		t.Errorf("prefixes = %v, want [app/ db/]", req.Prefixes)
	}
}

func TestSSEWatch_Headers(t *testing.T) {
	t.Parallel()
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
	if conn := result.Header().Get("Connection"); conn != "keep-alive" {
		t.Errorf("Connection = %q, want keep-alive", conn)
	}
}

// nonFlushWriter is an http.ResponseWriter that does not implement http.Flusher.
type nonFlushWriter struct {
	code int
	body strings.Builder
	h    http.Header
}

func (w *nonFlushWriter) Header() http.Header         { return w.h }
func (w *nonFlushWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *nonFlushWriter) WriteHeader(code int)        { w.code = code }

func TestSSEWatch_NoFlusher(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	handler, store := setupSSETest(t)

	// Seed data.
	if _, err := store.Set(context.Background(), "test", "key1", config.NewValue("val1")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	// Existing gRPC-Gateway routes should still work.
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces/test/keys/key1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET key status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestWithHeartbeatInterval(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

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

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	configpb.RegisterConfigServiceServer(grpcServer, svc)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	return lis
}

func TestSSEWatch_Remote(t *testing.T) {
	t.Parallel()
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
	if !waitForBody(t, rec, 15*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for remote SSE preamble")
	}

	if _, err := store.Set(context.Background(), "test", "remote-key", config.NewValue("remote-val")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	if !waitForBody(t, rec, 15*time.Second, func(s string) bool {
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
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	// Use DenyAll guard via AuthInterceptor on the gRPC server.
	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(
		grpc.ChainStreamInterceptor(service.StreamAuthInterceptor(service.DenyAll())),
	)
	configpb.RegisterConfigServiceServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
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
	if !strings.Contains(body, "no security guard configured") {
		t.Errorf("expected auth denial message in body: %s", body)
	}
}

func TestSSEWatch_Remote_ClosedConn(t *testing.T) {
	t.Parallel()
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
	sseHandler := newRemoteSSEHandler(client, 30*time.Second, newEventBuffer(defaultEventBufferSize))
	handler := composeHandlers(http.NewServeMux(), sseHandler, http.NotFoundHandler(), nil, "")

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
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantBody string // substring expected in body
	}{
		{"PermissionDenied", status.Error(codes.PermissionDenied, "forbidden"), http.StatusForbidden, "forbidden"},
		{"Unauthenticated", status.Error(codes.Unauthenticated, "no creds"), http.StatusUnauthorized, "no creds"},
		{"InvalidArgument", status.Error(codes.InvalidArgument, "bad input"), http.StatusBadRequest, "bad input"},
		{"NotFound", status.Error(codes.NotFound, "missing"), http.StatusNotFound, "missing"},
		{"AlreadyExists", status.Error(codes.AlreadyExists, "dup"), http.StatusConflict, "dup"},
		{"Unimplemented", status.Error(codes.Unimplemented, "not impl"), http.StatusNotImplemented, "not impl"},
		{"Unavailable", status.Error(codes.Unavailable, "connection refused"), http.StatusInternalServerError, "internal error"},
		{"Internal", status.Error(codes.Internal, "db host=10.0.0.1"), http.StatusInternalServerError, "internal error"},
		{"non-gRPC error", fmt.Errorf("raw error details"), http.StatusInternalServerError, "internal error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			writeHTTPError(rec, tt.err)
			if rec.Code != tt.wantCode {
				t.Errorf("writeHTTPError(%v) status = %d, want %d", tt.err, rec.Code, tt.wantCode)
			}
			body := rec.Body.String()
			if !strings.Contains(body, tt.wantBody) {
				t.Errorf("writeHTTPError(%v) body = %q, want substring %q", tt.err, body, tt.wantBody)
			}
		})
	}
}

func TestMustJSON(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestSSEWatchStream_NoOpStubs(t *testing.T) {
	t.Parallel()
	stream := &sseWatchStream{}
	if err := stream.SetHeader(nil); err != nil {
		t.Errorf("SetHeader returned error: %v", err)
	}
	if err := stream.SendHeader(nil); err != nil {
		t.Errorf("SendHeader returned error: %v", err)
	}
	stream.SetTrailer(nil) // no return value
	if err := stream.SendMsg(nil); err != nil {
		t.Errorf("SendMsg returned error: %v", err)
	}
	if err := stream.RecvMsg(nil); err != nil {
		t.Errorf("RecvMsg returned error: %v", err)
	}
}

func TestSSEWatch_ConcurrentConnections(t *testing.T) {
	t.Parallel()
	handler, store := setupSSETest(t)

	const numClients = 5

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start multiple concurrent SSE watchers.
	var wg sync.WaitGroup
	recorders := make([]*syncRecorder, numClients)
	for i := range numClients {
		recorders[i] = newSyncRecorder()
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
			req = req.WithContext(ctx)
			handler.ServeHTTP(recorders[idx], req)
		}(i)
	}

	// Wait for all clients to connect.
	for i := range numClients {
		if !waitForBody(t, recorders[i], 2*time.Second, func(s string) bool {
			return strings.Contains(s, ": connected")
		}) {
			t.Fatalf("client %d: timed out waiting for preamble", i)
		}
	}

	// Trigger a change — all clients should receive it.
	if _, err := store.Set(context.Background(), "test", "concurrent-key", config.NewValue("val")); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	for i := range numClients {
		if !waitForBody(t, recorders[i], 2*time.Second, func(s string) bool {
			return strings.Contains(s, "event: set")
		}) {
			t.Fatalf("client %d: timed out waiting for SET event", i)
		}
	}

	cancel()

	// Wait for all handler goroutines to exit.
	wgDone := make(chan struct{})
	go func() { wg.Wait(); close(wgDone) }()
	select {
	case <-wgDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handlers did not exit after cancel")
	}

	// Verify all clients got the event.
	for i := range numClients {
		body := recorders[i].bodyString()
		evt, ok := findSSEEvent(t, body, "SET")
		if !ok {
			t.Errorf("client %d: expected SET event", i)
			continue
		}
		if evt.Key != "concurrent-key" {
			t.Errorf("client %d: key = %q, want concurrent-key", i, evt.Key)
		}
	}
}

func TestSSEWatch_MetadataPropagation(t *testing.T) {
	t.Parallel()
	// Verify that HTTP headers are propagated as gRPC metadata
	// in the in-process handler.
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	handler, err := NewInProcessHandler(ctx, svc, WithHeartbeatInterval(30*time.Second))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=test", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("X-Custom-Header", "custom-value")
	req = req.WithContext(reqCtx)
	rec := newSyncRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(rec, req)
	}()

	// If metadata propagation works, the handler should start normally
	// (AllowAll guard accepts everything).
	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble with auth headers")
	}

	cancel()
	<-done
}

func TestHttpHeadersToMetadata(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/v1/watch", nil)
	r.Header.Set("Authorization", "Bearer token123")
	r.Header.Set("X-Role", "admin")
	r.Header.Set("X-Request-Id", "req-42")
	r.Header.Set("X-Forwarded-For", "1.2.3.4")       // proxy header, should be excluded
	r.Header.Set("Connection", "keep-alive")         // hop-by-hop, should be excluded
	r.Header.Set("Accept-Encoding", "gzip")          // transport, should be excluded
	r.Header.Set("Content-Type", "application/json") // should be excluded
	r.Header.Set("Cookie", "session=abc")            // should be excluded

	ctx := httpHeadersToMetadata(context.Background(), r)

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		t.Fatal("expected incoming metadata on context")
	}

	// Allowlisted headers should be forwarded.
	if got := md.Get("authorization"); len(got) == 0 || got[0] != "Bearer token123" {
		t.Errorf("authorization = %v, want [Bearer token123]", got)
	}
	if got := md.Get("x-role"); len(got) == 0 || got[0] != "admin" {
		t.Errorf("x-role = %v, want [admin]", got)
	}
	if got := md.Get("x-request-id"); len(got) == 0 || got[0] != "req-42" {
		t.Errorf("x-request-id = %v, want [req-42]", got)
	}

	// Proxy, hop-by-hop, and transport headers should be excluded.
	for _, excluded := range []string{"x-forwarded-for", "connection", "accept-encoding", "content-type", "cookie"} {
		if got := md.Get(excluded); len(got) != 0 {
			t.Errorf("%s should be excluded, got %v", excluded, got)
		}
	}
}

func TestIsForwardableHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		header string
		want   bool
	}{
		{"authorization", true},
		{"x-request-id", true},
		{"x-correlation-id", true},
		{"x-custom-anything", true},
		{"x-role", true},
		// Excluded: proxy headers that clients could spoof.
		{"x-forwarded-for", false},
		{"x-forwarded-host", false},
		{"x-forwarded-proto", false},
		{"x-forwarded-port", false},
		{"x-real-ip", false},
		// Excluded: hop-by-hop and transport headers.
		{"cookie", false},
		{"connection", false},
		{"transfer-encoding", false},
		{"accept-encoding", false},
		{"content-type", false},
		{"host", false},
		{"user-agent", false},
	}
	for _, tt := range tests {
		if got := isForwardableHeader(tt.header); got != tt.want {
			t.Errorf("isForwardableHeader(%q) = %v, want %v", tt.header, got, tt.want)
		}
	}
}

func TestSanitizeSSEField(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input, want string
	}{
		{"set", "set"},
		{"heartbeat", "heartbeat"},
		{"has\nnewline", "hasnewline"},
		{"has\r\ncrlf", "hascrlf"},
		{"has\rcarriage", "hascarriage"},
		{"clean", "clean"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := sanitizeSSEField(tt.input); got != tt.want {
			t.Errorf("sanitizeSSEField(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestWriteSSEError_Sanitization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{
			name:    "client-actionable PermissionDenied",
			err:     status.Error(codes.PermissionDenied, "access denied"),
			wantMsg: "access denied",
		},
		{
			name:    "client-actionable InvalidArgument",
			err:     status.Error(codes.InvalidArgument, "bad key format"),
			wantMsg: "bad key format",
		},
		{
			name:    "internal error sanitized",
			err:     status.Error(codes.Internal, "db connection failed: host=10.0.0.1"),
			wantMsg: "internal error",
		},
		{
			name:    "unavailable error sanitized",
			err:     status.Error(codes.Unavailable, "connection refused"),
			wantMsg: "internal error",
		},
		{
			name:    "non-gRPC error sanitized",
			err:     fmt.Errorf("raw error with details"),
			wantMsg: "internal error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newSyncRecorder()
			sw := &sseWriter{w: rec, flusher: rec}
			writeSSEError(sw, tt.err)

			body := rec.bodyString()
			if !strings.Contains(body, tt.wantMsg) {
				t.Errorf("expected %q in body, got:\n%s", tt.wantMsg, body)
			}
			if tt.wantMsg == "internal error" && strings.Contains(body, "connection") {
				t.Errorf("internal details leaked in body:\n%s", body)
			}
		})
	}
}

func TestSSEWatch_SpecialCharacterValue(t *testing.T) {
	t.Parallel()
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

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, ": connected")
	}) {
		t.Fatal("timed out waiting for SSE preamble")
	}

	// Set a value with characters that could be problematic in SSE frames:
	// newlines, quotes, backslashes, unicode, and angle brackets.
	specialValue := "line1\nline2\r\nline3\ttab \"quotes\" <html> & émojis 🎉"
	if _, err := store.Set(context.Background(), "test", "special-key", config.NewValue(specialValue)); err != nil {
		t.Fatalf("failed to set test data: %v", err)
	}

	if !waitForBody(t, rec, 2*time.Second, func(s string) bool {
		return strings.Contains(s, "event: set")
	}) {
		t.Fatal("timed out waiting for SET event")
	}

	cancel()
	<-done

	body := rec.bodyString()

	// Verify the event was received and the SSE frame is well-formed:
	// each "data:" line should be a single line (no raw newlines splitting it).
	evt, ok := findSSEEvent(t, body, "SET")
	if !ok {
		t.Fatal("expected SET event for special character value")
	}
	if evt.Key != "special-key" {
		t.Errorf("key = %q, want special-key", evt.Key)
	}
	// Value is base64-encoded by json.Marshal, so it should be safe.
	if len(evt.Value) == 0 {
		t.Error("expected non-empty value")
	}

	// Verify no raw newlines appear inside a data: line (which would break SSE framing).
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			if strings.ContainsAny(payload, "\r") {
				t.Errorf("data line contains carriage return: %q", payload)
			}
		}
	}
}

func TestParseWatchQuery_Limits(t *testing.T) {
	t.Parallel()
	t.Run("too_many_namespaces", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("/v1/watch?")
		for i := range maxWatchParams + 1 {
			if i > 0 {
				b.WriteByte('&')
			}
			fmt.Fprintf(&b, "namespaces=ns%d", i)
		}
		r := httptest.NewRequest(http.MethodGet, b.String(), nil)
		_, _, err := parseWatchQuery(r)
		if err == nil {
			t.Fatal("expected error for too many namespaces")
		}
		if !strings.Contains(err.Error(), "too many namespaces") {
			t.Errorf("error = %v, want 'too many namespaces'", err)
		}
	})

	t.Run("too_many_prefixes", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("/v1/watch?")
		for i := range maxWatchParams + 1 {
			if i > 0 {
				b.WriteByte('&')
			}
			fmt.Fprintf(&b, "prefixes=p%d", i)
		}
		r := httptest.NewRequest(http.MethodGet, b.String(), nil)
		_, _, err := parseWatchQuery(r)
		if err == nil {
			t.Fatal("expected error for too many prefixes")
		}
	})

	t.Run("namespace_too_long", func(t *testing.T) {
		longNS := strings.Repeat("a", maxParamValueLen+1)
		r := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces="+longNS, nil)
		_, _, err := parseWatchQuery(r)
		if err == nil {
			t.Fatal("expected error for namespace too long")
		}
		if !strings.Contains(err.Error(), "too long") {
			t.Errorf("error = %v, want 'too long'", err)
		}
	})

	t.Run("prefix_too_long", func(t *testing.T) {
		longPF := strings.Repeat("b", maxParamValueLen+1)
		r := httptest.NewRequest(http.MethodGet, "/v1/watch?prefixes="+longPF, nil)
		_, _, err := parseWatchQuery(r)
		if err == nil {
			t.Fatal("expected error for prefix too long")
		}
	})

	t.Run("valid_within_limits", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/watch?namespaces=ns1&prefixes=p1", nil)
		req, _, err := parseWatchQuery(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(req.Namespaces) != 1 || req.Namespaces[0] != "ns1" {
			t.Errorf("namespaces = %v, want [ns1]", req.Namespaces)
		}
	})

	t.Run("empty_params_valid", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/watch", nil)
		req, _, err := parseWatchQuery(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(req.Namespaces) != 0 || len(req.Prefixes) != 0 {
			t.Errorf("expected empty slices, got ns=%v pf=%v", req.Namespaces, req.Prefixes)
		}
	})
}

func TestSSEWatch_QueryParamValidation_HTTP(t *testing.T) {
	t.Parallel()
	handler, _ := setupSSETest(t)

	// Build a URL with too many namespaces.
	var b strings.Builder
	b.WriteString("/v1/watch?")
	for i := range maxWatchParams + 1 {
		if i > 0 {
			b.WriteByte('&')
		}
		fmt.Fprintf(&b, "namespaces=ns%d", i)
	}

	req := httptest.NewRequest(http.MethodGet, b.String(), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "too many namespaces") {
		t.Errorf("body = %q, want 'too many namespaces'", rec.Body.String())
	}
}

func TestSSEWatch_CleanDisconnect_NoErrorEvent(t *testing.T) {
	t.Parallel()
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

	// Client disconnects normally.
	cancel()
	<-done

	body := rec.bodyString()

	// A clean client disconnect should NOT produce an error event.
	if strings.Contains(body, "event: error") {
		t.Errorf("clean disconnect should not produce error event, got:\n%s", body)
	}
}
