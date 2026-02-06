package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sseEvent is the JSON payload written for each SSE data line.
type sseEvent struct {
	Type      string     `json:"type"`
	Namespace string     `json:"namespace"`
	Key       string     `json:"key"`
	Value     []byte     `json:"value,omitempty"`
	Codec     string     `json:"codec,omitempty"`
	Version   int64      `json:"version,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

// sseWriter serializes writes to an http.ResponseWriter to prevent
// concurrent writes from the heartbeat goroutine and event goroutine.
type sseWriter struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	flusher http.Flusher
}

// writeEvent writes an SSE event frame. Both eventType and data must be
// single-line: newlines are stripped defensively to prevent frame injection.
func (sw *sseWriter) writeEvent(eventType string, data []byte) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	eventType = sanitizeSSEField(eventType)
	if _, err := fmt.Fprintf(sw.w, "event: %s\ndata: %s\n\n", eventType, data); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

func (sw *sseWriter) writeComment(comment string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	comment = sanitizeSSEField(comment)
	if _, err := fmt.Fprintf(sw.w, ": %s\n\n", comment); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// writeRaw writes s directly without sanitization. Only use with trusted,
// static content (e.g., the stream preamble).
func (sw *sseWriter) writeRaw(s string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if _, err := fmt.Fprint(sw.w, s); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// sseWatchStream implements grpc.ServerStreamingServer[configpb.WatchResponse]
// by writing SSE events to an http.ResponseWriter.
type sseWatchStream struct {
	grpc.ServerStream
	ctx context.Context
	sw  *sseWriter
}

func (s *sseWatchStream) Context() context.Context {
	return s.ctx
}

func (s *sseWatchStream) Send(resp *configpb.WatchResponse) error {
	return sendSSEResponse(s.sw, resp)
}

// sendSSEResponse converts a WatchResponse to an SSE event and writes it.
// Used by both the in-process stream adapter and the remote relay loop.
func sendSSEResponse(sw *sseWriter, resp *configpb.WatchResponse) error {
	evt := responseToSSEEvent(resp)

	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal SSE event: %w", err)
	}

	return sw.writeEvent(strings.ToLower(evt.Type), data)
}

// No-op stubs to satisfy grpc.ServerStream. The SSE adapter only uses
// Context() and Send(); metadata and raw message methods are unused.
func (s *sseWatchStream) SetHeader(metadata.MD) error  { return nil }
func (s *sseWatchStream) SendHeader(metadata.MD) error  { return nil }
func (s *sseWatchStream) SetTrailer(metadata.MD)        {}
func (s *sseWatchStream) SendMsg(any) error              { return nil }
func (s *sseWatchStream) RecvMsg(any) error              { return nil }

// responseToSSEEvent converts a WatchResponse proto to an sseEvent.
func responseToSSEEvent(resp *configpb.WatchResponse) sseEvent {
	evt := sseEvent{}

	switch resp.GetType() {
	case configpb.ChangeType_CHANGE_TYPE_SET:
		evt.Type = "SET"
	case configpb.ChangeType_CHANGE_TYPE_DELETE:
		evt.Type = "DELETE"
	default:
		evt.Type = "UNKNOWN"
	}

	if entry := resp.GetEntry(); entry != nil {
		evt.Namespace = entry.GetNamespace()
		evt.Key = entry.GetKey()
		evt.Value = entry.GetValue()
		evt.Codec = entry.GetCodec()
		evt.Version = entry.GetVersion()
		if ts := entry.GetCreatedAt(); ts != nil {
			t := ts.AsTime()
			evt.CreatedAt = &t
		}
		if ts := entry.GetUpdatedAt(); ts != nil {
			t := ts.AsTime()
			evt.UpdatedAt = &t
		}
	}

	return evt
}

// writeSSEHeaders sets the standard SSE response headers and flushes them.
func writeSSEHeaders(w http.ResponseWriter, flusher http.Flusher) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")       // Hint for HTTP/1.1 proxies.
	w.Header().Set("X-Accel-Buffering", "no")         // Prevent nginx proxy buffering.
	flusher.Flush()
}

// writeStreamPreamble sends the initial SSE stream preamble: a retry hint
// for client reconnection and a connection confirmation comment.
func writeStreamPreamble(sw *sseWriter) error {
	return sw.writeRaw("retry: 5000\n\n: connected\n\n")
}

// newInProcessSSEHandler creates an HTTP handler that calls svc.Watch directly
// and streams results as SSE events.
//
// Note: SSE headers are committed before calling svc.Watch because Watch is a
// blocking call that requires a pre-constructed stream. If authorization fails,
// the client receives a 200 with an SSE error event rather than a 403 status.
// This is an inherent tradeoff of the in-process adapter pattern. The remote
// handler (newRemoteSSEHandler) does not have this limitation because gRPC
// stream creation is non-blocking.
//
// In practice, however, gRPC server-streaming RPCs return a stream immediately
// from client.Watch(); server-side errors (including auth denial) only surface
// on the first Recv(). So the remote handler also commits SSE headers before
// it can detect authorization failures — both paths share this tradeoff.
func newInProcessSSEHandler(svc configpb.ConfigServiceServer, heartbeat time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		req := parseWatchQuery(r)

		// Set SSE headers before calling Watch (which blocks).
		writeSSEHeaders(w, flusher)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Forward HTTP headers as gRPC incoming metadata so interceptor-based
		// auth patterns (e.g., Authorization, x-role) work in the in-process path.
		ctx = httpHeadersToMetadata(ctx, r)

		sw := &sseWriter{w: w, flusher: flusher}
		if err := writeStreamPreamble(sw); err != nil {
			return
		}

		stream := &sseWatchStream{
			ctx: ctx,
			sw:  sw,
		}

		// Start heartbeat goroutine and ensure it exits before the
		// handler returns, preventing writes to a recycled ResponseWriter.
		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			runHeartbeat(ctx, sw, heartbeat)
		}()

		// svc.Watch blocks until the context is cancelled or an error occurs.
		err := svc.Watch(req, stream)
		if err != nil && ctx.Err() == nil {
			// Send an SSE error event only for real errors, not context
			// cancellation (which means the client disconnected normally).
			writeSSEError(sw, err)
		}

		cancel()
		<-hbDone
	})
}

// newRemoteSSEHandler creates an HTTP handler that calls client.Watch
// over a gRPC connection and relays responses as SSE events.
func newRemoteSSEHandler(client configpb.ConfigServiceClient, heartbeat time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		req := parseWatchQuery(r)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		stream, err := client.Watch(ctx, req)
		if err != nil {
			writeHTTPError(w, err)
			return
		}

		// Set SSE headers after successful stream creation.
		writeSSEHeaders(w, flusher)

		sw := &sseWriter{w: w, flusher: flusher}
		if err := writeStreamPreamble(sw); err != nil {
			return
		}

		// Start heartbeat goroutine and ensure it exits before the
		// handler returns, preventing writes to a recycled ResponseWriter.
		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			runHeartbeat(ctx, sw, heartbeat)
		}()

		for {
			resp, err := stream.Recv()
			if err != nil {
				// io.EOF means normal stream termination — disconnect
				// cleanly without sending a misleading error event.
				if err != io.EOF {
					writeSSEError(sw, err)
				}
				break
			}

			if err := sendSSEResponse(sw, resp); err != nil {
				break
			}
		}

		cancel()
		<-hbDone
	})
}

// parseWatchQuery extracts namespaces and prefixes from query parameters.
func parseWatchQuery(r *http.Request) *configpb.WatchRequest {
	q := r.URL.Query()
	return &configpb.WatchRequest{
		Namespaces: q["namespaces"],
		Prefixes:   q["prefixes"],
	}
}

// httpHeadersToMetadata converts selected HTTP request headers into gRPC
// incoming metadata on the context. Only headers relevant to authentication
// and request tracing are forwarded; hop-by-hop and transport headers
// (Connection, Transfer-Encoding, etc.) are excluded.
func httpHeadersToMetadata(ctx context.Context, r *http.Request) context.Context {
	md := make(metadata.MD)
	for key, values := range r.Header {
		lower := strings.ToLower(key)
		if isForwardableHeader(lower) {
			md[lower] = values
		}
	}
	if len(md) == 0 {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, md)
}

// isForwardableHeader returns true for headers that should be propagated
// as gRPC metadata. This allowlists auth, tracing, and custom headers
// while excluding hop-by-hop, transport, and proxy headers.
func isForwardableHeader(lower string) bool {
	switch lower {
	case "authorization":
		return true
	}
	// Forward x-* custom headers used for auth/context, but exclude
	// well-known proxy headers that should not be trusted from clients.
	if strings.HasPrefix(lower, "x-") {
		switch lower {
		case "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto",
			"x-forwarded-port", "x-real-ip":
			return false
		}
		return true
	}
	return false
}

// runHeartbeat sends SSE comment lines at the given interval to keep
// the connection alive through proxies and load balancers.
func runHeartbeat(ctx context.Context, sw *sseWriter, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sw.writeComment("heartbeat"); err != nil {
				return
			}
		}
	}
}

// writeSSEError writes an SSE error event. For client-actionable errors
// (auth, validation), the gRPC message is forwarded. For internal errors,
// a generic message is sent to avoid leaking implementation details.
// The write error is intentionally discarded because if the client has
// disconnected, there is nothing more to do.
func writeSSEError(sw *sseWriter, err error) {
	msg := "internal error"
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.PermissionDenied, codes.Unauthenticated,
			codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
			codes.FailedPrecondition, codes.Unimplemented:
			msg = st.Message()
		}
	}
	_ = sw.writeEvent("error", mustJSON(map[string]string{"error": msg}))
}

// writeHTTPError maps a gRPC error to an HTTP status code before SSE headers are sent.
func writeHTTPError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var httpCode int
	switch st.Code() {
	case codes.PermissionDenied:
		httpCode = http.StatusForbidden
	case codes.Unauthenticated:
		httpCode = http.StatusUnauthorized
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.AlreadyExists:
		httpCode = http.StatusConflict
	case codes.Unimplemented:
		httpCode = http.StatusNotImplemented
	case codes.Unavailable:
		httpCode = http.StatusServiceUnavailable
	default:
		httpCode = http.StatusInternalServerError
	}

	http.Error(w, st.Message(), httpCode)
}

// mustJSON marshals v to JSON, returning a fallback error object on failure.
func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"error":"internal error"}`)
	}
	return data
}

// sanitizeSSEField strips carriage returns and newlines from s to prevent
// SSE frame injection. All current callers pass safe literals or enum-derived
// strings, but this provides defense-in-depth.
func sanitizeSSEField(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	r := strings.NewReplacer("\r\n", "", "\r", "", "\n", "")
	return r.Replace(s)
}

// composeHandlers creates a single http.Handler that routes SSE watch
// requests to sseHandler and everything else to gwMux.
func composeHandlers(gwMux http.Handler, sseHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/watch", sseHandler)
	// Return 405 for non-GET methods on /v1/watch instead of falling
	// through to the gRPC-Gateway handler (which would return 404).
	mux.HandleFunc("/v1/watch", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
	mux.Handle("/", gwMux)
	return mux
}
