// Package gateway provides an HTTP/JSON handler for the ConfigService,
// built on grpc-gateway and layered with custom handlers for features
// that do not fit the standard gRPC-Gateway mapping.
//
// Surface area:
//   - Auto-generated REST routes (Get/Set/Delete/List/GetVersions/CheckAccess)
//     mapped from the proto HTTP annotations.
//   - Server-Sent Events (SSE) endpoint at /v1/watch for browser-friendly
//     Watch with automatic reconnection. Events carry monotonically
//     increasing id: lines, and the handler maintains an in-memory ring
//     buffer (see WithEventBufferSize) so clients reconnecting with the
//     Last-Event-ID HTTP header receive missed events before the live
//     stream resumes.
//   - Version diff endpoint at /v1/namespaces/{namespace}/keys/{key}/diff
//     that returns both versions' raw bytes and codecs plus a changed flag.
//   - Optional embedded dashboard web UI at /dashboard/ via WithDashboard
//     (mount path configurable via WithDashboardPath).
//
// Two constructors are provided:
//   - NewHandler dials a remote gRPC backend.
//   - NewInProcessHandler calls the service directly without a network hop.
//
// Both accept the same Option set; the event ring buffer and dashboard are
// per-handler and per-process.
package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc"
)

// Handler is an HTTP handler for the ConfigService gateway.
// For handlers created by NewHandler, Close releases the underlying gRPC
// connection. For handlers created by NewInProcessHandler, Close is a no-op.
type Handler struct {
	http.Handler
	closeOnce sync.Once
	closeErr  error
	conn      *grpc.ClientConn // nil for in-process handlers
	done      chan struct{}
}

// Close releases resources held by the handler.
// For remote handlers, this closes the gRPC connection to the backend.
// Close is safe to call multiple times; all calls return the same error.
func (h *Handler) Close() error {
	h.closeOnce.Do(func() {
		close(h.done)
		if h.conn != nil {
			h.closeErr = h.conn.Close()
		}
	})
	return h.closeErr
}

// NewHandler creates a Handler that proxies to the gRPC service.
// The handler can be mounted on any HTTP router.
//
// The caller should call Close when the handler is no longer needed to release
// the gRPC connection. Alternatively, cancelling ctx will also close the
// connection, but calling Close is preferred for deterministic cleanup.
//
// By default, connections are insecure. Use WithTLS for production.
//
// Example:
//
//	handler, _ := gateway.NewHandler(ctx, "localhost:9090", gateway.WithInsecure())
//	defer handler.Close()
//	http.Handle("/api/", http.StripPrefix("/api", handler))
func NewHandler(ctx context.Context, grpcAddr string, opts ...Option) (*Handler, error) {
	if grpcAddr == "" {
		return nil, errors.New("gateway: gRPC address must not be empty")
	}

	o := newResolvedOptions(opts)

	conn, err := grpc.NewClient(grpcAddr, o.buildDialOpts()...)
	if err != nil {
		return nil, err
	}

	mux := runtime.NewServeMux(o.muxOpts...)
	if err := configpb.RegisterConfigServiceHandler(ctx, mux, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	buf := newEventBuffer(resolveEventBufferSize(o.eventBufferSize))
	client := configpb.NewConfigServiceClient(conn)

	h := &Handler{
		Handler: composeHandlers(
			mux,
			newRemoteSSEHandler(client, o.heartbeatInterval, buf),
			newRemoteDiffHandler(client),
			o.dashHandler(),
			o.dashboardPath,
		),
		conn: conn,
		done: make(chan struct{}),
	}

	// Clean up when either the parent context is cancelled or Close is called.
	// This goroutine is guaranteed to exit because Close closes h.done.
	go func() {
		select {
		case <-ctx.Done():
		case <-h.done:
		}
		_ = h.Close()
	}()

	return h, nil
}

// NewInProcessHandler creates a handler that calls the service directly
// without going through a network connection. This is more efficient
// when the gRPC service and HTTP gateway run in the same process.
//
// Example:
//
//	svc := service.NewService(store, service.WithSecurityGuard(guard))
//	handler, _ := gateway.NewInProcessHandler(ctx, svc)
//	http.Handle("/api/", http.StripPrefix("/api", handler))
func NewInProcessHandler(ctx context.Context, svc configpb.ConfigServiceServer, opts ...Option) (*Handler, error) {
	o := newResolvedOptions(opts)

	mux := runtime.NewServeMux(o.muxOpts...)
	if err := configpb.RegisterConfigServiceHandlerServer(ctx, mux, svc); err != nil {
		return nil, err
	}

	buf := newEventBuffer(resolveEventBufferSize(o.eventBufferSize))

	return &Handler{
		Handler: composeHandlers(
			mux,
			newInProcessSSEHandler(svc, o.heartbeatInterval, buf),
			newInProcessDiffHandler(svc),
			o.dashHandler(),
			o.dashboardPath,
		),
		done: make(chan struct{}),
	}, nil
}
