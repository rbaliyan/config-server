// Package gateway provides an HTTP handler for the ConfigService using gRPC-Gateway.
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
	conn      *grpc.ClientConn // nil for in-process handlers
	done      chan struct{}
}

// Close releases resources held by the handler.
// For remote handlers, this closes the gRPC connection to the backend.
// Close is safe to call multiple times.
func (h *Handler) Close() error {
	var err error
	h.closeOnce.Do(func() {
		close(h.done)
		if h.conn != nil {
			err = h.conn.Close()
		}
	})
	return err
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

	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := o.buildDialOpts()

	conn, err := grpc.NewClient(grpcAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	mux := runtime.NewServeMux(o.muxOpts...)

	if err := configpb.RegisterConfigServiceHandler(ctx, mux, conn); err != nil {
		conn.Close()
		return nil, err
	}

	heartbeat := o.heartbeatInterval
	if heartbeat <= 0 {
		heartbeat = defaultHeartbeatInterval
	}

	client := configpb.NewConfigServiceClient(conn)
	sseHandler := newRemoteSSEHandler(client, heartbeat)

	h := &Handler{
		Handler: composeHandlers(mux, sseHandler),
		conn:    conn,
		done:    make(chan struct{}),
	}

	// Clean up when either the parent context is cancelled or Close is called.
	// This goroutine is guaranteed to exit because Close closes h.done.
	go func() {
		select {
		case <-ctx.Done():
		case <-h.done:
		}
		h.Close()
	}()

	return h, nil
}

// NewInProcessHandler creates a handler that calls the service directly
// without going through a network connection. This is more efficient
// when the gRPC service and HTTP gateway run in the same process.
//
// Example:
//
//	svc := service.NewService(store, service.WithAuthorizer(auth))
//	handler, _ := gateway.NewInProcessHandler(ctx, svc)
//	http.Handle("/api/", http.StripPrefix("/api", handler))
func NewInProcessHandler(ctx context.Context, svc configpb.ConfigServiceServer, opts ...Option) (*Handler, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	mux := runtime.NewServeMux(o.muxOpts...)

	err := configpb.RegisterConfigServiceHandlerServer(ctx, mux, svc)
	if err != nil {
		return nil, err
	}

	heartbeat := o.heartbeatInterval
	if heartbeat <= 0 {
		heartbeat = defaultHeartbeatInterval
	}

	sseHandler := newInProcessSSEHandler(svc, heartbeat)

	return &Handler{
		Handler: composeHandlers(mux, sseHandler),
		done:    make(chan struct{}),
	}, nil
}
