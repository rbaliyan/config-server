// Package gateway provides an HTTP handler for the ConfigService using gRPC-Gateway.
package gateway

import (
	"context"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

// NewHandler creates an http.Handler that proxies to the gRPC service.
// The handler can be mounted on any HTTP router.
//
// By default, connections are insecure. Use WithTLS for production.
//
// Example with standard library:
//
//	handler, _ := gateway.NewHandler(ctx, "localhost:9090", gateway.WithInsecure())
//	http.Handle("/api/", http.StripPrefix("/api", handler))
//
// Example with TLS:
//
//	handler, _ := gateway.NewHandler(ctx, "config-server:9090", gateway.WithTLS(nil))
//	http.Handle("/api/", http.StripPrefix("/api", handler))
func NewHandler(ctx context.Context, grpcAddr string, opts ...Option) (http.Handler, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := o.buildDialOpts()
	mux := runtime.NewServeMux(o.muxOpts...)

	err := configpb.RegisterConfigServiceHandlerFromEndpoint(
		ctx, mux, grpcAddr, dialOpts,
	)
	if err != nil {
		return nil, err
	}

	return mux, nil
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
func NewInProcessHandler(ctx context.Context, svc configpb.ConfigServiceServer, opts ...Option) (http.Handler, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	mux := runtime.NewServeMux(o.muxOpts...)

	err := configpb.RegisterConfigServiceHandlerServer(ctx, mux, svc)
	if err != nil {
		return nil, err
	}

	return mux, nil
}
