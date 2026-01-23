// Example showing how to embed the config service into an existing gRPC server.
package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create store
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		log.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	// Seed data
	store.Set(ctx, "production", "api/key", config.NewValue("secret-key"))
	store.Set(ctx, "staging", "api/key", config.NewValue("staging-key"))

	// Create config service with custom authorizer
	configSvc := service.NewService(store,
		service.WithAuthorizer(&namespaceAuthorizer{
			allowed: map[string][]string{
				"admin":    {"production", "staging"},
				"readonly": {"staging"},
			},
		}),
	)

	// Create gRPC server with auth interceptor
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			authInterceptor,
			service.LoggingInterceptor(logger),
		),
	)

	// Register config service alongside your other services
	configpb.RegisterConfigServiceServer(grpcServer, configSvc)
	// myv1.RegisterMyServiceServer(grpcServer, mySvc) // Your other services

	// Start server
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		logger.Info("starting gRPC server", "addr", ":9090")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
	grpcServer.GracefulStop()
}

// authInterceptor extracts the role from metadata and adds it to context.
func authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	roles := md.Get("x-role")
	if len(roles) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing role")
	}

	// Add role to context for authorizer
	ctx = context.WithValue(ctx, roleKey{}, roles[0])
	return handler(ctx, req)
}

type roleKey struct{}

// namespaceAuthorizer implements service.Authorizer with namespace-based access control.
type namespaceAuthorizer struct {
	allowed map[string][]string // role -> allowed namespaces
}

func (a *namespaceAuthorizer) Authorize(ctx context.Context, req service.AuthRequest) error {
	role, ok := ctx.Value(roleKey{}).(string)
	if !ok {
		return status.Error(codes.PermissionDenied, "no role in context")
	}

	namespaces, ok := a.allowed[role]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "unknown role: %s", role)
	}

	// Skip namespace check for operations that don't have one
	if req.Namespace == "" {
		return nil
	}

	for _, ns := range namespaces {
		if ns == req.Namespace {
			return nil
		}
	}

	return status.Errorf(codes.PermissionDenied,
		"role %s cannot access namespace %s", role, req.Namespace)
}
