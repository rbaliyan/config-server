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
	defer func() { _ = store.Close(ctx) }()

	// Seed data
	_, _ = store.Set(ctx, "production", "api/key", config.NewValue("secret-key"))
	_, _ = store.Set(ctx, "staging", "api/key", config.NewValue("staging-key"))

	// Create security guard with namespace-based access control
	guard := &namespaceGuard{
		allowed: map[string][]string{
			"admin":    {"production", "staging"},
			"readonly": {"staging"},
		},
	}

	// Create config service
	configSvc, err := service.NewService(store,
		service.WithSecurityGuard(guard),
	)
	if err != nil {
		log.Fatal("failed to create config service:", err)
	}

	// Create gRPC server with built-in auth interceptors.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			service.AuthInterceptor(guard),
			service.LoggingInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			service.StreamAuthInterceptor(guard),
			service.StreamLoggingInterceptor(logger),
		),
	)

	// Register config service alongside your other services
	configpb.RegisterConfigServiceServer(grpcServer, configSvc)
	// myv1.RegisterMyServiceServer(grpcServer, mySvc) // Your other services

	// Start server
	lis, err := net.Listen("tcp", ":9090") // #nosec G102 -- example code
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

type roleKey struct{}

// namespaceGuard implements service.SecurityGuard with namespace-based access control.
type namespaceGuard struct {
	allowed map[string][]string // role -> allowed namespaces
}

func (g *namespaceGuard) Authenticate(ctx context.Context) (service.Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	roles := md.Get("x-role")
	if len(roles) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing role")
	}

	return &roleIdentity{role: roles[0]}, nil
}

func (g *namespaceGuard) Authorize(_ context.Context, id service.Identity, _ string, _ service.Resource) (service.Decision, error) {
	role := id.(*roleIdentity).role
	if _, ok := g.allowed[role]; !ok {
		return service.Decision{
			Allowed: false,
			Reason:  "unknown role: " + role,
		}, nil
	}
	return service.Decision{Allowed: true}, nil
}

// roleIdentity holds the authenticated role.
type roleIdentity struct {
	role string
}

func (r *roleIdentity) UserID() string         { return r.role }
func (r *roleIdentity) Claims() map[string]any { return map[string]any{"role": r.role} }
