// Example standalone config server demonstrating full gRPC + HTTP setup.
package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/gateway"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create an in-memory store for demonstration
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		log.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	// Seed some initial data
	store.Set(ctx, "default", "app/name", config.NewValue("My App"))
	store.Set(ctx, "default", "app/version", config.NewValue("1.0.0"))
	store.Set(ctx, "default", "database/host", config.NewValue("localhost"))
	store.Set(ctx, "default", "database/port", config.NewValue(5432))

	// Create the config service with AllowAll authorizer (for demo only!)
	configSvc := service.NewService(store,
		service.WithAuthorizer(service.AllowAll()),
	)

	// Create gRPC server with interceptors
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			service.LoggingInterceptor(logger),
			service.RecoveryInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			service.StreamLoggingInterceptor(logger),
			service.StreamRecoveryInterceptor(logger),
		),
	)
	configpb.RegisterConfigServiceServer(grpcServer, configSvc)

	// Start gRPC server
	grpcLis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		logger.Info("starting gRPC server", "addr", ":9090")
		if err := grpcServer.Serve(grpcLis); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	// Create HTTP gateway (use WithInsecure for local development)
	httpHandler, err := gateway.NewHandler(ctx, "localhost:9090", gateway.WithInsecure())
	if err != nil {
		log.Fatalf("failed to create gateway: %v", err)
	}

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: httpHandler,
	}

	go func() {
		logger.Info("starting HTTP server", "addr", ":8080")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutting down...")

	grpcServer.GracefulStop()
	httpServer.Shutdown(context.Background())
}
