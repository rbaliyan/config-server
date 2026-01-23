// Example client using RemoteStore with config.Manager.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create remote store connecting to the config server
	// Use WithInsecure for local development, WithTLS for production
	store, err := client.NewRemoteStore("localhost:9090",
		client.WithInsecure(),
		client.WithDialOptions(
			grpc.WithUnaryInterceptor(addRoleInterceptor("admin")),
		),
	)
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}

	// Use the remote store with config.Manager - same API as local store!
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		log.Fatalf("failed to create manager: %v", err)
	}

	if err := mgr.Connect(ctx); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer mgr.Close(ctx)

	// Access configuration through namespaces
	cfg := mgr.Namespace("default")

	// Get a value
	val, err := cfg.Get(ctx, "app/name")
	if err != nil {
		log.Fatalf("failed to get: %v", err)
	}
	name, _ := val.String()
	fmt.Printf("app/name = %s\n", name)

	// Get another value
	val, err = cfg.Get(ctx, "database/port")
	if err != nil {
		log.Fatalf("failed to get: %v", err)
	}
	port, _ := val.Int64()
	fmt.Printf("database/port = %d\n", port)

	// Set a value
	err = cfg.Set(ctx, "app/environment", "development")
	if err != nil {
		log.Fatalf("failed to set: %v", err)
	}
	fmt.Println("set app/environment = development")

	// List values with prefix
	page, err := cfg.Find(ctx, config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		log.Fatalf("failed to list: %v", err)
	}
	fmt.Println("\nAll app/* keys:")
	for key, val := range page.Results() {
		str, _ := val.String()
		fmt.Printf("  %s = %s\n", key, str)
	}
}

// addRoleInterceptor adds a role to outgoing requests.
func addRoleInterceptor(role string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-role", role)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
