# Config Server

[![CI](https://github.com/rbaliyan/config-server/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/config-server/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/config-server.svg)](https://pkg.go.dev/github.com/rbaliyan/config-server)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/config-server)](https://goreportcard.com/report/github.com/rbaliyan/config-server)

A gRPC configuration server with HTTP/JSON gateway, pluggable authorization, and a Go client that implements `config.Store`.

## Features

- **gRPC API**: Full CRUD + Watch (server-streaming) for configuration entries
- **HTTP/JSON Gateway**: RESTful API via gRPC-Gateway, auto-generated from proto definitions
- **Pluggable Authorization**: Namespace + operation-level access control via `Authorizer` interface
- **Go Client (`RemoteStore`)**: Implements `config.Store` — use with `config.Manager` like any local store
- **Resilience**: Retries with exponential backoff, circuit breaker, per-call timeouts
- **Watch Streams**: Real-time change notifications with automatic reconnection
- **In-Process Mode**: Run HTTP gateway and gRPC service in the same process without network overhead

## Installation

```bash
go get github.com/rbaliyan/config-server
```

## Quick Start

### Server

```go
package main

import (
    "context"
    "log"
    "net"

    "github.com/rbaliyan/config/memory"
    configpb "github.com/rbaliyan/config-server/proto/config/v1"
    "github.com/rbaliyan/config-server/service"
    "google.golang.org/grpc"
)

func main() {
    ctx := context.Background()

    store := memory.NewStore()
    store.Connect(ctx)
    defer store.Close(ctx)

    svc := service.NewService(store,
        service.WithAuthorizer(service.AllowAll()), // dev only!
    )

    grpcServer := grpc.NewServer()
    configpb.RegisterConfigServiceServer(grpcServer, svc)

    lis, _ := net.Listen("tcp", ":9090")
    log.Fatal(grpcServer.Serve(lis))
}
```

### Client

```go
store, _ := client.NewRemoteStore("localhost:9090",
    client.WithInsecure(),
    client.WithRetry(3, 100*time.Millisecond, 5*time.Second),
)

mgr, _ := config.New(config.WithStore(store))
mgr.Connect(ctx)
defer mgr.Close(ctx)

cfg := mgr.Namespace("production")
val, _ := cfg.Get(ctx, "app/timeout")
```

The `RemoteStore` implements `config.Store`, so it works seamlessly with `config.Manager`, `live.Ref[T]`, `bind.Binder`, and all other config library features.

## API

### gRPC

| RPC | Description |
|-----|-------------|
| `Get(namespace, key)` | Retrieve a single config entry |
| `Set(namespace, key, value, codec, write_mode)` | Create or update an entry |
| `Delete(namespace, key)` | Remove an entry |
| `List(namespace, prefix, limit, cursor)` | List entries with pagination |
| `Watch(namespaces, prefixes)` | Stream real-time changes (server-streaming) |
| `CheckAccess(namespace)` | Check read/write access for a namespace |

### HTTP/JSON Gateway

The gateway exposes a RESTful API auto-mapped from the proto definitions:

| HTTP | gRPC | Path |
|------|------|------|
| `GET` | Get | `/v1/namespaces/{namespace}/keys/{key}` |
| `POST` | Set | `/v1/namespaces/{namespace}/keys/{key}` |
| `DELETE` | Delete | `/v1/namespaces/{namespace}/keys/{key}` |
| `GET` | List | `/v1/namespaces/{namespace}/keys?prefix=app/&limit=100&cursor=...` |
| `GET` | CheckAccess | `/v1/namespaces/{namespace}/access` |
| `GET` | Watch (SSE) | `/v1/watch?namespaces=ns1&namespaces=ns2&prefixes=app/` |

#### Examples

```bash
# Get a value
curl http://localhost:8080/v1/namespaces/production/keys/app/timeout

# Set a value
curl -X POST http://localhost:8080/v1/namespaces/production/keys/app/timeout \
  -H 'Content-Type: application/json' \
  -d '{"value": "MzA=", "codec": "json"}'

# List with prefix
curl 'http://localhost:8080/v1/namespaces/production/keys?prefix=app/'

# Delete
curl -X DELETE http://localhost:8080/v1/namespaces/production/keys/app/timeout

# Check access
curl http://localhost:8080/v1/namespaces/production/access

# Watch for changes (SSE stream)
curl -N 'http://localhost:8080/v1/watch?namespaces=production&prefixes=app/'
```

#### SSE Watch

The `/v1/watch` endpoint streams real-time config changes as Server-Sent Events, making Watch available to HTTP clients (browsers, curl, etc.).

Query parameters:
- `namespaces` (repeated) — namespaces to watch (empty = all)
- `prefixes` (repeated) — key prefixes to filter on (empty = all)

SSE stream format:
```
retry: 5000
: connected

event: set
data: {"type":"SET","namespace":"production","key":"app/timeout","value":"MzA=","codec":"json","version":2}

event: delete
data: {"type":"DELETE","namespace":"production","key":"app/old"}

: heartbeat
```

The stream begins with a `retry: 5000` hint (reconnect after 5 seconds) and a `: connected` comment. Heartbeat comments are sent every 30 seconds (configurable via `WithHeartbeatInterval`) to keep connections alive through proxies.

The `value` field is base64-encoded (standard JSON encoding for byte arrays). Use `atob()` in JavaScript or `base64.StdEncoding.DecodeString()` in Go to decode it.

**Note:** `Last-Event-ID` resumption is not supported. On reconnect, clients receive events from the current point forward — there is no replay of missed events. For durable delivery, use the gRPC Watch stream with the Go client's automatic reconnection (`WithWatchReconnect`).

JavaScript example:
```javascript
const es = new EventSource('/v1/watch?namespaces=production&prefixes=app/');
es.addEventListener('set', (e) => console.log('SET:', JSON.parse(e.data)));
es.addEventListener('delete', (e) => console.log('DELETE:', JSON.parse(e.data)));
es.addEventListener('error', (e) => console.error('Error:', e.data));
```

#### Gateway Setup

Connect to a remote gRPC server:

```go
handler, _ := gateway.NewHandler(ctx, "config-server:9090",
    gateway.WithTLS(nil),          // System TLS
    gateway.WithMuxOptions(...),   // Custom ServeMux options
)
http.Handle("/", handler)
```

Or run in-process (no network hop):

```go
svc := service.NewService(store, service.WithAuthorizer(auth))
handler, _ := gateway.NewInProcessHandler(ctx, svc)
http.Handle("/", handler)
```

## Authorization

Authorization defaults to **deny-all** for safety. You must explicitly configure an authorizer.

### Authorizer Interface

```go
type Authorizer interface {
    Authorize(ctx context.Context, req AuthRequest) error
}

type AuthRequest struct {
    Namespace string
    Key       string    // Empty for List/Watch operations
    Operation Operation // Read, Write, Delete, List, Watch
}
```

Return `nil` to allow, or a gRPC status error (typically `codes.PermissionDenied`) to deny.

### Built-in Authorizers

```go
service.AllowAll()  // Permits everything (dev/testing only)
service.DenyAll()   // Denies everything (safe default)
```

### Custom Authorizer Example

Implement namespace-based RBAC using gRPC metadata for authentication:

```go
// 1. Extract auth info via gRPC interceptor
func authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
    md, _ := metadata.FromIncomingContext(ctx)
    roles := md.Get("x-role")
    if len(roles) == 0 {
        return nil, status.Error(codes.Unauthenticated, "missing role")
    }
    ctx = context.WithValue(ctx, roleKey{}, roles[0])
    return handler(ctx, req)
}

// 2. Check permissions in authorizer
type rbacAuthorizer struct {
    allowed map[string][]string // role -> namespaces
}

func (a *rbacAuthorizer) Authorize(ctx context.Context, req service.AuthRequest) error {
    role := ctx.Value(roleKey{}).(string)
    for _, ns := range a.allowed[role] {
        if ns == req.Namespace {
            return nil
        }
    }
    return status.Errorf(codes.PermissionDenied, "access denied")
}

// 3. Wire it up
svc := service.NewService(store,
    service.WithAuthorizer(&rbacAuthorizer{
        allowed: map[string][]string{
            "admin":    {"production", "staging"},
            "readonly": {"staging"},
        },
    }),
)
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(authInterceptor, service.LoggingInterceptor(logger)),
)
```

The separation between **authentication** (interceptor extracts identity) and **authorization** (Authorizer checks permissions) lets you integrate with any auth system: JWT, mTLS, API keys, OAuth, etc.

## Server Interceptors

Built-in interceptors for common server concerns:

```go
grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        service.LoggingInterceptor(logger),       // Request logging
        service.RecoveryInterceptor(logger),      // Panic recovery
    ),
    grpc.ChainStreamInterceptor(
        service.StreamLoggingInterceptor(logger),
        service.StreamRecoveryInterceptor(logger),
    ),
)
```

## Client Options

```go
store, _ := client.NewRemoteStore("config-server:9090",
    // TLS (default: insecure)
    client.WithTLS(nil),                    // System TLS
    client.WithInsecure(),                  // No TLS (dev only)

    // Retries
    client.WithRetry(3, 100*time.Millisecond, 5*time.Second),
    client.WithCallTimeout(2*time.Second),  // Per-attempt deadline

    // Circuit breaker
    client.WithCircuitBreaker(5, 30*time.Second),

    // Watch behavior
    client.WithWatchReconnect(true, time.Second),
    client.WithWatchBufferSize(100),
    client.WithWatchMaxErrors(10),

    // Keepalive
    client.WithKeepalive(30*time.Second, 10*time.Second),

    // Observability
    client.WithStateCallback(func(state client.ConnState) {
        log.Printf("connection: %s", state)
    }),
    client.WithWatchErrorCallback(func(err error) {
        log.Printf("watch error: %v", err)
    }),

    // Custom gRPC options (e.g., interceptors for auth)
    client.WithDialOptions(
        grpc.WithUnaryInterceptor(myAuthInterceptor),
    ),
)
```

## Watch with Error Handling

The standard `Watch()` returns a channel (satisfies `config.Store`). For better control, use `WatchWithResult()`:

```go
result, _ := store.WatchWithResult(ctx, config.WatchFilter{
    Namespaces: []string{"production"},
    Prefixes:   []string{"app/"},
})
defer result.Stop()

for event := range result.Events {
    fmt.Printf("%s %s/%s\n", event.Type, event.Namespace, event.Key)
}

// Check why the watch ended
if err := result.Err(); err != nil {
    log.Printf("watch ended with error: %v", err)
}
```

## Conditional Writes

```go
// Create only (fails with AlreadyExists if key exists)
cfg.Set(ctx, "feature/flag", true, config.WithIfNotExists())

// Update only (fails with NotFound if key doesn't exist)
cfg.Set(ctx, "feature/flag", false, config.WithIfExists())
```

## Proto Definition

The service is defined in [`proto/config/v1/config.proto`](proto/config/v1/config.proto). Key types:

- **`Entry`**: namespace, key, value (bytes), codec, type, version, timestamps
- **`WriteMode`**: UPSERT (default), CREATE, UPDATE
- **`ChangeType`**: SET, DELETE

## Examples

See the [`examples/`](examples/) directory:

- **[`standalone/`](examples/standalone/)** - Full gRPC + HTTP server with interceptors
- **[`embedded/`](examples/embedded/)** - Embed config service into existing gRPC server with custom auth
- **[`client/`](examples/client/)** - Client usage with `config.Manager`

## License

MIT License
