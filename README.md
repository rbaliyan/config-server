# Config Server

[![CI](https://github.com/rbaliyan/config-server/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/config-server/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/config-server.svg)](https://pkg.go.dev/github.com/rbaliyan/config-server)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/config-server)](https://goreportcard.com/report/github.com/rbaliyan/config-server)
[![Release](https://img.shields.io/github/v/release/rbaliyan/config-server)](https://github.com/rbaliyan/config-server/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/config-server/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/config-server)

A gRPC configuration server with HTTP/JSON gateway, pluggable authorization, and a Go client that implements `config.Store`.

## Features

- **gRPC API**: Full CRUD + Watch (server-streaming) + Version History for configuration entries
- **HTTP/JSON Gateway**: RESTful API via gRPC-Gateway, auto-generated from proto definitions, with SSE watch (including `Last-Event-ID` resumption) and a version-diff endpoint
- **Pluggable Security**: Authentication + authorization via the `SecurityGuard` interface; OPA integration shipped as a sub-module
- **Embedded Dashboard**: Optional web UI mounted at `/dashboard/` via `gateway.WithDashboard()`
- **Go Client (`RemoteStore`)**: Implements `config.Store` and `config.VersionedStore` — use with `config.Manager` like any local store
- **Resilience**: Retries with exponential backoff, circuit breaker, per-call timeouts, server-side rate limiting
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

    svc, err := service.NewService(store,
        service.WithSecurityGuard(service.AllowAll()), // dev only!
    )
    if err != nil {
        log.Fatal(err)
    }

    // Install AuthInterceptor so every RPC is authenticated against the guard
    // before it reaches the service. AllowAll treats every caller as anonymous.
    grpcServer := grpc.NewServer(
        grpc.ChainUnaryInterceptor(service.AuthInterceptor(service.AllowAll())),
        grpc.ChainStreamInterceptor(service.StreamAuthInterceptor(service.AllowAll())),
    )
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

The `RemoteStore` implements `config.Store` and `config.VersionedStore`, so it works seamlessly with `config.Manager`, `live.Ref[T]`, `bind.Binder`, and all other config library features — including version history retrieval.

## API

### gRPC

| RPC | Description |
|-----|-------------|
| `Get(namespace, key)` | Retrieve a single config entry |
| `Set(namespace, key, value, codec, write_mode)` | Create or update an entry |
| `Delete(namespace, key)` | Remove an entry |
| `List(namespace, prefix, limit, cursor)` | List entries with pagination |
| `GetVersions(namespace, key, version, limit, cursor)` | Retrieve version history for a key |
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
| `GET` | GetVersions | `/v1/namespaces/{namespace}/keys/{key}/versions?version=3&limit=10&cursor=...` |
| `GET` | Diff | `/v1/namespaces/{namespace}/keys/{key}/diff?v1=1&v2=2` |
| `GET` | CheckAccess | `/v1/namespaces/{namespace}/access` |
| `GET` | Watch (SSE) | `/v1/watch?namespaces=ns1&namespaces=ns2&prefixes=app/` |

The `diff` endpoint returns a JSON object with both versions' raw bytes, codecs, and a `changed` flag. It is implemented in the gateway itself (not in the proto service) and is available on both `NewHandler` and `NewInProcessHandler`.

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

# Get version history
curl 'http://localhost:8080/v1/namespaces/production/keys/app/timeout/versions?limit=10'

# Get a specific version
curl 'http://localhost:8080/v1/namespaces/production/keys/app/timeout/versions?version=2'

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

id: 42
event: set
data: {"type":"SET","namespace":"production","key":"app/timeout","value":"MzA=","codec":"json","version":2}

id: 43
event: delete
data: {"type":"DELETE","namespace":"production","key":"app/old"}

: heartbeat
```

The stream begins with a `retry: 5000` hint (reconnect after 5 seconds) and a `: connected` comment. Heartbeat comments are sent every 30 seconds (configurable via `WithHeartbeatInterval`) to keep connections alive through proxies.

The `value` field is base64-encoded (standard JSON encoding for byte arrays). Use `atob()` in JavaScript or `base64.StdEncoding.DecodeString()` in Go to decode it.

#### Last-Event-ID Resumption

Each event carries a monotonically increasing `id:` line. The gateway keeps a bounded in-memory ring buffer (default: 500 events, configurable via `WithEventBufferSize`; set `0` to disable) and, when a client reconnects with the standard `Last-Event-ID` HTTP header or SSE `EventSource` automatic reconnect, replays every buffered event whose id is strictly greater than `Last-Event-ID`. Replay happens before the live stream resumes, so no ordering guarantees are broken.

The buffer is per-handler (per `NewHandler` / `NewInProcessHandler`) and per-process; if your deployment fans clients across multiple gateway instances, each instance has an independent buffer. Events older than the buffer window (or lost to a gateway restart) are not replayed — clients that cannot tolerate gaps should fall back to re-reading the affected keys or use the gRPC Watch stream with the Go client's automatic reconnection (`WithWatchReconnect`).

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
    gateway.WithTLS(nil),             // System TLS
    gateway.WithMuxOptions(...),      // Custom ServeMux options
    gateway.WithEventBufferSize(500), // Enables Last-Event-ID replay (default 500, 0 disables)
    gateway.WithDashboard(),          // Mount dashboard at /dashboard/
)
defer handler.Close()
http.Handle("/", handler)
```

Or run in-process (no network hop):

```go
svc, _ := service.NewService(store, service.WithSecurityGuard(guard))
handler, _ := gateway.NewInProcessHandler(ctx, svc, gateway.WithDashboard())
http.Handle("/", handler)
```

#### Embedded Dashboard

Pass `gateway.WithDashboard()` to mount an embedded web UI at `/dashboard/`. The dashboard is a static bundle (HTML/JS/CSS) served from the gateway and drives all data operations through the existing REST endpoints, so no additional server state is required. Use `gateway.WithDashboardPath("/ui")` to mount it at a different path (path must start with `/` and should not end with `/`).

## Security

Security is modelled by a single `SecurityGuard` interface that handles **both** authentication and authorization. The service defaults to `DenyAll` for safety — you must explicitly configure a guard via `service.WithSecurityGuard`.

### SecurityGuard Interface

```go
type SecurityGuard interface {
    // Authenticate extracts and validates the caller's identity from ctx.
    Authenticate(ctx context.Context) (Identity, error)

    // Authorize checks whether id may perform action on resource.
    // action is one of "read", "write", "delete", "list", "watch".
    // Resource carries the namespace and/or key when known (both may be
    // empty for method-level checks).
    Authorize(ctx context.Context, id Identity, action string, resource Resource) (Decision, error)
}

type Identity interface {
    UserID() string
    Claims() map[string]any
}

type Resource struct {
    Namespace string
    Key       string
}

type Decision struct {
    Allowed bool
    Scope   string
    Reason  string
}
```

Install `service.AuthInterceptor(guard)` and `service.StreamAuthInterceptor(guard)` on the gRPC server so the interceptor calls `guard.Authenticate` once per RPC and places the resulting Identity on the context. Each Service method then calls `guard.Authorize` with the specific action and resource it is about to execute.

### Built-in Guards

```go
service.AllowAll()  // Authenticates everyone as anonymous, allows everything (dev/testing only)
service.DenyAll()   // Authenticate always fails — the safe default
```

### Custom Guard Example

A minimal RBAC guard using gRPC metadata for authentication:

```go
type rbacGuard struct {
    allowed map[string][]string // role -> namespaces
}

type roleIdentity struct{ role, user string }

func (r *roleIdentity) UserID() string         { return r.user }
func (r *roleIdentity) Claims() map[string]any { return map[string]any{"role": r.role} }

func (g *rbacGuard) Authenticate(ctx context.Context) (service.Identity, error) {
    md, _ := metadata.FromIncomingContext(ctx)
    roles := md.Get("x-role")
    users := md.Get("x-user")
    if len(roles) == 0 {
        return nil, status.Error(codes.Unauthenticated, "missing role")
    }
    user := "anonymous"
    if len(users) > 0 {
        user = users[0]
    }
    return &roleIdentity{role: roles[0], user: user}, nil
}

func (g *rbacGuard) Authorize(ctx context.Context, id service.Identity, action string, r service.Resource) (service.Decision, error) {
    role, _ := id.Claims()["role"].(string)
    for _, ns := range g.allowed[role] {
        if ns == r.Namespace {
            return service.Decision{Allowed: true}, nil
        }
    }
    return service.Decision{Allowed: false, Reason: "role cannot access namespace"}, nil
}

guard := &rbacGuard{allowed: map[string][]string{
    "admin":    {"production", "staging"},
    "readonly": {"staging"},
}}

svc, _ := service.NewService(store, service.WithSecurityGuard(guard))

grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        service.AuthInterceptor(guard),
        service.LoggingInterceptor(logger),
    ),
    grpc.ChainStreamInterceptor(
        service.StreamAuthInterceptor(guard),
    ),
)
```

The interface lets you plug in any auth scheme: JWT, mTLS, API keys, OAuth, session cookies, etc.

### OPA SecurityGuard

For policy-driven authorization, the `authorizer/opa` sub-module provides an OPA-backed `SecurityGuard`:

```bash
go get github.com/rbaliyan/config-server/authorizer/opa
```

```go
import "github.com/rbaliyan/config-server/authorizer/opa"

const policy = `
package config.authz
default allow = false
allow if {
    input.action == "read"
    input.identity.user_id != ""
}
`

guard, _ := opa.NewAuthorizer(ctx, policy, "data.config.authz.allow")
// Or pull policy from a bundle URL that is re-fetched every 30s:
// guard, _ := opa.NewBundleAuthorizer(ctx, "https://...", "data.config.authz.allow")

svc, _ := service.NewService(store, service.WithSecurityGuard(guard))
```

**Note:** The OPA authorizer parses JWT tokens to expose their claims to the Rego policy but does **not** verify the JWT signature. Signature, issuer, audience, and expiry checks are the responsibility of the Rego policy (via OPA's built-in token introspection) or of an upstream proxy that has already validated the token.

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

## Client-Side Codecs

When clients use codecs that the server doesn't have registered (e.g., encryption codecs from `config-crypto`), the server treats the bytes as opaque pass-through. This lets clients encrypt values before sending them without requiring the server to hold encryption keys.

Convention: prefix the codec name with `client:` to signal a client-managed codec.

```go
// Client-side: create an encrypted codec with client prefix
encCodec, _ := crypto.NewCodec(jsoncodec.New(), keyProvider, crypto.WithClientCodec())
// encCodec.Name() == "client:encrypted:json"

// Register locally (client only — server never sees this codec)
codec.Register(encCodec)

// Set a value — bytes are encrypted before sending
cfg.Set(ctx, "secrets/api-key", mySecret)
```

The server stores the encrypted bytes and the codec name `"client:encrypted:json"` without attempting to decode them. On retrieval, the client decodes locally.

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

## Peer Synchronisation (`peersync`)

The `peersync` package wraps a `config.Store` with consistent-hash namespace ownership and gossip-based cluster membership. Each node holds its own backing store; the ring partitions namespaces across nodes and transparently forwards reads/writes to the owner via `PeerDialer`.

```bash
go get github.com/rbaliyan/config-server/peersync
```

### Transport options

Two transports are provided out of the box. Both satisfy `peersync.Transport`.

**Redis** (centralised broker — requires a shared Redis instance):

All cluster nodes must use the same Redis instance or channel. Use a distinct channel per logical cluster when multiple clusters share one Redis:

```go
import (
    "github.com/rbaliyan/config-server/peersync"
    goredis "github.com/redis/go-redis/v9"
)

rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

// "" defaults to the built-in channel name "config:sync".
// Use an explicit channel (e.g. "prod:config:sync") when multiple
// independent clusters share a single Redis instance to prevent
// cross-cluster gossip pollution.
tr, err := peersync.NewRedisTransport(rdb, "prod:config:sync")
```

**Memberlist** (peer-to-peer gossip — no external broker):

```go
import "github.com/hashicorp/memberlist"

cfg := memberlist.DefaultLANConfig()
cfg.BindAddr = "0.0.0.0"
cfg.BindPort = 7946

tr, err := peersync.NewMemberlistTransport(cfg)

// Join an existing cluster node; skip for a brand-new single-node cluster.
tr.Join([]string{"peer1:7946", "peer2:7946"})
```

Memberlist uses a peer-to-peer SWIM gossip protocol — no Redis or other broker required. Messages are gossiped across the cluster in O(log N) rounds. Use Redis when you need sub-second convergence; use memberlist when you want zero external dependencies.

### Quick example

```go
storeA := memory.NewStore()
nodeA, _ := peersync.New(storeA, peersync.Member{ID: "nodeA", Addr: "nodeA:9000"}, tr)
nodeA.Connect(ctx)
defer nodeA.Close(ctx)

// Claim makes this node the persistent owner of "payments".
nodeA.Claim(ctx, "payments")

owner, _ := nodeA.OwnerOf("payments")
fmt.Println(owner) // "nodeA"
```

### Persistent ownership (`OwnershipStore`)

Without an `OwnershipStore`, claimed namespaces are in-memory only and lost on restart. Implement the interface against any durable store (e.g. the same SQLite/PostgreSQL database that backs the node) to survive restarts:

```go
// myOwnershipStore implements peersync.OwnershipStore using any SQL database.
type myOwnershipStore struct{ db *sql.DB }

func (s *myOwnershipStore) LoadOwned(ctx context.Context, nodeID string) ([]string, error) {
    rows, err := s.db.QueryContext(ctx,
        "SELECT namespace FROM ns_owners WHERE node_id = $1", nodeID)
    // ... scan rows into a []string
}
func (s *myOwnershipStore) SaveOwner(ctx context.Context, ns, nodeID string) error {
    _, err := s.db.ExecContext(ctx,
        "INSERT INTO ns_owners(namespace, node_id) VALUES($1,$2) ON CONFLICT(namespace) DO UPDATE SET node_id=EXCLUDED.node_id",
        ns, nodeID)
    return err
}
func (s *myOwnershipStore) DeleteOwner(ctx context.Context, ns string) error {
    _, err := s.db.ExecContext(ctx, "DELETE FROM ns_owners WHERE namespace = $1", ns)
    return err
}

nodeA, _ := peersync.New(storeA, peersync.Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
    peersync.WithOwnershipStore(&myOwnershipStore{db: db}),
)
```

On `Connect`, claimed namespaces are reloaded and re-announced before the first gossip broadcast, so ownership survives restarts without operator intervention.

See the [package documentation](https://pkg.go.dev/github.com/rbaliyan/config-server/peersync) for the full API including `Pin`, `Claim`, health checking, and dead-owner handling.

## License

MIT License
