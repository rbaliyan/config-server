# Config Server

[![CI](https://github.com/rbaliyan/config-server/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/config-server/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/config-server.svg)](https://pkg.go.dev/github.com/rbaliyan/config-server)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/config-server)](https://goreportcard.com/report/github.com/rbaliyan/config-server)
[![Release](https://img.shields.io/github/v/release/rbaliyan/config-server)](https://github.com/rbaliyan/config-server/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/config-server/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/config-server)

A gRPC configuration server with HTTP/JSON gateway, pluggable authorization, and a Go client that implements `config.Store`.

## Features

- **gRPC API**: 13 RPCs — CRUD, Watch (server-streaming), version history, namespace snapshots, key aliases, and codec/namespace discovery
- **HTTP/JSON Gateway**: RESTful API via gRPC-Gateway, auto-generated from proto definitions, with SSE watch (including `Last-Event-ID` resumption) and a version-diff endpoint
- **Pluggable Security**: Authentication + authorization via the `SecurityGuard` interface; OPA integration shipped as a sub-module
- **Embedded Dashboard**: Optional web UI mounted at `/dashboard/` via `gateway.WithDashboard()`, with pluggable auth (cookie, bearer, HMAC, OIDC)
- **Audit Logging**: Pluggable `Auditor` (slog, SQL, or fan-out) records every config mutation
- **Snapshots**: Point-in-time namespace export with ETag / `If-None-Match` caching
- **Go Client (`RemoteStore`)**: Implements `config.Store` and `config.VersionedStore` — use with `config.Manager` like any local store
- **Resilience**: Retries with exponential backoff, circuit breaker, per-call timeouts, server-side token-bucket rate limiting
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
| `Snapshot(namespace, if_none_match)` | Point-in-time export of a namespace with ETag caching |
| `Watch(namespaces, prefixes)` | Stream real-time changes (server-streaming) |
| `CheckAccess(namespace)` | Check read/write access for a namespace |
| `SetAlias(alias, target)` | Create a key alias mapping |
| `DeleteAlias(alias)` | Remove a key alias |
| `GetAlias(alias)` | Retrieve a specific alias and its target |
| `ListAliases()` | List all registered key aliases |
| `ListCodecs()` | List codec names registered on the server |
| `ListNamespaces(prefix, limit, cursor)` | List namespaces that contain at least one entry |

Stores that do not implement `config.NamespaceLister` fall back to a `Stats()` call whose result is cached (default 30s, singleflight-deduplicated) and tunable via `service.WithNamespaceStatsCacheTTL`.

All RPCs except `Watch` (server-streaming, gRPC only) are also exposed over HTTP/JSON — see the table below.

### HTTP/JSON Gateway

The gateway exposes a RESTful API auto-mapped from the proto definitions:

| HTTP | gRPC | Path |
|------|------|------|
| `GET` | Get | `/v1/namespaces/{namespace}/keys/{key}` |
| `POST` | Set | `/v1/namespaces/{namespace}/keys/{key}` |
| `DELETE` | Delete | `/v1/namespaces/{namespace}/keys/{key}` |
| `GET` | List | `/v1/namespaces/{namespace}/keys?prefix=app/&limit=100&cursor=...` |
| `GET` | GetVersions | `/v1/namespaces/{namespace}/keys/{key}/versions?version=3&limit=10&cursor=...` |
| `GET` | Snapshot | `/v1/namespaces/{namespace}/snapshot` |
| `GET` | Diff | `/v1/namespaces/{namespace}/keys/{key}/diff?v1=1&v2=2` |
| `GET` | CheckAccess | `/v1/namespaces/{namespace}/access` |
| `GET` | ListNamespaces | `/v1/namespaces?prefix=&limit=100&cursor=...` |
| `GET` | ListCodecs | `/v1/codecs` |
| `PUT` | SetAlias | `/v1/aliases/{alias}` |
| `GET` | GetAlias | `/v1/aliases/{alias}` |
| `DELETE` | DeleteAlias | `/v1/aliases/{alias}` |
| `GET` | ListAliases | `/v1/aliases` |
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

#### Securing the Dashboard

By default the dashboard is served without access control. Pass
`gateway.WithDashboardAuth` with a `dashboard.DashboardAuth` to protect both the
dashboard route and the API calls the dashboard JS makes — the same credential
is validated on both paths, so configure your `SecurityGuard` to accept it too.

Three built-in strategies are provided:

```go
// Session/JWT cookie — the browser forwards it automatically; no token UI.
auth := dashboard.CookieAuth("session", func(r *http.Request, v string) error {
    return validateSession(v) // nil = accept; non-nil = 401
})

// Bearer token — a sidebar token field stored in sessionStorage, attached as
// "Authorization: Bearer <token>" on every request.
auth := dashboard.BearerTokenAuth(func(r *http.Request, token string) error {
    return validateToken(token)
})

// Self-contained passphrase login — serves an inline login form, issues a
// stateless HMAC-SHA256 session cookie, no session store required.
auth, err := dashboard.HMACAuth(dashboard.HMACConfig{
    Secret:     []byte(os.Getenv("DASH_SECRET")), // >= 32 bytes
    Passphrase: os.Getenv("DASH_PASSPHRASE"),
    Secure:     true, // requires HTTPS in production
})

handler, _ := gateway.NewHandler(ctx, "config-server:9090",
    gateway.WithDashboard(),
    gateway.WithDashboardAuth(auth),
)
```

For any other scheme (mTLS, HTTP Basic, OIDC, a custom header) implement the
`dashboard.DashboardAuth` interface directly — `Middleware(next)` enforces auth
server-side and `ClientConfig()` tells the dashboard JS how to attach the
credential. Two complete worked examples ship in the repo:

- [`examples/dashboard-auth/jwt/`](examples/dashboard-auth/jwt/) — `CookieAuth` and `BearerTokenAuth` backed by JWT validation
- [`examples/dashboard-auth/oidc/`](examples/dashboard-auth/oidc/) — a full OIDC authorization-code flow implementing `DashboardAuth`

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

#### JWT signature verification

By default the OPA authorizer base64-decodes a JWT's claims segment to expose
them to the Rego policy as `input.identity.claims` **without verifying the
signature** — any well-formed token is accepted. In this mode you must pair the
guard with an upstream proxy (API gateway, ingress, or service mesh) that has
already validated the token, or call OPA's built-in token introspection
functions (`io.jwt.verify_*`, `io.jwt.decode_verify`) from the Rego policy.

Alternatively, enable in-process verification with `opa.WithJWTVerifier`. When a
verifier is set, `Authenticate` calls `Verify`, which enforces the signature,
expiry, and audience; any failure rejects the request. The package ships three
verifier constructors:

```go
// HMAC (HS256 by default; HS384/HS512 via (*HMACVerifier).WithHMACAlgorithm)
guard, _ := opa.NewAuthorizer(ctx, policy, "data.config.authz.allow",
    opa.WithJWTVerifier(opa.NewHMACVerifier([]byte("shared-secret"))))

// RSA public key (RS256 by default; RS384/RS512 via (*RSAVerifier).WithRSAAlgorithm)
guard, _ := opa.NewAuthorizer(ctx, policy, "data.config.authz.allow",
    opa.WithJWTVerifier(opa.NewRSAVerifier(pubKey)))

// JWKS endpoint (fetched per Verify call; suits OIDC providers that rotate keys)
guard, _ := opa.NewAuthorizer(ctx, policy, "data.config.authz.allow",
    opa.WithJWTVerifier(opa.NewJWKSVerifier(jwksURL)))
```

`NewHMACVerifier(secret []byte)`, `NewRSAVerifier(key *rsa.PublicKey)`, and
`NewJWKSVerifier(url string)` each return a `JWTVerifier`; you may also supply
your own implementation of the interface.

#### Other OPA options

| Option | Purpose |
|--------|---------|
| `opa.WithAuthHeader(header string)` | gRPC metadata key (or HTTP header) to read the bearer token from. Default `"authorization"`. |
| `opa.WithSubjectClaim(claim string)` | JWT claim used as the user ID. Default `"sub"`. |
| `opa.WithBundlePollInterval(d time.Duration)` | How often `NewBundleAuthorizer` re-fetches the bundle URL. Default 30s. |
| `opa.WithTLSConfig(cfg *tls.Config)` | TLS config for bundle HTTP fetching. |

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

for event := range result.Events() {
    fmt.Printf("%s %s/%s\n", event.Type, event.Namespace, event.Key)
}

// Check why the watch ended
if err := result.Err(); err != nil {
    log.Printf("watch ended with error: %v", err)
}
```

## Snapshots

`Snapshot` returns a point-in-time export of every entry in a namespace, with an
opaque `ETag` for client-side caching. Pass the previous ETag via
`client.WithIfNoneMatch` to skip the transfer when nothing has changed: the
server compares it against the freshly computed ETag and, on a match, returns
`NotModified: true` with an empty `Entries` map (an `If-None-Match` conditional
fetch).

```go
result, _ := store.Snapshot(ctx, "production")
fmt.Println(result.ETag, len(result.Entries))

// Later: re-fetch only if the namespace changed.
next, _ := store.Snapshot(ctx, "production", client.WithIfNoneMatch(result.ETag))
if next.NotModified {
    // Nothing changed; keep using the cached snapshot.
}
```

`SnapshotResult` carries `Entries map[string]config.Value`, `ETag string`, and
`NotModified bool`. The ETag is a SHA-256 digest of the sorted `(key, version)`
tuples, so it changes whenever any value in the namespace is written or deleted.

Over HTTP: `GET /v1/namespaces/{namespace}/snapshot`. The number of entries a
single snapshot may return is bounded by `service.WithMaxSnapshotEntries`
(default 10000); a namespace larger than the cap fails with `ResourceExhausted`.

## Aliases

When the backing store implements `config.AliasStore`, the server exposes alias
management — a second key name that resolves to a canonical target key. The
`RemoteStore` proxies all four operations:

```go
// Create an alias "db/url" → "database/connection-string".
_, _ = store.SetAlias(ctx, "db/url", "database/connection-string")

val, _ := store.GetAlias(ctx, "db/url")   // resolves the target
all, _ := store.ListAliases(ctx)          // map[alias]config.Value
_ = store.DeleteAlias(ctx, "db/url")
```

`SetAlias` returns `AlreadyExists` if the alias key is already registered or
conflicts with an existing config key. Stores that do not implement
`config.AliasStore` return `Unimplemented` for every alias RPC. Alias mutations
also surface on the Watch stream as `CHANGE_TYPE_ALIAS_SET` /
`CHANGE_TYPE_ALIAS_DELETE` events.

Over HTTP: `PUT /v1/aliases/{alias}` (body `{"target":"..."}`),
`GET /v1/aliases/{alias}`, `DELETE /v1/aliases/{alias}`, and
`GET /v1/aliases` to list.

## Audit Logging

Pass `service.WithAuditor` to record every config mutation (set, delete, and
alias changes). An `Auditor` implements a single method:

```go
type Auditor interface {
    Record(ctx context.Context, entry AuditEntry) error
}
```

Three implementations ship with the service:

```go
// 1. Structured slog records at Info level.
logAud := service.NewLogAuditor(slog.Default())

// 2. Persist to a SQL table (PostgreSQL or SQLite). driverName must match the
//    driver passed to sql.Open; call CreateTable once before first use.
sqlAud := service.NewSQLAuditor(db, "postgres", service.WithAuditTable("my_audit"))
_ = sqlAud.CreateTable(ctx)

// 3. Fan out to several auditors at once; Record joins their errors.
aud := service.NewMultiAuditor(logAud, sqlAud)

svc, _ := service.NewService(store, service.WithAuditor(aud))
```

`AuditEntry` carries the timestamp, the authenticated `Identity`, the operation,
namespace, key, base64-encoded value (set operations only), codec, and a free-form
metadata map. The SQL table name defaults to `config_audit_log` and is overridable
with `service.WithAuditTable`.

## Rate Limiting

The service ships a per-client token-bucket limiter and gRPC interceptors that
reject excess traffic with `ResourceExhausted`:

```go
limiter := service.NewTokenBucketLimiter(
    service.WithRate(50),                       // tokens/second (default 10)
    service.WithBurst(100),                     // bucket size (default 20)
    service.WithCleanupInterval(5*time.Minute), // evict idle clients (default 5m)
    service.WithClientIdentifier(func(ctx context.Context) string {
        // Default identifies clients by peer address.
        return userIDFromContext(ctx)
    }),
)
defer limiter.Close()

grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        service.RateLimitInterceptor(limiter),
        service.AuthInterceptor(guard),
    ),
    grpc.ChainStreamInterceptor(
        service.StreamRateLimitInterceptor(limiter),
        service.StreamAuthInterceptor(guard),
    ),
)
```

`RateLimitInterceptor` / `StreamRateLimitInterceptor` accept any `RateLimiter`.
A limiter that also implements `ClientIdentifier` (as `TokenBucketLimiter` does)
has its `ClientID` called to bucket requests; otherwise the interceptor falls
back to the gRPC peer address.

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
- **`ChangeType`**: SET, DELETE, ALIAS_SET, ALIAS_DELETE

## Examples

See the [`examples/`](examples/) directory:

- **[`standalone/`](examples/standalone/)** - Full gRPC + HTTP server with interceptors
- **[`embedded/`](examples/embedded/)** - Embed config service into existing gRPC server with custom auth
- **[`client/`](examples/client/)** - Client usage with `config.Manager`
- **[`dashboard-auth/jwt/`](examples/dashboard-auth/jwt/)** - Secure the dashboard with `CookieAuth` / `BearerTokenAuth` + JWT
- **[`dashboard-auth/oidc/`](examples/dashboard-auth/oidc/)** - Secure the dashboard with a custom OIDC `DashboardAuth`

## Peer Synchronisation (`peersync`)

The `peersync` package wraps a `config.Store` with consistent-hash namespace ownership and gossip-based cluster membership. Each node holds its own backing store; the ring partitions namespaces across nodes and transparently forwards reads/writes to the owner via `PeerDialer`.

```bash
go get github.com/rbaliyan/config-server/peersync
```

### Forwarding to peers (`GRPCDialer`)

`PeerDialer` is the interface peersync uses to forward an operation to the node
that owns a namespace. `peersync.NewGRPCDialer` is the built-in implementation:
it opens (and caches, by address) a `client.RemoteStore` connection to each
peer, applying the supplied client options to every connection.

```go
dialer := peersync.NewGRPCDialer(client.WithInsecure())
defer dialer.Close()

nodeA, _ := peersync.New(storeA, peersync.Member{ID: "nodeA", Addr: "nodeA:9000"}, tr,
    peersync.WithPeerDialer(dialer),
)
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

You don't have to write the SQL yourself — the `peersync/sqlownership` sub-package
ships a ready-made `OwnershipStore` for PostgreSQL and SQLite:

```go
import "github.com/rbaliyan/config-server/peersync/sqlownership"

os := sqlownership.New(db, "postgres") // or "sqlite3"; sqlownership.WithTable to rename
_ = os.CreateTable(ctx)

nodeA, _ := peersync.New(storeA, self, tr, peersync.WithOwnershipStore(os))
```

See the [package documentation](https://pkg.go.dev/github.com/rbaliyan/config-server/peersync) for the full API including `Pin`, `Claim`, health checking, and dead-owner handling.

## License

MIT License
