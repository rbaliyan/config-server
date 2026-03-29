# CLAUDE.md

This file provides guidance for AI assistants working on this codebase.

## Project Overview

A gRPC config server with HTTP/JSON gateway that exposes `config.Store` operations over the network. Includes a Go client (`RemoteStore`) that implements `config.Store` for transparent remote access.

## Architecture

### Package Structure

```
config-server/
├── proto/config/v1/     # Protobuf definitions and generated code
│   └── config.proto     # Service, messages, enums
├── service/             # gRPC service implementation
│   ├── service.go       # ConfigService (Get, Set, Delete, List, GetVersions, Watch, CheckAccess)
│   ├── authorizer.go    # Authorizer interface, AllowAll, DenyAll
│   ├── errors.go        # config error → gRPC status mapping
│   ├── interceptors.go  # Logging and recovery interceptors
│   └── options.go       # Service options (WithAuthorizer)
├── gateway/             # HTTP/JSON gateway via gRPC-Gateway
│   ├── handler.go       # NewHandler (remote), NewInProcessHandler (in-process)
│   ├── sse.go           # SSE Watch endpoint (Server-Sent Events for /v1/watch)
│   └── options.go       # Gateway options (TLS, dial opts, mux opts, heartbeat)
├── client/              # Go client implementing config.Store
│   ├── store.go         # RemoteStore with retry, circuit breaker, watch
│   └── options.go       # Client options (TLS, retry, circuit, watch, keepalive)
└── examples/            # Usage examples
    ├── standalone/      # Full gRPC + HTTP server
    ├── embedded/        # Embed into existing gRPC server with custom auth
    └── client/          # Client with config.Manager
```

### Key Design Decisions

- **Authorizer interface**: Separates authentication (interceptors) from authorization (Authorizer). Default is DenyAll for safety.
- **RemoteStore implements config.Store and config.VersionedStore**: Transparent to callers — works with Manager, live.Ref, bind.Binder. Versioning support is proxied to the server; if the server's store doesn't support versioning, `ErrVersioningNotSupported` is returned.
- **Retry with circuit breaker**: Exponential backoff with jitter, per-call timeout support, non-retryable error classification.
- **Watch reconnection**: Auto-reconnects on network errors, resets backoff after successful stream connection, max consecutive errors limit.
- **WatchResult**: Extended watch API with error access and stop control, alongside standard Watch() for config.Store compatibility.

### HTTP Routes (from proto annotations)

| Method | Path | RPC |
|--------|------|-----|
| GET | `/v1/namespaces/{namespace}/keys/{key}` | Get |
| POST | `/v1/namespaces/{namespace}/keys/{key}` | Set |
| DELETE | `/v1/namespaces/{namespace}/keys/{key}` | Delete |
| GET | `/v1/namespaces/{namespace}/keys` | List |
| GET | `/v1/namespaces/{namespace}/keys/{key}/versions` | GetVersions |
| GET | `/v1/namespaces/{namespace}/access` | CheckAccess |
| GET | `/v1/watch?namespaces=...&prefixes=...` | Watch (SSE) |

## Code Style

- Same conventions as the config library (see config/CLAUDE.md)
- gRPC errors use `google.golang.org/grpc/status` and `codes`
- Authorization errors return `codes.PermissionDenied`
- Input validation errors return `codes.InvalidArgument`
- Client option functions validate inputs with safe clamping

## Testing

```bash
go test -race ./...
```

Tests use:
- `bufconn` for in-process gRPC integration tests (service, gateway)
- Mock clients and streams for unit tests (client)
- No external dependencies required

## Error Mapping

| config error | gRPC code |
|-------------|-----------|
| `ErrNotFound` | `NotFound` |
| `ErrKeyExists` | `AlreadyExists` |
| `ErrInvalidKey` | `InvalidArgument` |
| `ErrTypeMismatch` | `InvalidArgument` |
| `ErrReadOnly` | `FailedPrecondition` |
| `ErrStoreClosed` | `Unavailable` |
| `ErrWatchNotSupported` | `Unimplemented` |
| `ErrVersionNotFound` | `NotFound` |
| `ErrVersioningNotSupported` | `Unimplemented` |

## Dependencies

- `github.com/rbaliyan/config` - Core config library
- `google.golang.org/grpc` - gRPC framework
- `google.golang.org/protobuf` - Protocol Buffers
- `github.com/grpc-ecosystem/grpc-gateway/v2` - HTTP/JSON gateway
