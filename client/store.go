// Package client provides a RemoteStore implementation that connects to a config server.
package client

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// RemoteStore implements config.Store by connecting to a config server.
// It is safe for concurrent use by multiple goroutines.
//
// Features:
//   - Automatic reconnection with exponential backoff
//   - Circuit breaker for resilience
//   - Configurable timeouts and retries
//   - Watch streams with automatic reconnection
//   - Health checks and keepalives
//
// Example:
//
//	store, _ := client.NewRemoteStore("config-server:9090",
//	    client.WithTLS(nil),  // Use system TLS
//	    client.WithRetry(3, 100*time.Millisecond, 5*time.Second),
//	)
//	mgr, _ := config.New(config.WithStore(store))
//	mgr.Connect(ctx)
type RemoteStore struct {
	addr string
	opts *options

	mu      sync.RWMutex
	conn    *grpc.ClientConn
	client  configpb.ConfigServiceClient
	state   atomic.Int32 // ConnState
	stateMu sync.Mutex   // serializes state transitions and callbacks

	// Circuit breaker state
	circuitMu       sync.Mutex
	circuitOpen     bool
	circuitOpenAt   time.Time
	consecutiveFail int

	// Shutdown
	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewRemoteStore creates a new RemoteStore connecting to the given address.
// Returns an error if addr is empty.
func NewRemoteStore(addr string, opts ...Option) (*RemoteStore, error) {
	if addr == "" {
		return nil, errors.New("config-server: address must not be empty")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	s := &RemoteStore{
		addr:    addr,
		opts:    o,
		closeCh: make(chan struct{}),
	}
	s.state.Store(int32(ConnStateDisconnected))

	return s, nil
}

// Compile-time interface checks
var (
	_ config.Store          = (*RemoteStore)(nil)
	_ config.VersionedStore = (*RemoteStore)(nil)
	_ config.AliasStore     = (*RemoteStore)(nil)
)

// State returns the current connection state.
func (s *RemoteStore) State() ConnState {
	return ConnState(s.state.Load())
}

func (s *RemoteStore) setState(state ConnState) {
	s.stateMu.Lock()
	old := ConnState(s.state.Swap(int32(state))) // #nosec G115 -- ConnState is a small enum
	if old != state && s.opts.onStateChange != nil {
		s.opts.onStateChange(state)
	}
	s.stateMu.Unlock()
}

// Connect establishes the connection to the config server.
// The context is reserved for future use.
//
// Note: the onStateChange callback (set via WithStateCallback) is invoked
// while holding stateMu but not s.mu. The callback must not acquire s.mu
// (e.g. by calling Ready()) on the same goroutine to avoid deadlock.
func (s *RemoteStore) Connect(ctx context.Context) error {
	if s.State() == ConnStateClosed {
		return config.ErrStoreClosed
	}

	// Transition to connecting before acquiring the lock so that the
	// onStateChange callback is never invoked under s.mu.
	s.setState(ConnStateConnecting)

	s.mu.Lock()
	// Re-check after acquiring lock in case Close() raced with us.
	if s.State() == ConnStateClosed {
		s.mu.Unlock()
		return config.ErrStoreClosed
	}

	// Close existing connection if any
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
		s.client = nil
	}

	// Build dial options
	dialOpts := s.opts.buildDialOpts()

	conn, err := grpc.NewClient(s.addr, dialOpts...)
	if err != nil {
		s.mu.Unlock()
		s.setState(ConnStateDisconnected)
		return err
	}

	s.conn = conn
	s.client = configpb.NewConfigServiceClient(conn)
	s.mu.Unlock()

	// setState called after releasing s.mu so onStateChange callbacks can
	// safely call back into the store (e.g. Ready()) without deadlocking.
	s.setState(ConnStateConnected)
	s.resetCircuit()

	return nil
}

// Close releases resources and closes the connection.
func (s *RemoteStore) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.setState(ConnStateClosed)

		s.mu.Lock()
		defer s.mu.Unlock()

		if s.conn != nil {
			err = s.conn.Close()
			s.conn = nil
			s.client = nil
		}
	})
	return err
}

// Ready returns true if the store is connected and ready for operations.
func (s *RemoteStore) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.conn == nil {
		return false
	}

	state := s.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// getClient returns the gRPC client, checking circuit breaker and connection state.
func (s *RemoteStore) getClient() (configpb.ConfigServiceClient, error) {
	if s.State() == ConnStateClosed {
		return nil, config.ErrStoreClosed
	}

	// Check circuit breaker
	if s.isCircuitOpen() {
		return nil, &RemoteError{Message: "circuit breaker open"}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.client == nil {
		return nil, config.ErrStoreNotConnected
	}
	return s.client, nil
}

// retry executes fn with retries and exponential backoff.
// If callTimeout is configured, each attempt's context is wrapped with a deadline.
func (s *RemoteStore) retry(ctx context.Context, fn func(ctx context.Context) error) error {
	var lastErr error
	backoff := s.opts.retryBackoff

	for attempt := 0; attempt <= s.opts.maxRetries; attempt++ {
		if attempt > 0 {
			// Add jitter: 0.5x to 1.5x
			jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64())) // #nosec G404 -- jitter does not require cryptographic randomness
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.closeCh:
				return config.ErrStoreClosed
			case <-time.After(jitter):
			}

			// Exponential backoff with cap
			backoff *= 2
			if backoff > s.opts.maxBackoff {
				backoff = s.opts.maxBackoff
			}
		}

		// Optionally wrap the context with a per-call timeout
		attemptCtx := ctx
		var cancel context.CancelFunc
		if s.opts.callTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, s.opts.callTimeout)
		}

		lastErr = fn(attemptCtx)

		if cancel != nil {
			cancel()
		}
		if lastErr == nil {
			s.recordSuccess()
			return nil
		}

		// Don't retry certain errors
		if isNonRetryable(lastErr) {
			s.recordFailure()
			return lastErr
		}
	}

	s.recordFailure()
	return lastErr
}

func isNonRetryable(err error) bool {
	// Don't retry client errors or not found
	switch {
	case errors.Is(err, config.ErrNotFound),
		errors.Is(err, config.ErrKeyExists),
		errors.Is(err, config.ErrInvalidKey),
		errors.Is(err, config.ErrInvalidNamespace),
		errors.Is(err, config.ErrInvalidValue),
		errors.Is(err, config.ErrReadOnly),
		errors.Is(err, config.ErrVersionNotFound),
		errors.Is(err, config.ErrVersioningNotSupported),
		// Store is permanently closed — retrying cannot succeed.
		errors.Is(err, config.ErrStoreClosed):
		return true
	}
	// Don't retry permission errors
	var permErr *PermissionDeniedError
	return errors.As(err, &permErr)
}

// Get retrieves a configuration value by namespace and key.
func (s *RemoteStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	var result config.Value
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.Get(ctx, &configpb.GetRequest{
			Namespace: namespace,
			Key:       key,
		})
		if err != nil {
			return fromGRPCError(err)
		}
		result = protoToValue(ctx, resp.Entry)
		return nil
	})
	return result, err
}

// Set creates or updates a configuration value.
func (s *RemoteStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	data, err := value.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	var writeMode configpb.WriteMode
	switch config.GetWriteMode(value) {
	case config.WriteModeCreate:
		writeMode = configpb.WriteMode_WRITE_MODE_CREATE
	case config.WriteModeUpdate:
		writeMode = configpb.WriteMode_WRITE_MODE_UPDATE
	default:
		writeMode = configpb.WriteMode_WRITE_MODE_UPSERT
	}

	var result config.Value
	err = s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.Set(ctx, &configpb.SetRequest{
			Namespace: namespace,
			Key:       key,
			Value:     data,
			Codec:     value.Codec(),
			WriteMode: writeMode,
		})
		if err != nil {
			return fromGRPCError(err)
		}
		result = protoToValue(ctx, resp.Entry)
		return nil
	})
	return result, err
}

// Delete removes a configuration value by namespace and key.
func (s *RemoteStore) Delete(ctx context.Context, namespace, key string) error {
	return s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		_, err = client.Delete(ctx, &configpb.DeleteRequest{
			Namespace: namespace,
			Key:       key,
		})
		return fromGRPCError(err)
	})
}

// Find returns a page of keys and values matching the filter within a namespace.
func (s *RemoteStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	var result config.Page
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.List(ctx, &configpb.ListRequest{
			Namespace: namespace,
			Prefix:    filter.Prefix(),
			Limit:     int32(min(filter.Limit(), math.MaxInt32)), // #nosec G115 -- clamped
			Cursor:    filter.Cursor(),
		})
		if err != nil {
			return fromGRPCError(err)
		}

		results := make(map[string]config.Value, len(resp.Entries))
		for _, e := range resp.Entries {
			results[e.Key] = protoToValue(ctx, e)
		}

		result = config.NewPage(results, resp.NextCursor, filter.Limit())
		return nil
	})
	return result, err
}

// GetVersions retrieves version history for a configuration key.
// Implements config.VersionedStore.
func (s *RemoteStore) GetVersions(ctx context.Context, namespace, key string, filter config.VersionFilter) (config.VersionPage, error) {
	var result config.VersionPage
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		req := &configpb.GetVersionsRequest{
			Namespace: namespace,
			Key:       key,
		}
		if filter != nil {
			req.Version = filter.Version()
			req.Limit = int32(min(filter.Limit(), math.MaxInt32)) // #nosec G115 -- clamped
			req.Cursor = filter.Cursor()
		}

		resp, err := client.GetVersions(ctx, req)
		if err != nil {
			return fromGRPCError(err)
		}

		versions := make([]config.Value, 0, len(resp.Entries))
		for _, e := range resp.Entries {
			versions = append(versions, protoToValue(ctx, e))
		}

		result = config.NewVersionPage(versions, resp.NextCursor, int(resp.Limit))
		return nil
	})
	return result, err
}

// SnapshotResult contains the result of a Snapshot call.
type SnapshotResult struct {
	// Entries contains all configuration values in the namespace.
	Entries map[string]config.Value

	// ETag is an opaque version identifier for caching.
	ETag string

	// NotModified is true if the provided ETag matched (Entries will be empty).
	NotModified bool
}

// SnapshotOption configures a Snapshot call.
type SnapshotOption func(*snapshotOptions)

type snapshotOptions struct {
	ifNoneMatch string
}

// WithIfNoneMatch sets the ETag from a previous snapshot for conditional fetching.
func WithIfNoneMatch(etag string) SnapshotOption {
	return func(o *snapshotOptions) {
		o.ifNoneMatch = etag
	}
}

// Snapshot returns a point-in-time export of all entries in a namespace.
// Use WithIfNoneMatch to enable ETag-based caching.
func (s *RemoteStore) Snapshot(ctx context.Context, namespace string, opts ...SnapshotOption) (*SnapshotResult, error) {
	o := &snapshotOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var result *SnapshotResult
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.Snapshot(ctx, &configpb.SnapshotRequest{
			Namespace:   namespace,
			IfNoneMatch: o.ifNoneMatch,
		})
		if err != nil {
			return fromGRPCError(err)
		}

		entries := make(map[string]config.Value, len(resp.Entries))
		for _, e := range resp.Entries {
			entries[e.Key] = protoToValue(ctx, e)
		}

		result = &SnapshotResult{
			Entries:     entries,
			ETag:        resp.Etag,
			NotModified: resp.NotModified,
		}
		return nil
	})
	return result, err
}


// SetAlias creates a new alias on the remote server.
func (s *RemoteStore) SetAlias(ctx context.Context, alias, target string) (config.Value, error) {
	var result config.Value
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.SetAlias(ctx, &configpb.SetAliasRequest{
			Alias:  alias,
			Target: target,
		})
		if err != nil {
			return fromGRPCError(err)
		}
		result = aliasProtoToValue(resp.Alias)
		return nil
	})
	return result, err
}

// DeleteAlias removes an alias on the remote server.
func (s *RemoteStore) DeleteAlias(ctx context.Context, alias string) error {
	return s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		_, err = client.DeleteAlias(ctx, &configpb.DeleteAliasRequest{
			Alias: alias,
		})
		if err != nil {
			return fromGRPCError(err)
		}
		return nil
	})
}

// GetAlias retrieves a specific alias from the remote server.
func (s *RemoteStore) GetAlias(ctx context.Context, alias string) (config.Value, error) {
	var result config.Value
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.GetAlias(ctx, &configpb.GetAliasRequest{
			Alias: alias,
		})
		if err != nil {
			return fromGRPCError(err)
		}
		result = aliasProtoToValue(resp.Alias)
		return nil
	})
	return result, err
}

// ListAliases returns all aliases from the remote server.
func (s *RemoteStore) ListAliases(ctx context.Context) (map[string]config.Value, error) {
	var result map[string]config.Value
	err := s.retry(ctx, func(ctx context.Context) error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.ListAliases(ctx, &configpb.ListAliasesRequest{})
		if err != nil {
			return fromGRPCError(err)
		}

		result = make(map[string]config.Value, len(resp.Aliases))
		for _, a := range resp.Aliases {
			result[a.Alias] = aliasProtoToValue(a)
		}
		return nil
	})
	return result, err
}

// aliasProtoToValue converts a proto Alias to a config.Value wrapping the target string.
func aliasProtoToValue(a *configpb.Alias) config.Value {
	if a == nil {
		return nil
	}

	var opts []config.ValueOption
	if a.Version > 0 || a.CreatedAt != nil {
		var createdAt time.Time
		if a.CreatedAt != nil {
			createdAt = a.CreatedAt.AsTime()
		}
		opts = append(opts, config.WithValueMetadata(a.Version, createdAt, createdAt))
	}

	return config.NewValue(a.Target, opts...)
}

// protoToValue converts a proto Entry to a config.Value.
func protoToValue(ctx context.Context, entry *configpb.Entry) config.Value {
	if entry == nil {
		return nil
	}

	var opts []config.ValueOption

	// Set metadata if present
	if entry.Version > 0 || entry.CreatedAt != nil || entry.UpdatedAt != nil {
		var createdAt, updatedAt time.Time
		if entry.CreatedAt != nil {
			createdAt = entry.CreatedAt.AsTime()
		}
		if entry.UpdatedAt != nil {
			updatedAt = entry.UpdatedAt.AsTime()
		}
		opts = append(opts, config.WithValueMetadata(entry.Version, createdAt, updatedAt))
	}

	// Set type if present
	if entry.Type > 0 {
		opts = append(opts, config.WithValueType(config.Type(entry.Type)))
	}

	val, err := config.NewValueFromBytes(ctx, entry.Value, entry.Codec, opts...)
	if err != nil {
		// Fallback: create value without decoding
		return config.NewValue(entry.Value, opts...)
	}
	return val
}
