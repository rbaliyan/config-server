// Package client provides a RemoteStore implementation that connects to a config server.
package client

import (
	"context"
	"io"
	"math/rand"
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

	mu     sync.RWMutex
	conn   *grpc.ClientConn
	client configpb.ConfigServiceClient
	state  atomic.Int32 // ConnState

	// Circuit breaker state
	circuitMu      sync.Mutex
	circuitOpen    bool
	circuitOpenAt  time.Time
	consecutiveFail int

	// Shutdown
	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewRemoteStore creates a new RemoteStore connecting to the given address.
func NewRemoteStore(addr string, opts ...Option) (*RemoteStore, error) {
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

// Compile-time interface check
var _ config.Store = (*RemoteStore)(nil)

// State returns the current connection state.
func (s *RemoteStore) State() ConnState {
	return ConnState(s.state.Load())
}

func (s *RemoteStore) setState(state ConnState) {
	old := ConnState(s.state.Swap(int32(state)))
	if old != state && s.opts.onStateChange != nil {
		s.opts.onStateChange(state)
	}
}

// Connect establishes the connection to the config server.
// It respects the context deadline and configured timeout.
func (s *RemoteStore) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State() == ConnStateClosed {
		return config.ErrStoreClosed
	}

	// Close existing connection if any
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
		s.client = nil
	}

	s.setState(ConnStateConnecting)

	// Apply connect timeout
	connectCtx, cancel := context.WithTimeout(ctx, s.opts.connectTimeout)
	defer cancel()

	// Build dial options
	dialOpts := s.opts.buildDialOpts()

	conn, err := grpc.DialContext(connectCtx, s.addr, dialOpts...)
	if err != nil {
		s.setState(ConnStateDisconnected)
		return err
	}

	s.conn = conn
	s.client = configpb.NewConfigServiceClient(conn)
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

// Circuit breaker methods
func (s *RemoteStore) isCircuitOpen() bool {
	if !s.opts.enableCircuit {
		return false
	}

	s.circuitMu.Lock()
	defer s.circuitMu.Unlock()

	if !s.circuitOpen {
		return false
	}

	// Check if circuit should be half-open (allow retry)
	if time.Since(s.circuitOpenAt) > s.opts.circuitTimeout {
		s.circuitOpen = false
		return false
	}

	return true
}

func (s *RemoteStore) recordSuccess() {
	if !s.opts.enableCircuit {
		return
	}
	s.circuitMu.Lock()
	s.consecutiveFail = 0
	s.circuitMu.Unlock()
}

func (s *RemoteStore) recordFailure() {
	if !s.opts.enableCircuit {
		return
	}
	s.circuitMu.Lock()
	s.consecutiveFail++
	if s.consecutiveFail >= s.opts.circuitThreshold {
		s.circuitOpen = true
		s.circuitOpenAt = time.Now()
	}
	s.circuitMu.Unlock()
}

func (s *RemoteStore) resetCircuit() {
	s.circuitMu.Lock()
	s.circuitOpen = false
	s.consecutiveFail = 0
	s.circuitMu.Unlock()
}

// retry executes fn with retries and exponential backoff.
func (s *RemoteStore) retry(ctx context.Context, fn func() error) error {
	var lastErr error
	backoff := s.opts.retryBackoff

	for attempt := 0; attempt <= s.opts.maxRetries; attempt++ {
		if attempt > 0 {
			// Add jitter: 0.5x to 1.5x
			jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()))
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

		lastErr = fn()
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
	switch err {
	case config.ErrNotFound, config.ErrKeyExists, config.ErrInvalidKey,
		config.ErrInvalidNamespace, config.ErrInvalidValue, config.ErrReadOnly:
		return true
	}
	// Don't retry permission errors
	if _, ok := err.(*PermissionDeniedError); ok {
		return true
	}
	return false
}

// Get retrieves a configuration value by namespace and key.
func (s *RemoteStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	var result config.Value
	err := s.retry(ctx, func() error {
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
		result = protoToValue(resp.Entry)
		return nil
	})
	return result, err
}

// Set creates or updates a configuration value.
func (s *RemoteStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	data, err := value.Marshal()
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
	err = s.retry(ctx, func() error {
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
		result = protoToValue(resp.Entry)
		return nil
	})
	return result, err
}

// Delete removes a configuration value by namespace and key.
func (s *RemoteStore) Delete(ctx context.Context, namespace, key string) error {
	return s.retry(ctx, func() error {
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
	err := s.retry(ctx, func() error {
		client, err := s.getClient()
		if err != nil {
			return err
		}

		resp, err := client.List(ctx, &configpb.ListRequest{
			Namespace: namespace,
			Prefix:    filter.Prefix(),
			Limit:     int32(filter.Limit()),
			Cursor:    filter.Cursor(),
		})
		if err != nil {
			return fromGRPCError(err)
		}

		results := make(map[string]config.Value, len(resp.Entries))
		for _, e := range resp.Entries {
			results[e.Key] = protoToValue(e)
		}

		result = config.NewPage(results, resp.NextCursor, filter.Limit())
		return nil
	})
	return result, err
}

// WatchResult wraps a change event channel with error reporting and control.
type WatchResult struct {
	// Events receives configuration change events.
	// The channel is closed when the watch ends.
	Events <-chan config.ChangeEvent

	// Err returns the error that caused the watch to end, or nil if cancelled.
	// This method blocks until the watch goroutine exits.
	Err func() error

	// Stop cancels the watch. Safe to call multiple times.
	Stop func()
}

// Watch returns a channel that receives change events.
// The returned channel is closed when the context is cancelled or an error occurs.
// For better error visibility and control, use WatchWithResult.
func (s *RemoteStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	result, err := s.WatchWithResult(ctx, filter)
	if err != nil {
		return nil, err
	}
	return result.Events, nil
}

// WatchWithResult returns a WatchResult providing the event channel,
// error access, and stop control.
//
// If reconnection is enabled (default), the watch will automatically
// reconnect on network errors with exponential backoff.
func (s *RemoteStore) WatchWithResult(ctx context.Context, filter config.WatchFilter) (*WatchResult, error) {
	client, err := s.getClient()
	if err != nil {
		return nil, err
	}

	// Create cancellable context for this watch
	watchCtx, cancel := context.WithCancel(ctx)

	ch := make(chan config.ChangeEvent, s.opts.watchBufferSize)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	go s.watchLoop(watchCtx, client, filter, ch, errCh, doneCh)

	return &WatchResult{
		Events: ch,
		Err: func() error {
			<-doneCh // Wait for goroutine to finish
			select {
			case err := <-errCh:
				return err
			default:
				return nil
			}
		},
		Stop: func() {
			cancel()
		},
	}, nil
}

func (s *RemoteStore) watchLoop(
	ctx context.Context,
	client configpb.ConfigServiceClient,
	filter config.WatchFilter,
	ch chan<- config.ChangeEvent,
	errCh chan<- error,
	doneCh chan<- struct{},
) {
	defer close(ch)
	defer close(doneCh)

	backoff := s.opts.retryBackoff
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closeCh:
			errCh <- config.ErrStoreClosed
			return
		default:
		}

		err := s.watchStream(ctx, client, filter, ch)

		if err == nil || ctx.Err() != nil {
			// Normal exit or context cancelled
			return
		}

		// Report error via callback if configured
		if s.opts.onWatchError != nil {
			s.opts.onWatchError(err)
		}

		// Check if reconnection is enabled
		if !s.opts.watchReconnect {
			errCh <- err
			return
		}

		consecutiveErrors++
		if consecutiveErrors > s.opts.watchMaxErrors {
			// Too many errors, give up
			errCh <- err
			return
		}

		// Wait before reconnecting with exponential backoff
		jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()))
		select {
		case <-ctx.Done():
			return
		case <-s.closeCh:
			errCh <- config.ErrStoreClosed
			return
		case <-time.After(jitter):
		}

		backoff *= 2
		if backoff > s.opts.maxBackoff {
			backoff = s.opts.maxBackoff
		}

		// Refresh client in case of reconnection
		newClient, err := s.getClient()
		if err != nil {
			continue // Will retry
		}
		client = newClient
	}
}

func (s *RemoteStore) watchStream(
	ctx context.Context,
	client configpb.ConfigServiceClient,
	filter config.WatchFilter,
	ch chan<- config.ChangeEvent,
) error {
	stream, err := client.Watch(ctx, &configpb.WatchRequest{
		Namespaces: filter.Namespaces,
		Prefixes:   filter.Prefixes,
	})
	if err != nil {
		return fromGRPCError(err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil // Normal close
			}
			if ctx.Err() != nil {
				return nil // Context cancelled
			}
			return fromGRPCError(err)
		}

		event := config.ChangeEvent{
			Namespace: resp.Entry.Namespace,
			Key:       resp.Entry.Key,
		}
		switch resp.Type {
		case configpb.ChangeType_CHANGE_TYPE_SET:
			event.Type = config.ChangeTypeSet
			event.Value = protoToValue(resp.Entry)
		case configpb.ChangeType_CHANGE_TYPE_DELETE:
			event.Type = config.ChangeTypeDelete
		}

		// Send with backpressure awareness
		select {
		case ch <- event:
			// Sent successfully
		case <-ctx.Done():
			return nil
		case <-s.closeCh:
			return config.ErrStoreClosed
		}
	}
}

// protoToValue converts a proto Entry to a config.Value.
func protoToValue(entry *configpb.Entry) config.Value {
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

	val, err := config.NewValueFromBytes(entry.Value, entry.Codec, opts...)
	if err != nil {
		// Fallback: create value without decoding
		return config.NewValue(entry.Value, opts...)
	}
	return val
}
