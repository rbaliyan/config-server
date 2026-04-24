package client

import (
	"context"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

// WatchResult wraps a change event channel with error reporting and control.
// Safe for concurrent use by multiple goroutines.
type WatchResult struct {
	events  <-chan config.ChangeEvent
	errCh   <-chan error
	doneCh  <-chan struct{}
	cancel  context.CancelFunc
	errOnce sync.Once
	err     error
}

// Events returns the channel that receives configuration change events. The
// channel is closed when the watch ends (cancelled or errored).
func (w *WatchResult) Events() <-chan config.ChangeEvent { return w.events }

// Err returns the error that caused the watch to end, or nil if it was
// cancelled normally. Err blocks until the watch goroutine has fully exited.
// Safe to call multiple times; all calls return the same error.
func (w *WatchResult) Err() error {
	w.errOnce.Do(func() {
		<-w.doneCh
		select {
		case w.err = <-w.errCh:
		default:
		}
	})
	return w.err
}

// Stop cancels the watch. Safe to call multiple times.
func (w *WatchResult) Stop() { w.cancel() }

// Watch returns a channel that receives change events.
// The returned channel is closed when the context is cancelled or an error occurs.
// For better error visibility and control, use WatchWithResult.
func (s *RemoteStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	result, err := s.WatchWithResult(ctx, filter)
	if err != nil {
		return nil, err
	}
	return result.Events(), nil
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

	watchCtx, cancel := context.WithCancel(ctx)

	ch := make(chan config.ChangeEvent, s.opts.watchBufferSize)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	go s.watchLoop(watchCtx, client, filter, ch, errCh, doneCh)

	return &WatchResult{
		events: ch,
		errCh:  errCh,
		doneCh: doneCh,
		cancel: cancel,
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

	backoff := s.opts.watchReconnectWait
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

		connected, err := s.watchStream(ctx, client, filter, ch)

		if err == nil || ctx.Err() != nil {
			return
		}

		// The failure happened mid-stream, so reset retry accounting.
		if connected {
			consecutiveErrors = 0
			backoff = s.opts.watchReconnectWait
		}

		if s.opts.onWatchError != nil {
			s.opts.onWatchError(err)
		}

		if !s.opts.watchReconnect {
			errCh <- err
			return
		}

		consecutiveErrors++
		if consecutiveErrors > s.opts.watchMaxErrors {
			errCh <- err
			return
		}

		// Wait before reconnecting with exponential backoff.
		jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64())) // #nosec G404 -- jitter does not require cryptographic randomness
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

		newClient, err := s.getClient()
		if err != nil {
			continue
		}
		client = newClient
	}
}

func (s *RemoteStore) watchStream(
	ctx context.Context,
	client configpb.ConfigServiceClient,
	filter config.WatchFilter,
	ch chan<- config.ChangeEvent,
) (connected bool, _ error) {
	stream, err := client.Watch(ctx, &configpb.WatchRequest{
		Namespaces: filter.Namespaces,
		Prefixes:   filter.Prefixes,
	})
	if err != nil {
		return false, fromGRPCError(err)
	}
	connected = true

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return connected, nil
			}
			if ctx.Err() != nil {
				return connected, nil
			}
			return connected, fromGRPCError(err)
		}

		if resp.Entry == nil {
			continue
		}

		event := config.ChangeEvent{
			Namespace: resp.Entry.Namespace,
			Key:       resp.Entry.Key,
		}
		switch resp.Type {
		case configpb.ChangeType_CHANGE_TYPE_SET:
			event.Type = config.ChangeTypeSet
			event.Value = protoToValue(ctx, resp.Entry)
		case configpb.ChangeType_CHANGE_TYPE_DELETE:
			event.Type = config.ChangeTypeDelete
		case configpb.ChangeType_CHANGE_TYPE_ALIAS_SET:
			event.Type = config.ChangeTypeAliasSet
			event.Value = protoToValue(ctx, resp.Entry)
		case configpb.ChangeType_CHANGE_TYPE_ALIAS_DELETE:
			event.Type = config.ChangeTypeAliasDelete
		default:
			continue
		}

		select {
		case ch <- event:
		case <-ctx.Done():
			return connected, nil
		case <-s.closeCh:
			return connected, config.ErrStoreClosed
		}
	}
}
