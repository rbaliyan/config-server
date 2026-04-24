package peersync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/client"
)

// GRPCDialer implements PeerDialer by opening gRPC connections to peer nodes.
// Connections are cached by address; a second Dial for the same address returns
// the cached store without opening a new connection. Close shuts down all cached
// connections.
type GRPCDialer struct {
	opts   []client.Option
	mu     sync.Mutex
	conns  map[string]*client.RemoteStore
	closed bool
}

var _ PeerDialer = (*GRPCDialer)(nil)

// NewGRPCDialer creates a GRPCDialer. The provided client options (e.g.
// client.WithTLS, client.WithRetry) are applied to every connection opened by
// Dial.
func NewGRPCDialer(opts ...client.Option) *GRPCDialer {
	return &GRPCDialer{
		opts:  opts,
		conns: make(map[string]*client.RemoteStore),
	}
}

// Dial returns a config.Store backed by a gRPC connection to addr. The
// connection is cached; subsequent Dial calls for the same addr return the same
// store. The returned store is already Connect()-ed; callers should not call
// Connect again. grpc.NewClient is lazy, so dialing does not perform a round
// trip — a peer whose address has just been announced via gossip can be dialed
// before it is actually reachable without triggering an immediate failure.
func (d *GRPCDialer) Dial(addr string) (config.Store, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, errors.New("peersync: GRPCDialer is closed")
	}
	if store, ok := d.conns[addr]; ok {
		return store, nil
	}
	store, err := client.NewRemoteStore(addr, d.opts...)
	if err != nil {
		return nil, fmt.Errorf("peersync: dial %s: %w", addr, err)
	}
	if err := store.Connect(context.Background()); err != nil {
		_ = store.Close(context.Background())
		return nil, fmt.Errorf("peersync: connect %s: %w", addr, err)
	}
	d.conns[addr] = store
	return store, nil
}

// Close closes all cached connections. Subsequent Dial calls return an error.
// Errors from individual connections are joined and returned together.
func (d *GRPCDialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	var errs []error
	for addr, store := range d.conns {
		if err := store.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", addr, err))
		}
	}
	d.conns = nil
	return errors.Join(errs...)
}
