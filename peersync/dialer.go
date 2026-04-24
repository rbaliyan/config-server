package peersync

import "github.com/rbaliyan/config"

// PeerDialer resolves a peer node address to a config.Store for operation
// forwarding. Implementations should cache and reuse connections.
//
// A typical implementation caches one *client.RemoteStore per address and
// returns the cached instance on subsequent Dial calls for the same address.
// RemoteStore (from config-server/client) implements config.Store and is the
// natural return value; PeerDialer itself is the caching wrapper around it.
type PeerDialer interface {
	Dial(addr string) (config.Store, error)
}
