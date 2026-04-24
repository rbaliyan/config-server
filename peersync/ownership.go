package peersync

import (
	"context"
	"errors"
)

// ErrNamespaceReadOnly is returned by write operations when the namespace's
// recorded owner is currently unreachable. The namespace remains readable;
// call Claim to transfer ownership to a live node before writing.
var ErrNamespaceReadOnly = errors.New("peersync: namespace is read-only (owner unreachable)")

// OwnershipStore persists namespace-to-owner mappings so that ownership
// survives node restarts. SyncStore reads owned namespaces on Connect and
// writes them on Claim/Unclaim. Any durable store may implement this interface
// (e.g. a table in the same SQLite/PostgreSQL database that backs the node).
type OwnershipStore interface {
	// LoadOwned returns all namespaces owned by nodeID. Returns an empty slice
	// (nil or non-nil) when nodeID owns nothing. Order is not significant.
	LoadOwned(ctx context.Context, nodeID string) ([]string, error)

	// SaveOwner records nodeID as the owner of namespace, replacing any
	// previous owner record.
	SaveOwner(ctx context.Context, namespace, nodeID string) error

	// DeleteOwner removes the ownership record for namespace so that
	// hash-ring routing resumes.
	DeleteOwner(ctx context.Context, namespace string) error
}
