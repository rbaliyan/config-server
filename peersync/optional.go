package peersync

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config"
)

// GetMany retrieves multiple values from the owner of namespace. Routing and
// fallback behaviour mirrors Get. Returns an error if the target store does
// not implement config.BulkStore.
//
// Atomicity depends on the target store's BulkStore implementation; peersync
// does not add transactional semantics across keys.
func (s *SyncStore) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	store, err := s.ownerStore(namespace, false)
	if err != nil {
		return nil, err
	}
	bs, ok := store.(config.BulkStore)
	if !ok {
		return nil, fmt.Errorf("peersync: GetMany: store for namespace %q does not implement BulkStore", namespace)
	}
	return bs.GetMany(ctx, namespace, keys)
}

// SetMany writes multiple values to the owner of namespace. Routing and error
// behaviour mirrors Set. Returns an error if the target store does not
// implement config.BulkStore. Atomicity across keys depends on the target
// store's implementation; peersync does not add transactional semantics.
func (s *SyncStore) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	store, err := s.ownerStore(namespace, true)
	if err != nil {
		return err
	}
	bs, ok := store.(config.BulkStore)
	if !ok {
		return fmt.Errorf("peersync: SetMany: store for namespace %q does not implement BulkStore", namespace)
	}
	return bs.SetMany(ctx, namespace, values)
}

// DeleteMany removes multiple keys from the owner of namespace. Routing and
// error behaviour mirrors Delete. Returns an error if the target store does
// not implement config.BulkStore. Atomicity across keys depends on the target
// store's implementation; peersync does not add transactional semantics.
func (s *SyncStore) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	store, err := s.ownerStore(namespace, true)
	if err != nil {
		return 0, err
	}
	bs, ok := store.(config.BulkStore)
	if !ok {
		return 0, fmt.Errorf("peersync: DeleteMany: store for namespace %q does not implement BulkStore", namespace)
	}
	return bs.DeleteMany(ctx, namespace, keys)
}

// SetAlias creates a persistent key alias on the owner of the alias path.
// Routing and error behaviour mirrors Set. Returns an error if the target
// store does not implement config.AliasStore.
//
// Note: the alias path is hashed as a namespace to find the owner, so an
// alias and its target key may resolve to different owner nodes if their
// paths hash differently. Keep alias paths under the same namespace prefix
// as their targets to ensure co-location (see package doc "Alias routing").
func (s *SyncStore) SetAlias(ctx context.Context, alias, target string) (config.Value, error) {
	store, err := s.ownerStore(alias, true)
	if err != nil {
		return nil, err
	}
	as, ok := store.(config.AliasStore)
	if !ok {
		return nil, fmt.Errorf("peersync: SetAlias: store for alias %q does not implement AliasStore", alias)
	}
	return as.SetAlias(ctx, alias, target)
}

// DeleteAlias removes an alias on the owner of the alias path. Routing and
// error behaviour mirrors Delete. Returns an error if the target store does
// not implement config.AliasStore.
func (s *SyncStore) DeleteAlias(ctx context.Context, alias string) error {
	store, err := s.ownerStore(alias, true)
	if err != nil {
		return err
	}
	as, ok := store.(config.AliasStore)
	if !ok {
		return fmt.Errorf("peersync: DeleteAlias: store for alias %q does not implement AliasStore", alias)
	}
	return as.DeleteAlias(ctx, alias)
}

// GetAlias retrieves the target for an alias from the owner of the alias path.
// Routing and fallback behaviour mirrors Get. Returns an error if the target
// store does not implement config.AliasStore.
func (s *SyncStore) GetAlias(ctx context.Context, alias string) (config.Value, error) {
	store, err := s.ownerStore(alias, false)
	if err != nil {
		return nil, err
	}
	as, ok := store.(config.AliasStore)
	if !ok {
		return nil, fmt.Errorf("peersync: GetAlias: store for alias %q does not implement AliasStore", alias)
	}
	return as.GetAlias(ctx, alias)
}

// ListAliases returns all aliases registered on the local store. Because
// aliases are partitioned across nodes (each alias is owned by one node),
// cross-node aggregation would require fanning out to all peers. Call
// ListAliases on each node independently to get the full cluster view.
func (s *SyncStore) ListAliases(ctx context.Context) (map[string]config.Value, error) {
	as, ok := s.local.(config.AliasStore)
	if !ok {
		return nil, fmt.Errorf("peersync: ListAliases: local store does not implement AliasStore")
	}
	return as.ListAliases(ctx)
}

// GetVersions retrieves version history for a key from the owner of namespace.
// Routing and fallback behaviour mirrors Get. Returns an error if the target
// store does not implement config.VersionedStore.
func (s *SyncStore) GetVersions(ctx context.Context, namespace, key string, filter config.VersionFilter) (config.VersionPage, error) {
	store, err := s.ownerStore(namespace, false)
	if err != nil {
		return nil, err
	}
	vs, ok := store.(config.VersionedStore)
	if !ok {
		return nil, fmt.Errorf("peersync: GetVersions: store for namespace %q does not implement VersionedStore", namespace)
	}
	return vs.GetVersions(ctx, namespace, key, filter)
}

// Stats returns *config.StoreStats from the local store only. Because each
// node stores a disjoint slice of namespaces, cross-node aggregation would
// require fanning out to all peers. Call Stats on each node independently
// and sum the results if a cluster-wide view is needed. Returns an error if
// the local store does not implement config.StatsProvider.
func (s *SyncStore) Stats(ctx context.Context) (*config.StoreStats, error) {
	sp, ok := s.local.(config.StatsProvider)
	if !ok {
		return nil, fmt.Errorf("peersync: Stats: local store does not implement StatsProvider")
	}
	return sp.Stats(ctx)
}

var _ config.BulkStore = (*SyncStore)(nil)
var _ config.AliasStore = (*SyncStore)(nil)
var _ config.VersionedStore = (*SyncStore)(nil)
var _ config.StatsProvider = (*SyncStore)(nil)
