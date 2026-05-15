package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rbaliyan/config"
	"golang.org/x/sync/singleflight"
)

// defaultNamespaceStatsCacheTTL is the cache window used by the
// [config.StatsProvider] fallback path of [Service.ListNamespaces] when
// the caller did not override it via [WithNamespaceStatsCacheTTL].
//
// 30s is a deliberate trade-off: long enough that a typical operator
// paginating a large namespace list at 50/page does not re-scan the
// store per page, short enough that newly-created namespaces appear in
// listings within a refresh cycle. Operators with strict freshness
// requirements can pass `WithNamespaceStatsCacheTTL(0)` to disable
// caching entirely.
const defaultNamespaceStatsCacheTTL = 30 * time.Second

// namespaceStatsCache memoises the result of [config.StatsProvider.Stats]
// for [Service.ListNamespaces]'s fallback path. Concurrent paginating
// clients are deduplicated via [singleflight] so a cache miss issues at
// most one upstream call regardless of fan-in.
//
// The cache stores both successful results and errors. Caching errors is
// deliberate: when Stats fails (e.g. because the backing database is
// unreachable), thundering-herd retries from every paginating client
// would amplify the outage. The cached error expires on the same TTL as
// successful values, so service recovery is bounded.
//
// [config.StoreStats] is immutable by interface (config v0.8.0+), so a
// snapshot held here can be observed by any number of goroutines
// without defensive copying.
//
// The zero value is not safe to use; build one with
// [newNamespaceStatsCache].
type namespaceStatsCache struct {
	ttl time.Duration

	mu      sync.Mutex
	val     config.StoreStats // last successful result, nil until first fetch
	err     error             // last error, nil if val is set and fresh
	fetchAt time.Time         // wall-clock time of the last upstream call

	sf singleflight.Group
}

// newNamespaceStatsCache builds a cache with the given TTL. A TTL of 0
// disables caching at the lookup level — every call goes straight to the
// provider (singleflight still collapses concurrent in-flight calls but
// the result is never reused across calls).
func newNamespaceStatsCache(ttl time.Duration) *namespaceStatsCache {
	if ttl < 0 {
		ttl = 0
	}
	return &namespaceStatsCache{ttl: ttl}
}

// Get returns the cached stats if they are still within TTL; otherwise
// it issues a single upstream call (collapsed across concurrent callers
// via singleflight) and caches the result for future lookups inside the
// same TTL window. Errors are cached on the same schedule as successful
// values; see the [namespaceStatsCache] godoc for the rationale.
//
// The returned [config.StoreStats] is the immutable interface value
// (config v0.8.0+) so callers cannot accidentally mutate the shared
// snapshot; no defensive copy is needed.
func (c *namespaceStatsCache) Get(ctx context.Context, sp config.StatsProvider) (config.StoreStats, error) {
	if c.ttl > 0 {
		c.mu.Lock()
		if c.fresh() {
			v, e := c.val, c.err
			c.mu.Unlock()
			return v, e
		}
		c.mu.Unlock()
	}

	// singleflight key is constant because the cache is per-store-per-
	// service; every Get on this cache is asking for the same payload.
	res, err, _ := c.sf.Do("stats", func() (any, error) {
		// Re-check under the lock — a concurrent caller may have
		// populated the cache while we waited for the flight slot.
		if c.ttl > 0 {
			c.mu.Lock()
			if c.fresh() {
				v, e := c.val, c.err
				c.mu.Unlock()
				return v, e
			}
			c.mu.Unlock()
		}

		stats, fetchErr := sp.Stats(ctx)

		// Store the result regardless of TTL. With ttl == 0 the next Get
		// will skip the fast path and re-fetch, but caching here still
		// lets a concurrent burst share the same in-flight result.
		c.mu.Lock()
		c.val = stats
		c.err = fetchErr
		c.fetchAt = time.Now()
		c.mu.Unlock()
		return stats, fetchErr
	})
	if err != nil {
		return nil, err
	}
	// res may be nil when sp.Stats returned (nil, nil); pass through.
	stats, _ := res.(config.StoreStats)
	return stats, nil
}

// fresh reports whether the cached value is within TTL. Must be called
// with the mutex held.
func (c *namespaceStatsCache) fresh() bool {
	if c.ttl == 0 || c.fetchAt.IsZero() {
		return false
	}
	return time.Since(c.fetchAt) < c.ttl
}

// fallbackDeprecationLogger emits a one-shot warning when a Service's
// [Service.ListNamespaces] takes the [config.StatsProvider] fallback path
// for the first time. The warning is intentionally a `slog.Warn` rather
// than an error: the fallback still functions correctly, but it carries
// O(namespaces)-per-page cost that the native [config.NamespaceLister]
// implementations avoid. Operators see the message once per process and
// can plan a backend upgrade.
//
// Future major versions of config-server may remove the fallback
// entirely; the warning is the early-warning signal.
type fallbackDeprecationLogger struct {
	once sync.Once
}

// logOnce emits the warning if it has not been emitted yet by this
// logger. Subsequent calls within the same Service lifetime are no-ops.
// The store-type label uses %T so operators can tell at a glance which
// backend they need to upgrade.
func (f *fallbackDeprecationLogger) logOnce(store config.Store) {
	f.once.Do(func() {
		slog.Warn(
			"ListNamespaces is using the StatsProvider fallback path; this is O(namespaces) per page and will be removed in a future major version. Upgrade the store to one that implements config.NamespaceLister.",
			"store_type", fmt.Sprintf("%T", store),
		)
	})
}
