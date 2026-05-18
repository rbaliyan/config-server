package service

import "time"

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	guard              SecurityGuard
	auditor            Auditor
	maxSnapshotEntries int
	maxValueSize       int
	maxWatchFilters    int

	// namespaceStatsCacheTTL controls the lifetime of the cached
	// [config.StatsProvider] result used by [Service.ListNamespaces] when
	// the underlying store does not implement [config.NamespaceLister].
	// 0 disables the cache (every page recomputes stats); negative is
	// rejected by [WithNamespaceStatsCacheTTL].
	namespaceStatsCacheTTL time.Duration
}

// Option configures the Service.
type Option func(*serviceOptions)

// WithSecurityGuard sets the security guard for the service.
// If g is nil, the option is ignored and the default DenyAll guard is kept.
func WithSecurityGuard(g SecurityGuard) Option {
	return func(o *serviceOptions) {
		if g != nil {
			o.guard = g
		}
	}
}

// WithMaxSnapshotEntries sets the maximum number of entries a Snapshot call may
// return. If the namespace contains more entries, the RPC returns ResourceExhausted.
// Default: 10000.
func WithMaxSnapshotEntries(n int) Option {
	return func(o *serviceOptions) {
		if n > 0 {
			o.maxSnapshotEntries = n
		}
	}
}

// WithMaxWatchFilters sets the maximum number of namespaces and prefixes allowed
// in a single Watch request. Requests exceeding this limit are rejected with
// InvalidArgument. Default: 100.
func WithMaxWatchFilters(n int) Option {
	return func(o *serviceOptions) {
		if n > 0 {
			o.maxWatchFilters = n
		}
	}
}

// WithMaxValueSize sets the maximum allowed size in bytes for values passed to Set.
// Requests exceeding this limit are rejected with InvalidArgument.
// Default: 1 MiB (1 << 20).
func WithMaxValueSize(n int) Option {
	return func(o *serviceOptions) {
		if n > 0 {
			o.maxValueSize = n
		}
	}
}

// WithAuditor sets the Auditor used to record config mutation events.
// If a is nil, the option is ignored and auditing remains disabled.
func WithAuditor(a Auditor) Option {
	return func(o *serviceOptions) {
		if a != nil {
			o.auditor = a
		}
	}
}

// WithNamespaceStatsCacheTTL sets the lifetime of the in-memory cache that
// fronts the [config.StatsProvider] fallback path of [Service.ListNamespaces].
//
// Stores that do not implement [config.NamespaceLister] fall back to
// `Stats(ctx)`, which recomputes the entire namespace set on every page
// request. Without the cache, paginating N namespaces at P entries per
// page costs ⌈N/P⌉ full-store scans for one logical enumeration. The
// cache collapses those calls into a single Stats fetch per TTL window.
//
// Concurrent paginating clients are deduplicated via singleflight, so a
// burst of requests during a cache miss issues exactly one upstream call.
//
// Default: 30 seconds. Pass 0 to disable caching (every page recomputes
// stats). Negative values are ignored.
//
// This option only affects backends that go through the StatsProvider
// fallback; stores that implement NamespaceLister natively (memory,
// sqlite, postgres, mongodb in config v0.7.2+) bypass the cache entirely.
func WithNamespaceStatsCacheTTL(d time.Duration) Option {
	return func(o *serviceOptions) {
		if d >= 0 {
			o.namespaceStatsCacheTTL = d
		}
	}
}
