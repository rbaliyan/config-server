package service

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	guard              SecurityGuard
	maxSnapshotEntries int
	maxValueSize       int
	maxWatchFilters    int
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
