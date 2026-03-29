package service

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	authorizer         Authorizer
	maxSnapshotEntries int
	maxValueSize       int
	maxWatchFilters    int
}

// Option configures the Service.
type Option func(*serviceOptions)

// WithAuthorizer sets the authorizer for the service.
// If a is nil, the option is ignored and the default DenyAll authorizer is kept.
func WithAuthorizer(a Authorizer) Option {
	return func(o *serviceOptions) {
		if a != nil {
			o.authorizer = a
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
