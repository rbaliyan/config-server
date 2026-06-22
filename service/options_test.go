package service

import (
	"testing"
	"time"
)

// TestWithOptionSetters drives each service Option against a fresh, zeroed
// serviceOptions value (this is an internal test in package service, so it can
// construct the unexported struct directly) and asserts the observable field
// effect, including the clamping behavior of the guarded setters.
func TestWithOptionSetters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		opt    Option
		assert func(t *testing.T, o *serviceOptions)
	}{
		{
			name: "WithMaxSnapshotEntries positive",
			opt:  WithMaxSnapshotEntries(42),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxSnapshotEntries != 42 {
					t.Errorf("maxSnapshotEntries = %d, want 42", o.maxSnapshotEntries)
				}
			},
		},
		{
			name: "WithMaxSnapshotEntries zero is ignored",
			opt:  WithMaxSnapshotEntries(0),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxSnapshotEntries != 0 {
					t.Errorf("maxSnapshotEntries = %d, want 0 (unchanged)", o.maxSnapshotEntries)
				}
			},
		},
		{
			name: "WithMaxSnapshotEntries negative is ignored",
			opt:  WithMaxSnapshotEntries(-5),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxSnapshotEntries != 0 {
					t.Errorf("maxSnapshotEntries = %d, want 0 (unchanged)", o.maxSnapshotEntries)
				}
			},
		},
		{
			name: "WithMaxWatchFilters positive",
			opt:  WithMaxWatchFilters(7),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxWatchFilters != 7 {
					t.Errorf("maxWatchFilters = %d, want 7", o.maxWatchFilters)
				}
			},
		},
		{
			name: "WithMaxWatchFilters zero is ignored",
			opt:  WithMaxWatchFilters(0),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxWatchFilters != 0 {
					t.Errorf("maxWatchFilters = %d, want 0 (unchanged)", o.maxWatchFilters)
				}
			},
		},
		{
			name: "WithMaxWatchFilters negative is ignored",
			opt:  WithMaxWatchFilters(-1),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxWatchFilters != 0 {
					t.Errorf("maxWatchFilters = %d, want 0 (unchanged)", o.maxWatchFilters)
				}
			},
		},
		{
			name: "WithMaxValueSize positive",
			opt:  WithMaxValueSize(2048),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxValueSize != 2048 {
					t.Errorf("maxValueSize = %d, want 2048", o.maxValueSize)
				}
			},
		},
		{
			name: "WithMaxValueSize zero is ignored",
			opt:  WithMaxValueSize(0),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxValueSize != 0 {
					t.Errorf("maxValueSize = %d, want 0 (unchanged)", o.maxValueSize)
				}
			},
		},
		{
			name: "WithMaxValueSize negative is ignored",
			opt:  WithMaxValueSize(-100),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.maxValueSize != 0 {
					t.Errorf("maxValueSize = %d, want 0 (unchanged)", o.maxValueSize)
				}
			},
		},
		{
			name: "WithNamespaceStatsCacheTTL positive",
			opt:  WithNamespaceStatsCacheTTL(15 * time.Second),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.namespaceStatsCacheTTL != 15*time.Second {
					t.Errorf("namespaceStatsCacheTTL = %v, want 15s", o.namespaceStatsCacheTTL)
				}
			},
		},
		{
			name: "WithNamespaceStatsCacheTTL zero disables (accepted)",
			opt:  WithNamespaceStatsCacheTTL(0),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.namespaceStatsCacheTTL != 0 {
					t.Errorf("namespaceStatsCacheTTL = %v, want 0", o.namespaceStatsCacheTTL)
				}
			},
		},
		{
			name: "WithNamespaceStatsCacheTTL negative is ignored",
			opt:  WithNamespaceStatsCacheTTL(-1 * time.Second),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.namespaceStatsCacheTTL != 0 {
					t.Errorf("namespaceStatsCacheTTL = %v, want 0 (unchanged)", o.namespaceStatsCacheTTL)
				}
			},
		},
		{
			name: "WithSecurityGuard sets guard",
			opt:  WithSecurityGuard(AllowAll()),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.guard == nil {
					t.Error("guard = nil, want non-nil after WithSecurityGuard(AllowAll())")
				}
			},
		},
		{
			name: "WithSecurityGuard nil is ignored",
			opt:  WithSecurityGuard(nil),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.guard != nil {
					t.Error("guard != nil, want nil (unchanged) after WithSecurityGuard(nil)")
				}
			},
		},
		{
			name: "WithAuditor nil is ignored",
			opt:  WithAuditor(nil),
			assert: func(t *testing.T, o *serviceOptions) {
				if o.auditor != nil {
					t.Error("auditor != nil, want nil (unchanged) after WithAuditor(nil)")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := &serviceOptions{}
			tt.opt(o)
			tt.assert(t, o)
		})
	}
}
