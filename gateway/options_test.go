package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rbaliyan/config-server/dashboard"
)

// TestOptionSetters verifies the gateway Option functions mutate the
// (unexported) options struct as documented, including the edge cases called
// out in their godoc: WithDashboardPath ignores empty / non-"/"-prefixed
// values, and WithEventBufferSize ignores negatives while distinguishing an
// explicit *0 (disable) from *N (positive size) and nil (unset → default).
func TestOptionSetters(t *testing.T) {
	t.Parallel()

	// A non-nil DashboardAuth built-in used to assert WithDashboardAuth stores
	// exactly what it is given.
	auth := dashboard.BearerTokenAuth(nil)

	tests := []struct {
		name   string
		opt    Option
		assert func(t *testing.T, o *options)
	}{
		{
			name: "WithDashboard enables dashboard",
			opt:  WithDashboard(),
			assert: func(t *testing.T, o *options) {
				if !o.dashboardEnabled {
					t.Errorf("dashboardEnabled = false, want true")
				}
			},
		},
		{
			name: "WithDashboardPath valid path",
			opt:  WithDashboardPath("/admin"),
			assert: func(t *testing.T, o *options) {
				if o.dashboardPath != "/admin" {
					t.Errorf("dashboardPath = %q, want /admin", o.dashboardPath)
				}
			},
		},
		{
			name: "WithDashboardPath empty ignored",
			opt:  WithDashboardPath(""),
			assert: func(t *testing.T, o *options) {
				if o.dashboardPath != "" {
					t.Errorf("dashboardPath = %q, want \"\" (unchanged)", o.dashboardPath)
				}
			},
		},
		{
			name: "WithDashboardPath non-slash-prefixed ignored",
			opt:  WithDashboardPath("admin"),
			assert: func(t *testing.T, o *options) {
				if o.dashboardPath != "" {
					t.Errorf("dashboardPath = %q, want \"\" (unchanged)", o.dashboardPath)
				}
			},
		},
		{
			name: "WithDashboardAuth stores auth",
			opt:  WithDashboardAuth(auth),
			assert: func(t *testing.T, o *options) {
				if o.dashboardAuth == nil {
					t.Fatalf("dashboardAuth = nil, want non-nil")
				}
				if o.dashboardAuth != auth {
					t.Errorf("dashboardAuth = %v, want the provided auth", o.dashboardAuth)
				}
			},
		},
		{
			name: "WithDashboardAuth nil stays nil",
			opt:  WithDashboardAuth(nil),
			assert: func(t *testing.T, o *options) {
				if o.dashboardAuth != nil {
					t.Errorf("dashboardAuth = %v, want nil", o.dashboardAuth)
				}
			},
		},
		{
			name: "WithEventBufferSize positive",
			opt:  WithEventBufferSize(42),
			assert: func(t *testing.T, o *options) {
				if o.eventBufferSize == nil {
					t.Fatalf("eventBufferSize = nil, want *42")
				}
				if *o.eventBufferSize != 42 {
					t.Errorf("*eventBufferSize = %d, want 42", *o.eventBufferSize)
				}
				if got := resolveEventBufferSize(o.eventBufferSize); got != 42 {
					t.Errorf("resolveEventBufferSize = %d, want 42", got)
				}
			},
		},
		{
			name: "WithEventBufferSize zero disables (explicit *0)",
			opt:  WithEventBufferSize(0),
			assert: func(t *testing.T, o *options) {
				if o.eventBufferSize == nil {
					t.Fatalf("eventBufferSize = nil, want *0 (explicit disable)")
				}
				if *o.eventBufferSize != 0 {
					t.Errorf("*eventBufferSize = %d, want 0", *o.eventBufferSize)
				}
				if got := resolveEventBufferSize(o.eventBufferSize); got != 0 {
					t.Errorf("resolveEventBufferSize = %d, want 0 (disabled)", got)
				}
			},
		},
		{
			name: "WithEventBufferSize negative ignored (stays nil)",
			opt:  WithEventBufferSize(-1),
			assert: func(t *testing.T, o *options) {
				if o.eventBufferSize != nil {
					t.Errorf("eventBufferSize = %v, want nil (negative ignored)", o.eventBufferSize)
				}
				if got := resolveEventBufferSize(o.eventBufferSize); got != defaultEventBufferSize {
					t.Errorf("resolveEventBufferSize = %d, want default %d", got, defaultEventBufferSize)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			o := &options{}
			tt.opt(o)
			tt.assert(t, o)
		})
	}
}

// TestResolveEventBufferSize_Semantics pins the nil/*0/*N contract of
// resolveEventBufferSize independently of the Option setters.
func TestResolveEventBufferSize_Semantics(t *testing.T) {
	t.Parallel()

	if got := resolveEventBufferSize(nil); got != defaultEventBufferSize {
		t.Errorf("resolveEventBufferSize(nil) = %d, want default %d", got, defaultEventBufferSize)
	}
	zero := 0
	if got := resolveEventBufferSize(&zero); got != 0 {
		t.Errorf("resolveEventBufferSize(*0) = %d, want 0", got)
	}
	n := 250
	if got := resolveEventBufferSize(&n); got != 250 {
		t.Errorf("resolveEventBufferSize(*250) = %d, want 250", got)
	}
}

// TestWithDashboardAuth_Wired confirms the stored auth actually drives the
// dashboard handler's access control via dashHandler (the only observable
// effect of WithDashboardAuth beyond the field assignment).
func TestWithDashboardAuth_Wired(t *testing.T) {
	t.Parallel()

	o := &options{}
	WithDashboard()(o)
	WithDashboardAuth(dashboard.BearerTokenAuth(nil))(o)
	WithDashboardPath("/dashboard")(o)

	h := o.dashHandler()
	if h == nil {
		t.Fatal("dashHandler() = nil, want a handler when dashboard enabled")
	}

	// A request without a bearer token must be rejected by the auth middleware
	// that WithDashboardAuth wires into the dashboard handler.
	req := httptest.NewRequest(http.MethodGet, "/dashboard/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (auth middleware should reject missing token)", rec.Code)
	}
}
