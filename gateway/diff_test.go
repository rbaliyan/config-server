package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
)

// newDiffTestServer wires the in-process diff handler onto a ServeMux using the
// same path pattern composeHandlers uses, so r.PathValue("namespace"/"key")
// resolve. The memory store is versioned, so two Sets create v1 and v2.
func newDiffTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	store := memory.NewStore()
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { store.Close(ctx) })

	// Seed two versions of one key.
	if _, err := store.Set(ctx, "ns", "key", config.NewValue("v1-value")); err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	if _, err := store.Set(ctx, "ns", "key", config.NewValue("v2-value")); err != nil {
		t.Fatalf("seed v2: %v", err)
	}

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /v1/namespaces/{namespace}/keys/{key}/diff", newInProcessDiffHandler(svc))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestDiffHandler(t *testing.T) {
	srv := newDiffTestServer(t)

	tests := []struct {
		name       string
		path       string
		wantStatus int
		// only checked when wantStatus == 200
		wantChanged *bool
	}{
		{
			name:        "valid diff changed",
			path:        "/v1/namespaces/ns/keys/key/diff?v1=1&v2=2",
			wantStatus:  http.StatusOK,
			wantChanged: boolPtr(true),
		},
		{
			name:        "valid diff unchanged (same version)",
			path:        "/v1/namespaces/ns/keys/key/diff?v1=2&v2=2",
			wantStatus:  http.StatusOK,
			wantChanged: boolPtr(false),
		},
		{
			name:       "missing v1",
			path:       "/v1/namespaces/ns/keys/key/diff?v2=2",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing v2",
			path:       "/v1/namespaces/ns/keys/key/diff?v1=1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-numeric v2",
			path:       "/v1/namespaces/ns/keys/key/diff?v1=1&v2=abc",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "version not found",
			path:       "/v1/namespaces/ns/keys/key/diff?v1=1&v2=99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "key not found",
			path:       "/v1/namespaces/ns/keys/missing/diff?v1=1&v2=2",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tt.path)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus != http.StatusOK {
				return
			}

			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}

			var body diffResponse
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Namespace != "ns" {
				t.Errorf("namespace = %q, want ns", body.Namespace)
			}
			if body.Key != "key" {
				t.Errorf("key = %q, want key", body.Key)
			}
			if tt.wantChanged != nil && body.Changed != *tt.wantChanged {
				t.Errorf("changed = %v, want %v", body.Changed, *tt.wantChanged)
			}
		})
	}
}

// TestDiffHandler_MissingPathValues verifies parseDiffParams rejects requests
// with no namespace/key path values (handler invoked without route patterns).
func TestDiffHandler_MissingPathValues(t *testing.T) {
	handler := newInProcessDiffHandler(nil)

	// Direct call without ServeMux pattern: PathValue returns "" for both.
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces//keys//diff?v1=1&v2=2", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func boolPtr(b bool) *bool { return &b }
