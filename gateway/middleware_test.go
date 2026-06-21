package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/rbaliyan/config-server/service"
)

// metadataGuard is a SecurityGuard that authenticates from gRPC incoming
// metadata, exercising the same access pattern as the OPA authorizer. It only
// allows requests carrying the expected Authorization metadata value.
type metadataGuard struct {
	want string
}

func (g metadataGuard) Authenticate(ctx context.Context) (service.Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return anonIdentity{}, nil
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return anonIdentity{}, nil
	}
	return tokenIdentity{token: vals[0]}, nil
}

func (g metadataGuard) Authorize(ctx context.Context, id service.Identity, action string, _ service.Resource) (service.Decision, error) {
	if ti, ok := id.(tokenIdentity); ok && ti.token == g.want {
		return service.Decision{Allowed: true}, nil
	}
	return service.Decision{Allowed: false, Reason: "missing or invalid credential"}, nil
}

type anonIdentity struct{}

func (anonIdentity) UserID() string         { return "" }
func (anonIdentity) Claims() map[string]any { return nil }

type tokenIdentity struct{ token string }

func (tokenIdentity) UserID() string         { return "user" }
func (tokenIdentity) Claims() map[string]any { return nil }

func TestResourceFromPath(t *testing.T) {
	cases := []struct {
		path   string
		wantNS string
		wantK  string
	}{
		{"/v1/namespaces/prod/keys/db/host", "prod", "db/host"},
		{"/v1/namespaces/prod/keys/db/host/versions", "prod", "db/host"},
		{"/v1/namespaces/prod/keys/db/host/diff", "prod", "db/host"},
		{"/v1/namespaces/prod/u/west/keys/k", "prod/u/west", "k"},
		{"/v1/namespaces/prod/keys", "prod", ""},
		{"/v1/namespaces/prod/snapshot", "prod", ""},
		{"/v1/namespaces/prod/access", "prod", ""},
		{"/v1/namespaces", "", ""},
		{"/v1/aliases/myalias", "", "myalias"},
		{"/v1/aliases", "", ""},
		{"/v1/codecs", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			res := resourceFromPath(tc.path)
			if res.Namespace != tc.wantNS || res.Key != tc.wantK {
				t.Errorf("resourceFromPath(%q) = {ns:%q key:%q}, want {ns:%q key:%q}",
					tc.path, res.Namespace, res.Key, tc.wantNS, tc.wantK)
			}
		})
	}
}

// TestAuthMiddleware_BridgesHTTPHeadersToMetadata is a regression test: the
// middleware must forward HTTP headers as gRPC incoming metadata before
// authenticating, so metadata-based guards see the real credential rather than
// authenticating as anonymous.
func TestAuthMiddleware_BridgesHTTPHeadersToMetadata(t *testing.T) {
	guard := metadataGuard{want: "Bearer secret-token"}
	var reached bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})
	handler := AuthMiddleware(guard)(next)

	t.Run("authorized request with header", func(t *testing.T) {
		reached = false
		req := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/keys/k", nil)
		req.Header.Set("Authorization", "Bearer secret-token")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 with valid header, got %d", rec.Code)
		}
		if !reached {
			t.Fatal("next handler was not reached for an authorized request")
		}
	})

	t.Run("forbidden request without header", func(t *testing.T) {
		reached = false
		req := httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/keys/k", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403 without header, got %d", rec.Code)
		}
		if reached {
			t.Fatal("next handler should not be reached when authorization is denied")
		}
	})
}
