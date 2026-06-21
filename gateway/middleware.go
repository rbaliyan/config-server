package gateway

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/rbaliyan/config-server/service"
)

// AuthMiddleware returns HTTP middleware that authenticates and authorizes
// requests using the provided SecurityGuard. The action is "METHOD:PATH".
//
// Returns 401 for authentication failures and 403 for authorization denials.
func AuthMiddleware(guard service.SecurityGuard) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bridge HTTP headers into gRPC incoming metadata before
			// authenticating, so metadata-based guards (e.g. the OPA authorizer
			// reading Authorization via metadata.FromIncomingContext) see the
			// real credential rather than an empty context. This mirrors the
			// SSE and diff handlers in this package.
			ctx := httpHeadersToMetadata(r.Context(), r)

			id, err := guard.Authenticate(ctx)
			if err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			action := fmt.Sprintf("%s:%s", r.Method, r.URL.Path)
			// Populate the namespace/key so resource-scoped policies can fire on
			// the HTTP path, matching the gRPC path (which sets Resource
			// per-RPC). Authorize-error is treated as deny (500), consistent
			// with the gRPC codes.Internal mapping.
			decision, err := guard.Authorize(ctx, id, action, resourceFromPath(r.URL.Path))
			if err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if !decision.Allowed {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}

			ctx = service.ContextWithIdentity(ctx, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// resourceFromPath does a best-effort extraction of the namespace and key from
// a gateway URL path so resource-scoped authorization works on the HTTP path,
// matching the gRPC path which populates [service.Resource] per RPC. It
// recognizes the /v1/namespaces/{namespace}/keys/{key} family (the key may
// itself contain slashes, with a trailing /versions or /diff action suffix
// stripped), namespace-scoped routes (List, snapshot, access), and the
// /v1/aliases/{alias} routes (mapped to Resource{Key: alias}, matching the
// gRPC SetAlias/GetAlias/DeleteAlias handlers). Paths that carry no
// namespace/key/alias (e.g. /v1/aliases list, /v1/codecs) yield a zero
// Resource; guards needing other identifiers can still parse r.URL.Path.
func resourceFromPath(path string) service.Resource {
	const nsMarker = "namespaces/"
	if idx := strings.Index(path, nsMarker); idx >= 0 {
		rest := path[idx+len(nsMarker):]

		var res service.Resource
		if k := strings.Index(rest, "/keys/"); k >= 0 {
			res.Namespace = rest[:k]
			key := rest[k+len("/keys/"):]
			key = strings.TrimSuffix(key, "/versions")
			key = strings.TrimSuffix(key, "/diff")
			res.Key = key
			return res
		}

		// Namespace-scoped route with no key segment: take up to the next slash.
		if s := strings.IndexByte(rest, '/'); s >= 0 {
			res.Namespace = rest[:s]
		} else {
			res.Namespace = rest
		}
		return res
	}

	// Alias routes: /v1/aliases/{alias} authorizes against Resource{Key: alias},
	// matching the gRPC alias handlers. The bare /v1/aliases list route (no
	// trailing alias) yields a zero Resource.
	const aliasMarker = "aliases/"
	if idx := strings.Index(path, aliasMarker); idx >= 0 {
		if alias := path[idx+len(aliasMarker):]; alias != "" && !strings.Contains(alias, "/") {
			return service.Resource{Key: alias}
		}
	}

	return service.Resource{}
}
