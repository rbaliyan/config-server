package gateway

import (
	"fmt"
	"net/http"

	"github.com/rbaliyan/config-server/service"
)

// AuthMiddleware returns HTTP middleware that authenticates and authorizes
// requests using the provided SecurityGuard. The action is "METHOD:PATH".
//
// Returns 401 for authentication failures and 403 for authorization denials.
func AuthMiddleware(guard service.SecurityGuard) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			id, err := guard.Authenticate(ctx)
			if err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			action := fmt.Sprintf("%s:%s", r.Method, r.URL.Path)
			decision, err := guard.Authorize(ctx, id, action)
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
