// Package dashboard provides an embedded web UI for the config-server gateway.
// It serves static files from the embedded filesystem and relies on the
// existing HTTP REST endpoints for all data operations.
package dashboard

import (
	"bytes"
	"embed"
	"encoding/json"
	"html"
	"log/slog"
	"net/http"
	"strings"
	"sync"
)

//go:embed static
var staticFS embed.FS

// Handler returns an HTTP handler serving the dashboard UI at the given
// mount path (e.g. "/dashboard"). The path must start with "/" and should
// not end with "/"; if empty, "/dashboard" is used.
//
// apiBase is the base path prefix used by the dashboard JS when constructing
// API URLs (e.g. "" for same-origin, "/api" for a proxied deployment).
// The value is injected into the page via the "api-base" meta tag.
//
// auth, if non-nil, is applied in two ways:
//   - auth.Middleware wraps the handler so unauthenticated requests are
//     rejected before the static files are served.
//   - auth.ClientConfig() is JSON-encoded and injected into index.html as
//     the "auth-config" meta tag so the dashboard JS knows how to attach
//     credentials to every config API request it makes.
//
// Pass nil for auth to serve the dashboard without any access control
// (suitable when the route is protected at the network or reverse-proxy level).
func Handler(mountPath, apiBase string, auth DashboardAuth) http.Handler {
	if mountPath == "" {
		mountPath = "/dashboard"
	}
	fs := http.FileServer(http.FS(staticFS))

	// Pre-marshal the auth client config once — it is static for the
	// lifetime of this handler.
	authConfigJSON := []byte("{}")
	if auth != nil {
		if b, err := json.Marshal(auth.ClientConfig()); err != nil {
			slog.Warn("dashboard: failed to marshal auth client config", "error", err)
		} else {
			authConfigJSON = b
		}
	}

	var (
		indexOnce  sync.Once
		indexBytes []byte
		indexErr   error
	)
	loadIndex := func() ([]byte, error) {
		indexOnce.Do(func() {
			raw, err := staticFS.ReadFile("static/index.html")
			if err != nil {
				indexErr = err
				return
			}
			// Inject apiBase into the "api-base" meta tag so JS can read it.
			b := bytes.Replace(raw,
				[]byte(`<meta id="api-base" content="">`),
				[]byte(`<meta id="api-base" content="`+html.EscapeString(apiBase)+`">`),
				1,
			)
			// Inject auth client config into the "auth-config" meta tag.
			b = bytes.Replace(b,
				[]byte(`<meta id="auth-config" content="">`),
				[]byte(`<meta id="auth-config" content="`+html.EscapeString(string(authConfigJSON))+`">`),
				1,
			)
			indexBytes = b
		})
		return indexBytes, indexErr
	}

	inner := http.StripPrefix(mountPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Serve the rewritten index.html for the root path so injected values are present.
		if r.URL.Path == "" || r.URL.Path == "/" || r.URL.Path == "/index.html" {
			body, err := loadIndex()
			if err != nil {
				http.Error(w, "dashboard: index not available", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(body)
			return
		}
		// Block directory-traversal attempts and funnel raw index requests
		// through the rewritten path above.
		if strings.Contains(r.URL.Path, "..") {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = "/static" + r.URL.Path
		fs.ServeHTTP(w, r)
	}))

	if auth == nil {
		return inner
	}
	// pathAware is an opt-in internal interface. Auth implementations that
	// need to know the mount path (e.g. to serve a login sub-page) implement
	// it. Not part of the public DashboardAuth contract.
	type pathAware interface{ initPath(string) }
	if pa, ok := auth.(pathAware); ok {
		pa.initPath(mountPath)
	}
	return auth.Middleware(inner)
}
