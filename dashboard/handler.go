// Package dashboard provides an embedded web UI for the config-server gateway.
// It serves static files from the embedded filesystem and relies on the
// existing HTTP REST endpoints for all data operations.
package dashboard

import (
	"bytes"
	"embed"
	"html"
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
func Handler(mountPath, apiBase string) http.Handler {
	if mountPath == "" {
		mountPath = "/dashboard"
	}
	fs := http.FileServer(http.FS(staticFS))

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
			needle := []byte(`<meta id="api-base" content="">`)
			replacement := []byte(`<meta id="api-base" content="` + html.EscapeString(apiBase) + `">`)
			indexBytes = bytes.Replace(raw, needle, replacement, 1)
		})
		return indexBytes, indexErr
	}

	return http.StripPrefix(mountPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Serve the rewritten index.html for the root path so apiBase is present.
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
		// Block directory-traversal attempts and requests for the raw
		// index to funnel through the rewritten path.
		if strings.Contains(r.URL.Path, "..") {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = "/static" + r.URL.Path
		fs.ServeHTTP(w, r)
	}))
}
