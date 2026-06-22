package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardHandler_StaticServe(t *testing.T) {
	h := Handler("/dashboard", "", nil)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	tests := []struct {
		name            string
		path            string
		wantStatus      int
		wantContentType string // substring match; "" to skip
	}{
		{
			name:            "root serves injected index html",
			path:            "/dashboard/",
			wantStatus:      http.StatusOK,
			wantContentType: "text/html",
		},
		{
			name:            "index.html serves html",
			path:            "/dashboard/index.html",
			wantStatus:      http.StatusOK,
			wantContentType: "text/html",
		},
		{
			name:            "known js asset",
			path:            "/dashboard/app.js",
			wantStatus:      http.StatusOK,
			wantContentType: "javascript",
		},
		{
			name:            "known css asset",
			path:            "/dashboard/style.css",
			wantStatus:      http.StatusOK,
			wantContentType: "css",
		},
		{
			name:       "missing asset 404",
			path:       "/dashboard/does-not-exist.js",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tt.path)
			if err != nil {
				t.Fatalf("GET %s: %v", tt.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantContentType != "" {
				ct := resp.Header.Get("Content-Type")
				if !strings.Contains(ct, tt.wantContentType) {
					t.Errorf("Content-Type = %q, want substring %q", ct, tt.wantContentType)
				}
			}
		})
	}
}

// TestDashboardHandler_PathTraversal verifies the defense-in-depth ".." guard
// rejects traversal attempts. The request bypasses net/http's own cleaning by
// using a raw client connection with an un-cleaned path.
func TestDashboardHandler_PathTraversal(t *testing.T) {
	h := Handler("/dashboard", "", nil)

	// A path containing ".." after StripPrefix. httptest.NewRequest preserves
	// the raw path so the handler's strings.Contains(path, "..") guard fires.
	req := httptest.NewRequest(http.MethodGet, "/dashboard/x", nil)
	// Invoke the handler directly (no ServeMux) so URL.Path is exactly what we
	// set and the handler's strings.Contains(path, "..") guard is the barrier.
	req.URL.Path = "/dashboard/../etc/passwd"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("traversal status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestDashboardHandler_DefaultMountPath(t *testing.T) {
	// Empty mount path defaults to /dashboard.
	h := Handler("", "", nil)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/dashboard/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// TestDashboardHandler_AuthLoginFlow exercises the HMACAuth login form + login
// handler happy paths through the dashboard Handler (which wires initPath).
func TestDashboardHandler_AuthLoginFlow(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	auth, err := HMACAuth(HMACConfig{
		Secret:     secret,
		Passphrase: "letmein",
	})
	if err != nil {
		t.Fatalf("HMACAuth: %v", err)
	}

	h := Handler("/dashboard", "", auth)
	// No redirect-following client so we can inspect intermediate responses.
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	// GET the login form (serveLoginForm happy path).
	loginResp, err := client.Get(srv.URL + "/dashboard/login")
	if err != nil {
		t.Fatalf("GET login: %v", err)
	}
	loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusOK {
		t.Fatalf("login form status = %d, want 200", loginResp.StatusCode)
	}
	if ct := loginResp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("login form Content-Type = %q, want text/html", ct)
	}

	// POST the correct passphrase (handleLogin happy path) -> 303 + cookie.
	form := strings.NewReader("passphrase=letmein")
	postReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/dashboard/login", form)
	postReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	postResp, err := client.Do(postReq)
	if err != nil {
		t.Fatalf("POST login: %v", err)
	}
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login POST status = %d, want 303", postResp.StatusCode)
	}
	var sessionCookie *http.Cookie
	for _, c := range postResp.Cookies() {
		if c.Name == "dash-session" {
			sessionCookie = c
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" {
		t.Fatal("expected dash-session cookie to be set on login")
	}

	// Unauthenticated dashboard request redirects to login.
	unauthResp, err := client.Get(srv.URL + "/dashboard/")
	if err != nil {
		t.Fatalf("GET dashboard unauth: %v", err)
	}
	unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusFound {
		t.Fatalf("unauth dashboard status = %d, want 302", unauthResp.StatusCode)
	}

	// With the cookie, the dashboard is served.
	authReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/dashboard/", nil)
	authReq.AddCookie(sessionCookie)
	authResp, err := client.Do(authReq)
	if err != nil {
		t.Fatalf("GET dashboard auth: %v", err)
	}
	authResp.Body.Close()
	if authResp.StatusCode != http.StatusOK {
		t.Fatalf("auth dashboard status = %d, want 200", authResp.StatusCode)
	}
}
