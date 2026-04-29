package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

var testSecret = make([]byte, 32) // zero-valued, valid for tests

func newTestHMAC(t *testing.T, opts ...func(*HMACConfig)) *hmacAuth {
	t.Helper()
	cfg := HMACConfig{Secret: testSecret, Passphrase: "correct-passphrase"}
	for _, o := range opts {
		o(&cfg)
	}
	a, err := HMACAuth(cfg)
	if err != nil {
		t.Fatalf("HMACAuth: %v", err)
	}
	ha := a.(*hmacAuth)
	ha.initPath("/dashboard")
	return ha
}

// ── Construction errors ────────────────────────────────────────────────────

func TestHMACAuth_ShortSecret(t *testing.T) {
	_, err := HMACAuth(HMACConfig{Secret: make([]byte, 16), Passphrase: "pass"})
	if err == nil {
		t.Fatal("expected error for short secret")
	}
}

func TestHMACAuth_EmptyPassphrase(t *testing.T) {
	_, err := HMACAuth(HMACConfig{Secret: testSecret, Passphrase: ""})
	if err == nil {
		t.Fatal("expected error for empty passphrase")
	}
}

func TestHMACAuth_LoginPathMissingLeadingSlash(t *testing.T) {
	_, err := HMACAuth(HMACConfig{Secret: testSecret, Passphrase: "p", LoginPath: "login"})
	if err == nil {
		t.Fatal("expected error for LoginPath without leading slash")
	}
}

func TestHMACAuth_LoginPathTrailingSlashStripped(t *testing.T) {
	a, err := HMACAuth(HMACConfig{Secret: testSecret, Passphrase: "p", LoginPath: "/login/"})
	if err != nil {
		t.Fatal(err)
	}
	ha := a.(*hmacAuth)
	if ha.cfg.LoginPath != "/login" {
		t.Errorf("LoginPath = %q, want /login", ha.cfg.LoginPath)
	}
}

// ── Login page ─────────────────────────────────────────────────────────────

func TestHMACAuth_LoginPage_GET(t *testing.T) {
	a := newTestHMAC(t)
	req := httptest.NewRequest("GET", "/dashboard/login", nil)
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<form") {
		t.Fatal("response should contain a form")
	}
}

func TestHMACAuth_LoginPage_UnknownMethod(t *testing.T) {
	a := newTestHMAC(t)
	req := httptest.NewRequest("DELETE", "/dashboard/login", nil)
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", rec.Code)
	}
}

// ── Login POST ─────────────────────────────────────────────────────────────

func postLogin(t *testing.T, a *hmacAuth, passphrase string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{"passphrase": {passphrase}}
	req := httptest.NewRequest("POST", "/dashboard/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	return rec
}

func TestHMACAuth_Login_WrongPassphrase(t *testing.T) {
	a := newTestHMAC(t)
	rec := postLogin(t, a, "wrong")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Incorrect passphrase") {
		t.Fatal("response should contain error message")
	}
}

func TestHMACAuth_Login_CorrectPassphrase_SetsCookieAndRedirects(t *testing.T) {
	a := newTestHMAC(t)
	rec := postLogin(t, a, "correct-passphrase")
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("want 303, got %d", rec.Code)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected session cookie to be set")
	}
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "dash-session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("dash-session cookie not found")
	}
	if !sessionCookie.HttpOnly {
		t.Error("cookie should be HttpOnly")
	}
	if sessionCookie.SameSite != http.SameSiteStrictMode {
		t.Error("cookie should be SameSite=Strict")
	}
}

// ── Session validation ─────────────────────────────────────────────────────

func TestHMACAuth_NoCookie_Redirects(t *testing.T) {
	a := newTestHMAC(t)
	req := httptest.NewRequest("GET", "/dashboard/", nil)
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/dashboard/login" {
		t.Fatalf("redirect location = %q, want /dashboard/login", loc)
	}
}

func TestHMACAuth_ValidCookie_PassesThrough(t *testing.T) {
	a := newTestHMAC(t)
	token, err := a.issueToken()
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/dashboard/", nil)
	req.AddCookie(&http.Cookie{Name: "dash-session", Value: token})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
}

func TestHMACAuth_TamperedCookie_Redirects(t *testing.T) {
	a := newTestHMAC(t)
	token, _ := a.issueToken()
	// flip last byte of signature
	tampered := token[:len(token)-1] + "X"
	req := httptest.NewRequest("GET", "/dashboard/", nil)
	req.AddCookie(&http.Cookie{Name: "dash-session", Value: tampered})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rec.Code)
	}
}

// expiredToken builds a valid but already-expired session token directly.
func expiredToken(secret []byte) string {
	p := sessionPayload{IAT: 1000, EXP: 1001} // expired in 2001
	data, _ := json.Marshal(p)
	encoded := base64.URLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(encoded))
	sig := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + sig
}

func TestHMACAuth_ExpiredToken_Redirects(t *testing.T) {
	a := newTestHMAC(t)
	token := expiredToken(testSecret)
	req := httptest.NewRequest("GET", "/dashboard/", nil)
	req.AddCookie(&http.Cookie{Name: "dash-session", Value: token})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rec.Code)
	}
}

func TestHMACAuth_DifferentSecret_Rejects(t *testing.T) {
	a1 := newTestHMAC(t)
	token, _ := a1.issueToken()

	otherSecret := make([]byte, 32)
	for i := range otherSecret {
		otherSecret[i] = 0xFF
	}
	a2 := newTestHMAC(t, func(c *HMACConfig) { c.Secret = otherSecret })

	req := httptest.NewRequest("GET", "/dashboard/", nil)
	req.AddCookie(&http.Cookie{Name: "dash-session", Value: token})
	rec := httptest.NewRecorder()
	a2.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rec.Code)
	}
}

// ── ClientConfig ───────────────────────────────────────────────────────────

func TestHMACAuth_ClientConfig(t *testing.T) {
	a := newTestHMAC(t)
	cfg := a.ClientConfig()
	if cfg["auth-type"] != "cookie" {
		t.Errorf("auth-type = %q, want cookie", cfg["auth-type"])
	}
	if cfg["auth-credentials"] != "include" {
		t.Errorf("auth-credentials = %q, want include", cfg["auth-credentials"])
	}
}

// ── Custom mount + login path ──────────────────────────────────────────────

func TestHMACAuth_CustomLoginPath(t *testing.T) {
	a := newTestHMAC(t, func(c *HMACConfig) { c.LoginPath = "/signin" })
	a.initPath("/admin")

	if a.loginFullPath != "/admin/signin" {
		t.Fatalf("loginFullPath = %q, want /admin/signin", a.loginFullPath)
	}

	req := httptest.NewRequest("GET", "/admin/signin", nil)
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 on login page, got %d", rec.Code)
	}
}

// ── Direct Middleware use (without Handler) ────────────────────────────────

// TestHMACAuth_DirectMiddleware verifies that HMACAuth works correctly when
// Middleware() is called directly without going through Handler(), so initPath
// is never called. The login path falls back to cfg.LoginPath ("/login").
func TestHMACAuth_DirectMiddleware(t *testing.T) {
	auth, err := HMACAuth(HMACConfig{
		Secret:     testSecret,
		Passphrase: "direct-pass",
		LoginPath:  "/login",
	})
	if err != nil {
		t.Fatal(err)
	}
	// No initPath call — mountPath is empty, loginFullPath defaults to "/login".
	handler := auth.Middleware(okHandler())

	// Unauthenticated request should redirect to /login.
	req := httptest.NewRequest("GET", "/anything", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Fatalf("redirect = %q, want /login", loc)
	}

	// Login page should be served at /login.
	req = httptest.NewRequest("GET", "/login", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 on login page, got %d", rec.Code)
	}
}

// ── Handler integration: initPath called via pathAware ─────────────────────

func TestHMACAuth_Handler_InitPath(t *testing.T) {
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = 0xAB
	}
	auth, err := HMACAuth(HMACConfig{Secret: secret, Passphrase: "s3cr3t"})
	if err != nil {
		t.Fatal(err)
	}
	// Handler triggers initPath("/dashboard").
	h := Handler("/dashboard", "", auth)

	// Unauthenticated request should redirect to /dashboard/login.
	req := httptest.NewRequest("GET", "/dashboard/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("want 302, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/dashboard/login" {
		t.Fatalf("redirect = %q, want /dashboard/login", loc)
	}
}
