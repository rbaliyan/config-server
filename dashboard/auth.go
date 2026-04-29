package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// DashboardAuth protects the dashboard route and configures the embedded JS
// to authenticate every config API request it makes.
//
// The two concerns are deliberately coupled: whatever credential the server
// accepts on the dashboard route (a session cookie, a JWT cookie, a bearer
// token) is the same credential the browser forwards to the config REST API,
// so the SecurityGuard on the gRPC service validates the same credential on
// both paths.
//
// Server side: Middleware wraps the static file handler; unauthenticated
// requests are rejected before they reach it.
//
// Client side: ClientConfig returns key→value pairs that are JSON-encoded and
// injected into index.html as the "auth-config" meta tag. The dashboard JS
// reads this to decide how to attach credentials to each fetch call.
//
// Built-in implementations are provided for the two most common cases:
//
//   - CookieAuth / CookieAuth with a JWT validator — the browser forwards
//     the cookie automatically; no token input is shown.
//   - BearerTokenAuth — the dashboard shows a persistent token input field
//     in the sidebar; the token is kept in sessionStorage.
//
// For any other strategy (e.g. mTLS, HTTP Basic, custom header) implement
// this interface directly.
type DashboardAuth interface {
	// Middleware returns an HTTP handler that enforces authentication.
	// Unauthenticated requests must not be forwarded to next; respond
	// with an appropriate status (typically 401) instead.
	Middleware(next http.Handler) http.Handler

	// ClientConfig returns the key→value pairs that are JSON-encoded and
	// injected into index.html as the "auth-config" meta tag content.
	//
	// Keys recognised by the built-in JS:
	//   "auth-type"        — "cookie", "bearer", or "none"
	//   "auth-credentials" — fetch credentials mode ("include", "same-origin")
	//   "auth-header"      — request header name for bearer auth
	ClientConfig() map[string]string
}

// CookieAuth returns a DashboardAuth that reads cookieName from the request
// cookie jar and calls validate with the raw value. validate may be nil, in
// which case any non-empty cookie is accepted.
//
// Because the browser forwards cookies automatically on same-origin requests,
// the dashboard JS is configured with credentials:"include" and no token-input
// UI is shown to the user. This covers both plain session cookies and JWT
// cookies — the validation logic lives entirely in validate.
func CookieAuth(cookieName string, validate func(r *http.Request, cookieValue string) error) DashboardAuth {
	return &cookieAuth{name: cookieName, validate: validate}
}

type cookieAuth struct {
	name     string
	validate func(r *http.Request, cookieValue string) error
}

func (a *cookieAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(a.name)
		if err != nil || cookie.Value == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if a.validate != nil {
			if err := a.validate(r, cookie.Value); err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (a *cookieAuth) ClientConfig() map[string]string {
	return map[string]string{
		"auth-type":        "cookie",
		"auth-credentials": "include",
	}
}

// BearerTokenAuth returns a DashboardAuth that expects an
// "Authorization: Bearer <token>" header on every dashboard request.
// validate receives the raw token without the "Bearer " prefix; return a
// non-nil error to reject with 401. validate may be nil, in which case any
// non-empty token is accepted.
//
// The dashboard JS shows a persistent token-input field in the sidebar. The
// token is stored in sessionStorage (current tab only) and attached as an
// Authorization header to every config API request.
func BearerTokenAuth(validate func(r *http.Request, token string) error) DashboardAuth {
	return &bearerAuth{validate: validate}
}

type bearerAuth struct {
	validate func(r *http.Request, token string) error
}

func (a *bearerAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(raw, "Bearer ")
		if !ok || token == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if a.validate != nil {
			if err := a.validate(r, token); err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (a *bearerAuth) ClientConfig() map[string]string {
	return map[string]string{
		"auth-type":   "bearer",
		"auth-header": "Authorization",
	}
}

// ── HMACAuth ───────────────────────────────────────────────────────────────

// HMACConfig configures the HMAC-based dashboard auth.
type HMACConfig struct {
	// Secret is the HMAC-SHA256 signing key for session tokens.
	// Must be at least 32 bytes. Load from an environment variable or secrets
	// manager — never hard-code in source.
	Secret []byte

	// Passphrase is the shared login passphrase validated on the login form.
	// Load from an environment variable — never hard-code in source.
	Passphrase string

	// CookieName is the session cookie name. Default: "dash-session".
	CookieName string

	// TokenTTL is how long a session token remains valid. Default: 8h.
	// Set to 0 for non-expiring tokens (not recommended for production).
	TokenTTL time.Duration

	// LoginPath is the path suffix (relative to the dashboard mount) for the
	// login page. Default: "/login".
	// If the dashboard is mounted at "/dashboard", the login page is at
	// "/dashboard/login".
	LoginPath string

	// Secure sets the Secure flag on the session cookie.
	// Should be true in production (requires HTTPS).
	Secure bool
}

// HMACAuth returns a fully self-contained DashboardAuth that:
//   - Serves an inline login form at <mountPath><LoginPath>.
//   - Validates the submitted passphrase with constant-time comparison.
//   - Issues a stateless, HMAC-SHA256-signed session cookie on success;
//     no session store is required.
//   - Rejects requests with missing or invalid cookies by redirecting to
//     the login page.
//
// From the JS side the session behaves identically to CookieAuth —
// ClientConfig returns {"auth-type":"cookie","auth-credentials":"include"}.
//
// This handler does not throttle login attempts. Deploy behind a
// rate-limiter or reverse proxy to prevent brute-force attacks on the
// passphrase.
//
// Returns an error if Secret is shorter than 32 bytes, Passphrase is empty,
// or LoginPath does not start with "/".
func HMACAuth(cfg HMACConfig) (DashboardAuth, error) {
	if len(cfg.Secret) < 32 {
		return nil, fmt.Errorf("dashboard: HMACAuth secret must be at least 32 bytes, got %d", len(cfg.Secret))
	}
	if cfg.Passphrase == "" {
		return nil, errors.New("dashboard: HMACAuth passphrase must not be empty")
	}
	if cfg.CookieName == "" {
		cfg.CookieName = "dash-session"
	}
	if cfg.TokenTTL == 0 {
		cfg.TokenTTL = 8 * time.Hour
	}
	if cfg.LoginPath == "" {
		cfg.LoginPath = "/login"
	}
	// LoginPath must start with "/" and must not have a trailing slash, which
	// would cause the equality check in Middleware to miss bare-path requests.
	if cfg.LoginPath[0] != '/' {
		return nil, errors.New("dashboard: HMACAuth LoginPath must start with /")
	}
	cfg.LoginPath = strings.TrimRight(cfg.LoginPath, "/")
	if cfg.LoginPath == "" {
		cfg.LoginPath = "/login"
	}
	tmpl, err := template.New("login").Parse(loginFormHTML)
	if err != nil {
		return nil, fmt.Errorf("dashboard: HMACAuth login template: %w", err)
	}
	a := &hmacAuth{cfg: cfg, loginTmpl: tmpl}
	// loginFullPath defaults to cfg.LoginPath so that callers who use
	// Middleware() directly (without going through Handler()) get a working
	// login path. Handler() overwrites this via initPath().
	a.loginFullPath = cfg.LoginPath
	return a, nil
}

type hmacAuth struct {
	cfg           HMACConfig
	loginFullPath string // set to mountPath+cfg.LoginPath by Handler via pathAware
	mountPath     string
	loginTmpl     *template.Template
}

func (a *hmacAuth) initPath(mountPath string) {
	a.mountPath = mountPath
	a.loginFullPath = mountPath + a.cfg.LoginPath
}

func (a *hmacAuth) ClientConfig() map[string]string {
	return map[string]string{
		"auth-type":        "cookie",
		"auth-credentials": "include",
	}
}

func (a *hmacAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == a.loginFullPath {
			switch r.Method {
			case http.MethodGet:
				a.serveLoginForm(w, http.StatusOK, false)
			case http.MethodPost:
				a.handleLogin(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
			return
		}

		cookie, err := r.Cookie(a.cfg.CookieName)
		if err != nil || a.validateToken(cookie.Value) != nil {
			http.Redirect(w, r, a.loginFullPath, http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *hmacAuth) handleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	submitted := r.FormValue("passphrase")
	if subtle.ConstantTimeCompare([]byte(submitted), []byte(a.cfg.Passphrase)) != 1 {
		a.serveLoginForm(w, http.StatusUnauthorized, true)
		return
	}

	token, err := a.issueToken()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	dashRoot := a.mountPath + "/" // "/" when mountPath is empty (direct Middleware use)
	http.SetCookie(w, &http.Cookie{
		Name:     a.cfg.CookieName,
		Value:    token,
		Path:     dashRoot,
		HttpOnly: true,
		Secure:   a.cfg.Secure,
		SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, dashRoot, http.StatusSeeOther)
}

// sessionPayload is the JSON body of a session token.
type sessionPayload struct {
	IAT int64 `json:"iat"`
	EXP int64 `json:"exp,omitempty"` // 0 = non-expiring
}

func (a *hmacAuth) issueToken() (string, error) {
	now := time.Now().Unix()
	p := sessionPayload{IAT: now}
	if a.cfg.TokenTTL > 0 {
		p.EXP = now + int64(a.cfg.TokenTTL.Seconds())
	}
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	encoded := base64.URLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, a.cfg.Secret)
	mac.Write([]byte(encoded))
	sig := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + sig, nil
}

func (a *hmacAuth) validateToken(token string) error {
	dot := strings.LastIndexByte(token, '.')
	if dot < 0 {
		return errors.New("invalid token")
	}
	payload, sigB64 := token[:dot], token[dot+1:]

	mac := hmac.New(sha256.New, a.cfg.Secret)
	mac.Write([]byte(payload))
	expected := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(sigB64)) {
		return errors.New("invalid signature")
	}

	data, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	var p sessionPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if p.EXP > 0 && time.Now().Unix() > p.EXP {
		return errors.New("token expired")
	}
	return nil
}

// ── Login form ─────────────────────────────────────────────────────────────

// loginFormHTML is the source for the inline login page. Parsed once per
// HMACAuth instance at construction time — no package-level panic risk.
const loginFormHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Config Dashboard — Sign in</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:system-ui,sans-serif;background:#f3f4f6;display:flex;align-items:center;justify-content:center;min-height:100vh}
    .card{background:#fff;border-radius:8px;padding:2rem;box-shadow:0 2px 8px rgba(0,0,0,.1);width:100%;max-width:340px}
    h1{font-size:1.1rem;color:#111;margin-bottom:1.5rem}
    label{display:block;font-size:.85rem;color:#555;margin-bottom:.35rem}
    input{width:100%;padding:.5rem .75rem;border:1px solid #d1d5db;border-radius:4px;font-size:.875rem}
    input:focus{outline:none;border-color:#2563eb;box-shadow:0 0 0 2px rgba(37,99,235,.2)}
    button{margin-top:1rem;width:100%;padding:.55rem;background:#2563eb;color:#fff;border:none;border-radius:4px;font-size:.875rem;cursor:pointer}
    button:hover{background:#1d4ed8}
    .err{color:#dc2626;font-size:.8rem;margin-bottom:1rem}
  </style>
</head>
<body>
  <div class="card">
    <h1>Config Dashboard</h1>
    {{if .Error}}<p class="err">Incorrect passphrase. Please try again.</p>{{end}}
    <form method="POST" autocomplete="off">
      <label for="p">Passphrase</label>
      <input id="p" type="password" name="passphrase" autocomplete="current-password" autofocus required>
      <button type="submit">Sign in</button>
    </form>
  </div>
</body>
</html>`

func (a *hmacAuth) serveLoginForm(w http.ResponseWriter, status int, showError bool) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := a.loginTmpl.Execute(w, map[string]bool{"Error": showError}); err != nil {
		slog.Warn("dashboard: login template execute failed", "error", err)
	}
}
