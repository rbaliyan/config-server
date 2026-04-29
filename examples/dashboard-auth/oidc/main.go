// Package oidc shows how to implement dashboard.DashboardAuth using OIDC.
//
// To run this example, copy the code block below into your own module and add:
//
//	go get github.com/coreos/go-oidc/v3/oidc golang.org/x/oauth2
package oidc

/*
How it works:

 1. oidcAuth.Middleware checks every request for a valid session cookie.
 2. Unauthenticated requests are redirected to the provider's authorization
    endpoint. A short-lived state cookie prevents CSRF.
 3. The provider redirects back to /dashboard/callback. The handler validates
    the state, exchanges the code for tokens, verifies the ID token, and sets
    an HttpOnly session cookie.
 4. From the JS side the user just has a cookie, so ClientConfig returns
    {"auth-type":"cookie","auth-credentials":"include"} — identical to the
    built-in CookieAuth.

Deploy notes:
  - Register /dashboard/callback as an allowed redirect URI in your provider.
  - Set OIDCConfig.Secure = true in production (requires HTTPS).
  - For multi-instance deployments, back the session store with Redis rather
    than the in-memory default shown here.

---

	package main

	import (
		"context"
		"crypto/rand"
		"encoding/base64"
		"log"
		"net/http"
		"os"
		"sync"
		"time"

		gooidc "github.com/coreos/go-oidc/v3/oidc"
		"golang.org/x/oauth2"

		"github.com/rbaliyan/config-server/dashboard"
		"github.com/rbaliyan/config-server/gateway"
	)

	// ── In-memory session store ────────────────────────────────────────────────

	type session struct {
		Subject string
		Expiry  time.Time
	}

	type memSessionStore struct {
		mu   sync.Mutex
		data map[string]session
	}

	func newMemSessionStore() *memSessionStore {
		return &memSessionStore{data: make(map[string]session)}
	}

	func (s *memSessionStore) Set(id, subject string, expiry time.Time) {
		s.mu.Lock()
		s.data[id] = session{Subject: subject, Expiry: expiry}
		s.mu.Unlock()
	}

	func (s *memSessionStore) Get(id string) (string, bool) {
		s.mu.Lock()
		sess, ok := s.data[id]
		s.mu.Unlock()
		if !ok || time.Now().After(sess.Expiry) {
			return "", false
		}
		return sess.Subject, true
	}

	// ── OIDC DashboardAuth implementation ─────────────────────────────────────

	type oidcAuth struct {
		provider     *gooidc.Provider
		oauth2Config oauth2.Config
		verifier     *gooidc.IDTokenVerifier
		sessions     *memSessionStore
		cookieName   string
		mountPath    string // set by Handler via pathAware
		callbackPath string // relative to mount, e.g. "/callback"
		secure       bool
	}

	// Compile-time check.
	var _ dashboard.DashboardAuth = (*oidcAuth)(nil)

	func newOIDCAuth(ctx context.Context) (*oidcAuth, error) {
		issuer    := os.Getenv("OIDC_ISSUER")        // e.g. https://accounts.google.com
		clientID  := os.Getenv("OIDC_CLIENT_ID")
		secret    := os.Getenv("OIDC_CLIENT_SECRET")
		redirect  := os.Getenv("OIDC_REDIRECT_URL")  // https://myapp.com/dashboard/callback

		provider, err := gooidc.NewProvider(ctx, issuer)
		if err != nil {
			return nil, err
		}
		return &oidcAuth{
			provider: provider,
			oauth2Config: oauth2.Config{
				ClientID:     clientID,
				ClientSecret: secret,
				RedirectURL:  redirect,
				Endpoint:     provider.Endpoint(),
				Scopes:       []string{gooidc.ScopeOpenID, "email", "profile"},
			},
			verifier:     provider.Verifier(&gooidc.Config{ClientID: clientID}),
			sessions:     newMemSessionStore(),
			cookieName:   "dash-oidc-session",
			callbackPath: "/callback",
			secure:       os.Getenv("APP_ENV") == "production",
		}, nil
	}

	// initPath is called by dashboard.Handler so the middleware knows the full
	// callback path without a separate config field.
	func (a *oidcAuth) initPath(mountPath string) { a.mountPath = mountPath }

	func (a *oidcAuth) ClientConfig() map[string]string {
		return map[string]string{
			"auth-type":        "cookie",
			"auth-credentials": "include",
		}
	}

	func (a *oidcAuth) Middleware(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callbackFull := a.mountPath + a.callbackPath

			// Handle OIDC callback.
			if r.URL.Path == callbackFull {
				a.handleCallback(w, r)
				return
			}

			// Check session cookie.
			if c, err := r.Cookie(a.cookieName); err == nil {
				if _, ok := a.sessions.Get(c.Value); ok {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Redirect to provider with CSRF state cookie.
			state := randomToken()
			http.SetCookie(w, &http.Cookie{
				Name: "oidc-state", Value: state, Path: "/",
				HttpOnly: true, Secure: a.secure,
				SameSite: http.SameSiteLaxMode, MaxAge: 300,
			})
			http.Redirect(w, r, a.oauth2Config.AuthCodeURL(state), http.StatusFound)
		})
	}

	func (a *oidcAuth) handleCallback(w http.ResponseWriter, r *http.Request) {
		// Validate CSRF state.
		sc, err := r.Cookie("oidc-state")
		if err != nil || sc.Value != r.URL.Query().Get("state") {
			http.Error(w, "invalid state", http.StatusBadRequest)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "oidc-state", MaxAge: -1, Path: "/"})

		// Exchange code for tokens.
		token, err := a.oauth2Config.Exchange(r.Context(), r.URL.Query().Get("code"))
		if err != nil {
			http.Error(w, "token exchange failed", http.StatusInternalServerError)
			return
		}
		rawID, _ := token.Extra("id_token").(string)
		idToken, err := a.verifier.Verify(r.Context(), rawID)
		if err != nil {
			http.Error(w, "id_token verification failed", http.StatusUnauthorized)
			return
		}

		var claims struct{ Sub string `json:"sub"` }
		_ = idToken.Claims(&claims)

		// Create session.
		sessionID := randomToken()
		expiry := time.Now().Add(8 * time.Hour)
		a.sessions.Set(sessionID, claims.Sub, expiry)

		http.SetCookie(w, &http.Cookie{
			Name:     a.cookieName,
			Value:    sessionID,
			Path:     a.mountPath + "/",
			HttpOnly: true,
			Secure:   a.secure,
			SameSite: http.SameSiteStrictMode,
			Expires:  expiry,
		})
		http.Redirect(w, r, a.mountPath+"/", http.StatusSeeOther)
	}

	func randomToken() string {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		return base64.URLEncoding.EncodeToString(b)
	}

	// ── Wire-up ────────────────────────────────────────────────────────────────

	func main() {
		ctx := context.Background()
		auth, err := newOIDCAuth(ctx)
		if err != nil {
			log.Fatal(err)
		}
		handler, err := gateway.NewInProcessHandler(ctx, nil, // supply your svc
			gateway.WithDashboard(),
			gateway.WithDashboardAuth(auth),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer handler.Close()
		log.Fatal(http.ListenAndServe(":8080", handler))
	}
*/
