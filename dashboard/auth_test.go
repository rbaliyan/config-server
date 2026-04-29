package dashboard

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// ── CookieAuth ─────────────────────────────────────────────────────────────

func TestCookieAuth_MissingCookie(t *testing.T) {
	a := CookieAuth("session", nil)
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestCookieAuth_EmptyCookie(t *testing.T) {
	a := CookieAuth("session", nil)
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: ""})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestCookieAuth_ValidCookie_NilValidate(t *testing.T) {
	a := CookieAuth("session", nil)
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "tok"})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
}

func TestCookieAuth_ValidateCalled(t *testing.T) {
	var got string
	a := CookieAuth("session", func(_ *http.Request, v string) error {
		got = v
		return nil
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "abc"})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if got != "abc" {
		t.Fatalf("validate called with %q, want %q", got, "abc")
	}
}

func TestCookieAuth_ValidateRejects(t *testing.T) {
	a := CookieAuth("session", func(_ *http.Request, _ string) error {
		return errors.New("invalid token")
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: "bad"})
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestCookieAuth_ClientConfig(t *testing.T) {
	cfg := CookieAuth("s", nil).ClientConfig()
	if cfg["auth-type"] != "cookie" {
		t.Errorf("auth-type = %q, want cookie", cfg["auth-type"])
	}
	if cfg["auth-credentials"] != "include" {
		t.Errorf("auth-credentials = %q, want include", cfg["auth-credentials"])
	}
}

// ── BearerTokenAuth ────────────────────────────────────────────────────────

func TestBearerTokenAuth_MissingHeader(t *testing.T) {
	a := BearerTokenAuth(nil)
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestBearerTokenAuth_WrongScheme(t *testing.T) {
	a := BearerTokenAuth(nil)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestBearerTokenAuth_EmptyToken(t *testing.T) {
	a := BearerTokenAuth(nil)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer ")
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestBearerTokenAuth_ValidToken_NilValidate(t *testing.T) {
	a := BearerTokenAuth(nil)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer mytoken")
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
}

func TestBearerTokenAuth_ValidateCalled(t *testing.T) {
	var got string
	a := BearerTokenAuth(func(_ *http.Request, token string) error {
		got = token
		return nil
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if got != "secret" {
		t.Fatalf("validate called with %q, want %q", got, "secret")
	}
}

func TestBearerTokenAuth_ValidateRejects(t *testing.T) {
	a := BearerTokenAuth(func(_ *http.Request, _ string) error {
		return errors.New("expired")
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer expired-token")
	rec := httptest.NewRecorder()
	a.Middleware(okHandler()).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestBearerTokenAuth_ClientConfig(t *testing.T) {
	cfg := BearerTokenAuth(nil).ClientConfig()
	if cfg["auth-type"] != "bearer" {
		t.Errorf("auth-type = %q, want bearer", cfg["auth-type"])
	}
	if cfg["auth-header"] != "Authorization" {
		t.Errorf("auth-header = %q, want Authorization", cfg["auth-header"])
	}
}
