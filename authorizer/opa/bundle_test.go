package opa

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/rbaliyan/config-server/service"
	"google.golang.org/grpc/metadata"
)

// ─── Option tests ─────────────────────────────────────────────────────────────

func TestWithAuthHeader_SetsField(t *testing.T) {
	o := defaultOptions()
	WithAuthHeader("x-api-key")(&o)
	if o.authHeader != "x-api-key" {
		t.Errorf("authHeader = %q, want %q", o.authHeader, "x-api-key")
	}
}

func TestWithSubjectClaim_SetsField(t *testing.T) {
	o := defaultOptions()
	WithSubjectClaim("email")(&o)
	if o.subjectClaim != "email" {
		t.Errorf("subjectClaim = %q, want %q", o.subjectClaim, "email")
	}
}

func TestWithBundlePollInterval_SetsField(t *testing.T) {
	o := defaultOptions()
	WithBundlePollInterval(10 * time.Minute)(&o)
	if o.bundlePollInterval != 10*time.Minute {
		t.Errorf("bundlePollInterval = %v, want 10m", o.bundlePollInterval)
	}
}

func TestWithTLSConfig_SetsField(t *testing.T) {
	o := defaultOptions()
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	WithTLSConfig(cfg)(&o)
	if o.tlsConfig != cfg {
		t.Error("tlsConfig not set")
	}
}

func TestWithJWTVerifier_NilIgnored(t *testing.T) {
	o := defaultOptions()
	WithJWTVerifier(nil)(&o)
	if o.jwtVerifier != nil {
		t.Error("expected nil jwtVerifier for nil input")
	}
}

// ─── NewBundleAuthorizer ──────────────────────────────────────────────────────

func TestNewBundleAuthorizer_InitialLoad(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, denyAllPolicy)
	}))
	defer srv.Close()

	ctx := context.Background()
	a, err := NewBundleAuthorizer(ctx, srv.URL, "data.test.authz.allow")
	if err != nil {
		t.Fatalf("NewBundleAuthorizer: %v", err)
	}
	defer a.Close()

	// Basic sanity: Authorize returns a decision (deny all).
	id := &opaIdentity{userID: "u", claims: map[string]any{}}
	d, err := a.Authorize(ctx, id, "read", service.Resource{Namespace: "ns", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("expected deny-all, got allow")
	}
}

func TestNewBundleAuthorizer_EmptyURL(t *testing.T) {
	_, err := NewBundleAuthorizer(context.Background(), "", "data.test.authz.allow")
	if err == nil {
		t.Error("expected error for empty bundleURL")
	}
}

func TestNewBundleAuthorizer_EmptyEntrypoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, denyAllPolicy)
	}))
	defer srv.Close()

	_, err := NewBundleAuthorizer(context.Background(), srv.URL, "")
	if err == nil {
		t.Error("expected error for empty entrypoint")
	}
}

func TestNewBundleAuthorizer_FetchError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := NewBundleAuthorizer(context.Background(), srv.URL, "data.test.authz.allow")
	if err == nil {
		t.Error("expected error for non-200 bundle response")
	}
}

// ─── fetchBundle ──────────────────────────────────────────────────────────────

func TestFetchBundle_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := fetchBundle(context.Background(), srv.URL, nil)
	if err == nil {
		t.Error("expected error for 404 response")
	}
}

func TestFetchBundle_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, denyAllPolicy)
	}))
	defer srv.Close()

	body, err := fetchBundle(context.Background(), srv.URL, nil)
	if err != nil {
		t.Fatalf("fetchBundle: %v", err)
	}
	if body == "" {
		t.Error("expected non-empty body")
	}
}

// ─── Authorize edge cases ─────────────────────────────────────────────────────

func TestAuthorize_UndefinedResult(t *testing.T) {
	// A policy with no allow rule (no default either) yields an undefined result
	// from OPA, which produces an empty result set — exercises the len(rs)==0 path.
	noDefaultPolicy := `package test.nodefault`
	a, err := NewAuthorizer(context.Background(), noDefaultPolicy, "data.test.nodefault.allow")
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	id := &opaIdentity{userID: "u", claims: map[string]any{}}
	d, err := a.Authorize(context.Background(), id, "read", service.Resource{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Error("expected deny for undefined result")
	}
	if d.Reason != "no OPA decision" {
		t.Errorf("reason = %q, want %q", d.Reason, "no OPA decision")
	}
}

// ─── Authenticate with WithAuthHeader ─────────────────────────────────────────

func TestAuthenticate_CustomAuthHeader(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow",
		WithAuthHeader("x-api-key"),
	)
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	md := metadata.Pairs("x-api-key", "my-opaque-token")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id, err := a.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID() != "my-opaque-token" {
		t.Errorf("expected my-opaque-token, got %q", id.UserID())
	}
}

func TestAuthenticate_JWTMissingSubFallsBackToAnonymous(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	// JWT with no "sub" claim.
	token := mkJWT(t, map[string]any{"email": "no-sub@example.com"})
	md := metadata.Pairs("authorization", "Bearer "+token)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id, err := a.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID() != "anonymous" {
		t.Errorf("expected anonymous for missing sub, got %q", id.UserID())
	}
}

func TestAuthenticate_WithSubjectClaim_CustomField(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow",
		WithSubjectClaim("email"),
	)
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	token := mkJWT(t, map[string]any{"email": "alice@example.com"})
	md := metadata.Pairs("authorization", "Bearer "+token)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id, err := a.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID() != "alice@example.com" {
		t.Errorf("expected alice@example.com, got %q", id.UserID())
	}
}

// ─── WithRSAAlgorithm ─────────────────────────────────────────────────────────

func TestRSAVerifier_WithAlgorithm_RS512(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	signed := buildToken(t, jwa.RS512(), privKey, "rs512-user", time.Now().Add(time.Hour))

	v := NewRSAVerifier(&privKey.PublicKey).WithRSAAlgorithm(jwa.RS512())
	claims, err := v.Verify(context.Background(), string(signed))
	if err != nil {
		t.Fatalf("Verify RS512: %v", err)
	}
	if sub, _ := claims["sub"].(string); sub != "rs512-user" {
		t.Errorf("expected sub=rs512-user, got %q", sub)
	}
}
