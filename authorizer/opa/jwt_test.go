package opa

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"google.golang.org/grpc/metadata"
)

// buildToken creates and signs a JWT for testing.
func buildToken(t *testing.T, alg jwa.SignatureAlgorithm, key any, subject string, expiry time.Time) []byte {
	t.Helper()
	b := jwt.NewBuilder().Subject(subject).IssuedAt(time.Now())
	if !expiry.IsZero() {
		b = b.Expiration(expiry)
	}
	tok, err := b.Build()
	if err != nil {
		t.Fatalf("jwt build: %v", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(alg, key))
	if err != nil {
		t.Fatalf("jwt sign: %v", err)
	}
	return signed
}

// ─── HMAC tests ───────────────────────────────────────────────────────────────

func TestHMACVerifier_Valid(t *testing.T) {
	secret := []byte("super-secret-key")
	signed := buildToken(t, jwa.HS256(), secret, "user-hmac", time.Now().Add(time.Hour))

	v := NewHMACVerifier(secret)
	claims, err := v.Verify(context.Background(), string(signed))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if sub, _ := claims["sub"].(string); sub != "user-hmac" {
		t.Errorf("expected sub=user-hmac, got %q", sub)
	}
}

func TestHMACVerifier_WrongKey(t *testing.T) {
	signed := buildToken(t, jwa.HS256(), []byte("correct-key"), "u", time.Now().Add(time.Hour))
	v := NewHMACVerifier([]byte("wrong-key"))
	if _, err := v.Verify(context.Background(), string(signed)); err == nil {
		t.Fatal("expected error for wrong HMAC key")
	}
}

func TestHMACVerifier_Expired(t *testing.T) {
	secret := []byte("secret")
	// Build token with exp in the past.
	tok, err := jwt.NewBuilder().
		Subject("user").
		IssuedAt(time.Now().Add(-2 * time.Hour)).
		Expiration(time.Now().Add(-time.Hour)).
		Build()
	if err != nil {
		t.Fatalf("jwt build: %v", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256(), secret))
	if err != nil {
		t.Fatalf("jwt sign: %v", err)
	}

	v := NewHMACVerifier(secret)
	if _, err := v.Verify(context.Background(), string(signed)); err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestHMACVerifier_WithAlgorithm_HS512(t *testing.T) {
	secret := []byte("hs512-secret")
	tok, err := jwt.NewBuilder().Subject("u512").IssuedAt(time.Now()).Expiration(time.Now().Add(time.Hour)).Build()
	if err != nil {
		t.Fatalf("jwt build: %v", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.HS512(), secret))
	if err != nil {
		t.Fatalf("jwt sign: %v", err)
	}

	v := NewHMACVerifier(secret).WithHMACAlgorithm(jwa.HS512())
	claims, err := v.Verify(context.Background(), string(signed))
	if err != nil {
		t.Fatalf("Verify HS512: %v", err)
	}
	if sub, _ := claims["sub"].(string); sub != "u512" {
		t.Errorf("expected sub=u512, got %q", sub)
	}
}

// ─── RSA tests ────────────────────────────────────────────────────────────────

func TestRSAVerifier_Valid(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	signed := buildToken(t, jwa.RS256(), privKey, "rsa-user", time.Now().Add(time.Hour))

	v := NewRSAVerifier(&privKey.PublicKey)
	claims, err := v.Verify(context.Background(), string(signed))
	if err != nil {
		t.Fatalf("Verify RSA: %v", err)
	}
	if sub, _ := claims["sub"].(string); sub != "rsa-user" {
		t.Errorf("expected sub=rsa-user, got %q", sub)
	}
}

func TestRSAVerifier_WrongKey(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey other: %v", err)
	}

	signed := buildToken(t, jwa.RS256(), privKey, "u", time.Now().Add(time.Hour))
	v := NewRSAVerifier(&otherKey.PublicKey)
	if _, err := v.Verify(context.Background(), string(signed)); err == nil {
		t.Fatal("expected error for wrong RSA public key")
	}
}

// ─── JWKS tests ───────────────────────────────────────────────────────────────

func TestJWKSVerifier_Valid(t *testing.T) {
	const kid = "test-kid-1"

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}

	// Build the JWKS from the public key.
	pubJWK, err := jwk.Import(privKey.Public())
	if err != nil {
		t.Fatalf("jwk.Import: %v", err)
	}
	if err := pubJWK.Set(jwk.KeyIDKey, kid); err != nil {
		t.Fatalf("pubJWK.Set kid: %v", err)
	}
	if err := pubJWK.Set(jwk.AlgorithmKey, jwa.RS256()); err != nil {
		t.Fatalf("pubJWK.Set alg: %v", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(pubJWK); err != nil {
		t.Fatalf("set.AddKey: %v", err)
	}
	jwksBytes, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("json.Marshal(set): %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBytes) //nolint:errcheck
	}))
	defer srv.Close()

	// Sign the token with kid in the protected header.
	tok, err := jwt.NewBuilder().Subject("jwks-user").IssuedAt(time.Now()).Expiration(time.Now().Add(time.Hour)).Build()
	if err != nil {
		t.Fatalf("jwt build: %v", err)
	}
	hdrs := jws.NewHeaders()
	if err := hdrs.Set(jws.KeyIDKey, kid); err != nil {
		t.Fatalf("hdrs.Set kid: %v", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), privKey, jws.WithProtectedHeaders(hdrs)))
	if err != nil {
		t.Fatalf("jwt sign: %v", err)
	}

	v := NewJWKSVerifier(srv.URL)
	claims, err := v.Verify(context.Background(), string(signed))
	if err != nil {
		t.Fatalf("JWKSVerifier.Verify: %v", err)
	}
	if sub, _ := claims["sub"].(string); sub != "jwks-user" {
		t.Errorf("expected sub=jwks-user, got %q", sub)
	}
}

func TestJWKSVerifier_BadURL(t *testing.T) {
	v := NewJWKSVerifier("http://127.0.0.1:0/no-such-server")
	_, err := v.Verify(context.Background(), "any.token.here")
	if err == nil {
		t.Fatal("expected error for bad JWKS URL")
	}
}

// ─── Integration with Authenticate ────────────────────────────────────────────

func TestAuthenticate_WithJWTVerifier_Valid(t *testing.T) {
	secret := []byte("auth-secret")
	signed := buildToken(t, jwa.HS256(), secret, "verified-user", time.Now().Add(time.Hour))

	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow",
		WithJWTVerifier(NewHMACVerifier(secret)),
	)
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	md := metadata.Pairs("authorization", "Bearer "+string(signed))
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id, err := a.Authenticate(ctx)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID() != "verified-user" {
		t.Errorf("expected userID=verified-user, got %q", id.UserID())
	}
}

func TestAuthenticate_WithJWTVerifier_InvalidToken(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow",
		WithJWTVerifier(NewHMACVerifier([]byte("correct-secret"))),
	)
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	// Sign with a different secret so verification fails.
	signed := buildToken(t, jwa.HS256(), []byte("wrong-secret"), "attacker", time.Now().Add(time.Hour))

	md := metadata.Pairs("authorization", "Bearer "+string(signed))
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err = a.Authenticate(ctx)
	if err == nil {
		t.Fatal("expected Authenticate to return error for invalid token")
	}
}
