package opa

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// JWTVerifier validates a JWT token string and returns its claims.
// When set via WithJWTVerifier, it replaces the default no-op claim parser in
// Authenticate so that signature, expiry, and audience checks are enforced.
type JWTVerifier interface {
	// Verify parses and validates token, returning the claims map on success.
	Verify(ctx context.Context, token string) (map[string]any, error)
}

// HMACVerifier verifies tokens signed with an HMAC-SHA algorithm (HS256/384/512).
type HMACVerifier struct {
	alg    jwa.SignatureAlgorithm
	secret []byte
}

// NewHMACVerifier returns a JWTVerifier that accepts tokens signed with the
// provided HMAC secret using HS256. Change the algorithm with WithAlgorithm if
// the issuer uses HS384 or HS512.
func NewHMACVerifier(secret []byte) *HMACVerifier {
	return &HMACVerifier{alg: jwa.HS256(), secret: secret}
}

// WithHMACAlgorithm changes the expected signature algorithm (default HS256).
func (v *HMACVerifier) WithHMACAlgorithm(alg jwa.SignatureAlgorithm) *HMACVerifier {
	v.alg = alg
	return v
}

// Verify implements JWTVerifier.
func (v *HMACVerifier) Verify(_ context.Context, token string) (map[string]any, error) {
	t, err := jwt.Parse([]byte(token), jwt.WithKey(v.alg, v.secret), jwt.WithValidate(true))
	if err != nil {
		return nil, fmt.Errorf("opa: jwt hmac verify: %w", err)
	}
	return tokenClaims(t)
}

// RSAVerifier verifies tokens signed with an RSA algorithm (RS256/RS384/RS512).
type RSAVerifier struct {
	alg jwa.SignatureAlgorithm
	key *rsa.PublicKey
}

// NewRSAVerifier returns a JWTVerifier that accepts tokens signed with the
// provided RSA public key using RS256. Change the algorithm with
// WithRSAAlgorithm if the issuer uses RS384 or RS512.
func NewRSAVerifier(key *rsa.PublicKey) *RSAVerifier {
	return &RSAVerifier{alg: jwa.RS256(), key: key}
}

// WithRSAAlgorithm changes the expected signature algorithm (default RS256).
func (v *RSAVerifier) WithRSAAlgorithm(alg jwa.SignatureAlgorithm) *RSAVerifier {
	v.alg = alg
	return v
}

// Verify implements JWTVerifier.
func (v *RSAVerifier) Verify(_ context.Context, token string) (map[string]any, error) {
	t, err := jwt.Parse([]byte(token), jwt.WithKey(v.alg, v.key), jwt.WithValidate(true))
	if err != nil {
		return nil, fmt.Errorf("opa: jwt rsa verify: %w", err)
	}
	return tokenClaims(t)
}

// JWKSVerifier fetches the issuer's JWKS on every call and verifies the token
// against the matching key. Suitable for use with OIDC providers (Google,
// Auth0, etc.) that rotate keys via a published JWKS endpoint.
//
// For high-traffic deployments, consider wrapping the verifier with a caching
// layer or using a longer-lived jwk.Cache externally.
type JWKSVerifier struct {
	url string
}

// NewJWKSVerifier returns a JWTVerifier that fetches the JWKS from url on
// every Verify call and validates the token signature against the matching key.
func NewJWKSVerifier(url string) *JWKSVerifier {
	return &JWKSVerifier{url: url}
}

// Verify implements JWTVerifier.
func (v *JWKSVerifier) Verify(ctx context.Context, token string) (map[string]any, error) {
	set, err := jwk.Fetch(ctx, v.url)
	if err != nil {
		return nil, fmt.Errorf("opa: jwks fetch %s: %w", v.url, err)
	}
	t, err := jwt.Parse([]byte(token), jwt.WithKeySet(set), jwt.WithValidate(true))
	if err != nil {
		return nil, fmt.Errorf("opa: jwt jwks verify: %w", err)
	}
	return tokenClaims(t)
}

// tokenClaims extracts all claims from a parsed token as map[string]any by
// round-tripping through JSON. This is the most future-proof way to extract
// arbitrary private claims without depending on Token's internal structure.
func tokenClaims(t jwt.Token) (map[string]any, error) {
	raw, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("opa: marshal jwt claims: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, fmt.Errorf("opa: unmarshal jwt claims: %w", err)
	}
	return claims, nil
}
