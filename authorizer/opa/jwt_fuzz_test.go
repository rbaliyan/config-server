package opa

import (
	"context"
	"strings"
	"testing"
)

// FuzzVerifyToken fuzzes HMACVerifier.Verify over untrusted token strings. The
// token is taken verbatim from the incoming "authorization" metadata in
// Authenticate, so the claim-parsing path (jwt.Parse + tokenClaims) processes
// fully attacker-controlled input. The invariants are:
//
//   - Verify must never panic on any token string.
//   - For any token that does not carry a valid HS256 signature over the fixed
//     secret, Verify must return a non-nil error and nil claims (it must never
//     accept an unsigned/forged token, and must never return claims alongside an
//     error).
//
// The verifier is built once with a fixed secret. Because the fuzzer cannot
// (with overwhelming probability) synthesize a valid HMAC signature for that
// secret, every fuzz-discovered input is expected to be rejected; the oracle
// asserts the rejection is clean (error set, claims nil).
func FuzzVerifyToken(f *testing.F) {
	// Realistic + adversarial seeds.
	f.Add("")                 // empty
	f.Add("a.b.c")            // three opaque segments
	f.Add("a.b")              // too few segments
	f.Add("a.b.c.d")          // too many segments
	f.Add("....")             // only dots
	f.Add("not-a-jwt-at-all") // no segments
	f.Add("Bearer something") // leftover scheme prefix
	// A real-ish JWT header.payload.sig (HS256, but wrong/unknown signature):
	// {"alg":"HS256","typ":"JWT"}.{"sub":"attacker","exp":9999999999}.<sig>
	f.Add("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." +
		"eyJzdWIiOiJhdHRhY2tlciIsImV4cCI6OTk5OTk5OTk5OX0." +
		"c2lnbmF0dXJlLXRoYXQtaXMtbm90LXZhbGlk")
	// alg:none confusion attempt: {"alg":"none","typ":"JWT"}.{"sub":"admin"}.
	f.Add("eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0." +
		"eyJzdWIiOiJhZG1pbiJ9.")
	// alg:none with no trailing dot.
	f.Add("eyJhbGciOiJub25lIn0.eyJzdWIiOiJhZG1pbiJ9")
	// Bad base64 in each position.
	f.Add("!!!.eyJzdWIiOiJ4In0.sig")
	f.Add("eyJhbGciOiJIUzI1NiJ9.!!!.sig")
	f.Add("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.!!!")
	// Padding/whitespace abuse.
	f.Add("  eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.sig  ")
	f.Add("\n\t.\n\t.\n\t")
	// Oversized token.
	f.Add(strings.Repeat("A", 100000))
	f.Add(strings.Repeat("a.b.c.", 5000))
	// Unicode / non-ASCII.
	f.Add("日本語.токен.署名")
	f.Add("\x00\x01\x02.\xff\xfe.\x00")
	// JSON injection inside a base64 payload boundary.
	f.Add("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOnsiJG5lIjpudWxsfX0.sig")
	// Deeply nested / huge claim attempt.
	f.Add("eyJhbGciOiJIUzI1NiJ9." + strings.Repeat("eyJhIjoxfQ", 1000) + ".sig")

	v := NewHMACVerifier([]byte("fixed-fuzz-secret-key-do-not-change"))

	f.Fuzz(func(t *testing.T, token string) {
		claims, err := v.Verify(context.Background(), token)

		// Invariant: a fuzzed token cannot be a validly signed HS256 token for
		// our secret, so Verify must reject it.
		if err == nil {
			t.Fatalf("Verify accepted an unsigned/forged token %q (claims=%v)", token, claims)
		}
		// Invariant: on error, claims must be nil — never return a partial or
		// non-nil claim map alongside an error.
		if claims != nil {
			t.Fatalf("Verify returned non-nil claims %v together with error %v for token %q",
				claims, err, token)
		}
	})
}
