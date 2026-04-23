package opa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/rbaliyan/config-server/service"
	"google.golang.org/grpc/metadata"
)

const denyAllPolicy = `
package test.authz
default allow = false
`

const allowReadPolicy = `
package test.authz
default allow = false
allow if {
	input.action == "read"
	input.identity.user_id != ""
}
`

func mkJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(body)
	return header + "." + payload + ".sig"
}

func TestNewAuthorizer_EmptyArgs(t *testing.T) {
	ctx := context.Background()
	if _, err := NewAuthorizer(ctx, "", "data.test.authz.allow"); err == nil {
		t.Error("expected error for empty policy")
	}
	if _, err := NewAuthorizer(ctx, denyAllPolicy, ""); err == nil {
		t.Error("expected error for empty entrypoint")
	}
}

func TestAuthenticate_AnonymousWithoutMetadata(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	id, err := a.Authenticate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID() != "anonymous" {
		t.Fatalf("expected anonymous, got %q", id.UserID())
	}
}

func TestAuthenticate_OpaqueToken(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	md := metadata.Pairs("authorization", "Bearer opaque-tok")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id, err := a.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID() != "opaque-tok" {
		t.Fatalf("expected opaque-tok, got %q", id.UserID())
	}
}

func TestAuthenticate_JWT(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatalf("NewAuthorizer: %v", err)
	}
	defer a.Close()

	token := mkJWT(t, map[string]any{"sub": "user-123", "email": "u@example.com"})
	md := metadata.Pairs("authorization", "Bearer "+token)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id, err := a.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID() != "user-123" {
		t.Fatalf("expected user-123, got %q", id.UserID())
	}
	if id.Claims()["email"] != "u@example.com" {
		t.Fatalf("expected claims to contain email, got %v", id.Claims())
	}
}

func TestAuthorize_DenyAll(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	id := &opaIdentity{userID: "u", claims: map[string]any{}}
	d, err := a.Authorize(context.Background(), id, "read", service.Resource{Namespace: "ns", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed {
		t.Fatal("expected decision=false")
	}
}

func TestAuthorize_AllowRead(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), allowReadPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	id := &opaIdentity{userID: "u-1", claims: map[string]any{}}
	d, err := a.Authorize(context.Background(), id, "read", service.Resource{Namespace: "ns", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed {
		t.Fatalf("expected allow for read, got deny (reason=%q)", d.Reason)
	}

	// Writing with the same policy should be denied.
	dWrite, err := a.Authorize(context.Background(), id, "write", service.Resource{Namespace: "ns", Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if dWrite.Allowed {
		t.Fatal("expected deny for write")
	}
}

func TestClose_Idempotent(t *testing.T) {
	a, err := NewAuthorizer(context.Background(), denyAllPolicy, "data.test.authz.allow")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil { // must not hang or panic
		t.Fatal(err)
	}
}

func TestParseJWTClaims_Malformed(t *testing.T) {
	if _, err := parseJWTClaims("not.a"); err == nil {
		t.Error("expected error for 2-part token")
	}
	if _, err := parseJWTClaims("a.b.c"); err == nil {
		t.Error("expected error for non-base64 payload")
	}
}
