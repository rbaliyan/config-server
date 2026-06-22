package service

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDenyAllGuard_Authorize(t *testing.T) {
	t.Parallel()

	g := DenyAll()

	// Authenticate must fail with Unauthenticated.
	if _, err := g.Authenticate(context.Background()); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Authenticate: got %v, want Unauthenticated", status.Code(err))
	}

	// Authorize must never allow and must carry a reason.
	dec, err := g.Authorize(context.Background(), anonymousIdentity{}, "read", Resource{})
	if err != nil {
		t.Fatalf("Authorize returned error: %v", err)
	}
	if dec.Allowed {
		t.Error("DenyAll.Authorize returned Allowed=true")
	}
	if dec.Reason == "" {
		t.Error("DenyAll.Authorize returned empty Reason")
	}
}

func TestAnonymousIdentity_Claims(t *testing.T) {
	t.Parallel()

	id := anonymousIdentity{}
	if got := id.UserID(); got != "anonymous" {
		t.Errorf("UserID() = %q, want anonymous", got)
	}
	if claims := id.Claims(); claims != nil {
		t.Errorf("Claims() = %v, want nil", claims)
	}
}

func TestAllowAllGuard(t *testing.T) {
	t.Parallel()

	g := AllowAll()
	id, err := g.Authenticate(context.Background())
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID() != "anonymous" {
		t.Errorf("UserID() = %q, want anonymous", id.UserID())
	}
	dec, err := g.Authorize(context.Background(), id, "write", Resource{Namespace: "ns", Key: "k"})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if !dec.Allowed {
		t.Error("AllowAll.Authorize returned Allowed=false")
	}
}
