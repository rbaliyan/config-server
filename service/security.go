// Package service provides the gRPC ConfigService implementation.
package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Identity represents an authenticated caller.
type Identity interface {
	UserID() string
	Claims() map[string]any
}

// Decision represents an authorization decision.
type Decision struct {
	Allowed bool
	Scope   string
	Reason  string
}

// Resource identifies the config resource being accessed. Empty fields mean
// the check is method-level only (e.g. ListAliases has no namespace or key).
type Resource struct {
	Namespace string
	Key       string
}

// SecurityGuard handles both authentication and authorization.
type SecurityGuard interface {
	// Authenticate extracts and validates the caller's identity from the context.
	Authenticate(ctx context.Context) (Identity, error)

	// Authorize checks whether the identified caller may perform the given action
	// on the given resource. Action is one of "read", "write", "delete", "list",
	// "watch". Resource carries the namespace and/or key when known.
	Authorize(ctx context.Context, id Identity, action string, resource Resource) (Decision, error)
}

type identityKey struct{}

// IdentityFromContext retrieves the Identity stored in the context.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

// ContextWithIdentity returns a new context carrying the given Identity.
func ContextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// AllowAll returns a SecurityGuard that authenticates every request as an
// anonymous identity and allows all actions. Use only for development/testing.
func AllowAll() SecurityGuard {
	return allowAllGuard{}
}

type allowAllGuard struct{}

func (allowAllGuard) Authenticate(context.Context) (Identity, error) {
	return anonymousIdentity{}, nil
}

func (allowAllGuard) Authorize(context.Context, Identity, string, Resource) (Decision, error) {
	return Decision{Allowed: true}, nil
}

// DenyAll returns a SecurityGuard whose Authenticate always fails.
// Useful as a safe default when no guard is configured.
func DenyAll() SecurityGuard {
	return denyAllGuard{}
}

type denyAllGuard struct{}

func (denyAllGuard) Authenticate(context.Context) (Identity, error) {
	return nil, status.Errorf(codes.Unauthenticated, "no security guard configured")
}

func (denyAllGuard) Authorize(context.Context, Identity, string, Resource) (Decision, error) {
	return Decision{Allowed: false, Reason: "no security guard configured"}, nil
}

// anonymousIdentity is used by AllowAll.
type anonymousIdentity struct{}

func (anonymousIdentity) UserID() string         { return "anonymous" }
func (anonymousIdentity) Claims() map[string]any { return nil }
