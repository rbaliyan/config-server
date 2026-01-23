// Package service provides the gRPC ConfigService implementation.
package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Operation represents a config operation for authorization.
type Operation int

const (
	OperationRead Operation = iota
	OperationWrite
	OperationDelete
	OperationList
	OperationWatch
)

func (o Operation) String() string {
	switch o {
	case OperationRead:
		return "read"
	case OperationWrite:
		return "write"
	case OperationDelete:
		return "delete"
	case OperationList:
		return "list"
	case OperationWatch:
		return "watch"
	default:
		return "unknown"
	}
}

// AuthRequest contains information about an authorization request.
type AuthRequest struct {
	Namespace string
	Key       string // Empty for List/Watch operations
	Operation Operation
}

// Authorizer defines the interface for authorization decisions.
// Implement this interface to integrate with your auth system.
//
// The context contains any authentication information extracted
// by your authentication middleware (e.g., user ID, roles, claims).
//
// Example implementation:
//
//	type RBACAuthorizer struct {
//	    permissions map[string][]string // role -> allowed namespaces
//	}
//
//	func (a *RBACAuthorizer) Authorize(ctx context.Context, req AuthRequest) error {
//	    role := GetRoleFromContext(ctx)
//	    if !a.canAccess(role, req.Namespace, req.Operation) {
//	        return status.Errorf(codes.PermissionDenied, "access denied")
//	    }
//	    return nil
//	}
type Authorizer interface {
	// Authorize checks if the operation is allowed.
	// Return nil to allow, or an error (typically codes.PermissionDenied) to deny.
	Authorize(ctx context.Context, req AuthRequest) error
}

// AllowAll returns an authorizer that permits all operations.
// Use only for development/testing.
func AllowAll() Authorizer {
	return allowAllAuthorizer{}
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(context.Context, AuthRequest) error {
	return nil
}

// DenyAll returns an authorizer that denies all operations.
// Useful as a fallback when no authorizer is configured.
func DenyAll() Authorizer {
	return denyAllAuthorizer{}
}

type denyAllAuthorizer struct{}

func (denyAllAuthorizer) Authorize(context.Context, AuthRequest) error {
	return status.Errorf(codes.PermissionDenied, "no authorizer configured")
}
