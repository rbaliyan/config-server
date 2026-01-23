package service

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAllowAll(t *testing.T) {
	auth := AllowAll()

	req := AuthRequest{
		Namespace: "test",
		Key:       "key",
		Operation: OperationRead,
	}

	if err := auth.Authorize(context.Background(), req); err != nil {
		t.Errorf("AllowAll should permit all requests, got error: %v", err)
	}
}

func TestDenyAll(t *testing.T) {
	auth := DenyAll()

	req := AuthRequest{
		Namespace: "test",
		Key:       "key",
		Operation: OperationRead,
	}

	err := auth.Authorize(context.Background(), req)
	if err == nil {
		t.Fatal("DenyAll should deny all requests")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}

	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got: %v", st.Code())
	}
}

func TestOperationString(t *testing.T) {
	tests := []struct {
		op   Operation
		want string
	}{
		{OperationRead, "read"},
		{OperationWrite, "write"},
		{OperationDelete, "delete"},
		{OperationList, "list"},
		{OperationWatch, "watch"},
		{Operation(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.op.String()
		if got != tt.want {
			t.Errorf("Operation(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}
