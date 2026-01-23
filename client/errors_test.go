package client

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFromGRPCError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     error
		wantType any
	}{
		{
			name: "nil error",
			err:  nil,
			want: nil,
		},
		{
			name: "non-grpc error passthrough",
			err:  errors.New("plain error"),
			// Passthrough returns the same error, we check message equality below
		},
		{
			name: "OK returns nil",
			err:  status.Error(codes.OK, "ok"),
			want: nil,
		},
		{
			name: "NotFound",
			err:  status.Error(codes.NotFound, "key not found"),
			want: config.ErrNotFound,
		},
		{
			name: "AlreadyExists",
			err:  status.Error(codes.AlreadyExists, "key exists"),
			want: config.ErrKeyExists,
		},
		{
			name: "InvalidArgument",
			err:  status.Error(codes.InvalidArgument, "invalid"),
			want: config.ErrInvalidValue,
		},
		{
			name: "FailedPrecondition",
			err:  status.Error(codes.FailedPrecondition, "read only"),
			want: config.ErrReadOnly,
		},
		{
			name: "Unavailable",
			err:  status.Error(codes.Unavailable, "unavailable"),
			want: config.ErrStoreNotConnected,
		},
		{
			name: "Unimplemented",
			err:  status.Error(codes.Unimplemented, "not implemented"),
			want: config.ErrWatchNotSupported,
		},
		{
			name:     "PermissionDenied",
			err:      status.Error(codes.PermissionDenied, "access denied"),
			want:     ErrPermissionDenied,
			wantType: &PermissionDeniedError{},
		},
		{
			name:     "Unauthenticated",
			err:      status.Error(codes.Unauthenticated, "not authenticated"),
			want:     ErrPermissionDenied,
			wantType: &PermissionDeniedError{},
		},
		{
			name: "Canceled",
			err:  status.Error(codes.Canceled, "canceled"),
			want: context.Canceled,
		},
		{
			name: "DeadlineExceeded",
			err:  status.Error(codes.DeadlineExceeded, "timeout"),
			want: context.DeadlineExceeded,
		},
		{
			name:     "Internal",
			err:      status.Error(codes.Internal, "internal error"),
			wantType: &RemoteError{},
		},
		{
			name:     "Unknown",
			err:      status.Error(codes.Unknown, "unknown error"),
			wantType: &RemoteError{},
		},
		{
			name: "OutOfRange",
			err:  status.Error(codes.OutOfRange, "out of range"),
			want: config.ErrInvalidValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fromGRPCError(tt.err)

			// Special case: passthrough should return the same error
			if tt.name == "non-grpc error passthrough" {
				if got != tt.err {
					t.Errorf("fromGRPCError() should passthrough non-gRPC errors, got %v", got)
				}
				return
			}

			if tt.want != nil {
				if !errors.Is(got, tt.want) {
					t.Errorf("fromGRPCError() = %v, want %v", got, tt.want)
				}
			}

			if tt.wantType != nil {
				switch tt.wantType.(type) {
				case *PermissionDeniedError:
					var permErr *PermissionDeniedError
					if !errors.As(got, &permErr) {
						t.Errorf("fromGRPCError() type = %T, want *PermissionDeniedError", got)
					}
				case *RemoteError:
					var remoteErr *RemoteError
					if !errors.As(got, &remoteErr) {
						t.Errorf("fromGRPCError() type = %T, want *RemoteError", got)
					}
				}
			}
		})
	}
}

func TestPermissionDeniedError(t *testing.T) {
	t.Run("with message", func(t *testing.T) {
		err := &PermissionDeniedError{Message: "access denied for namespace"}
		if err.Error() != "config: permission denied: access denied for namespace" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
	})

	t.Run("without message", func(t *testing.T) {
		err := &PermissionDeniedError{}
		if err.Error() != "config: permission denied" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
	})

	t.Run("Is ErrPermissionDenied", func(t *testing.T) {
		err := &PermissionDeniedError{Message: "test"}
		if !errors.Is(err, ErrPermissionDenied) {
			t.Error("PermissionDeniedError should match ErrPermissionDenied")
		}
	})
}

func TestRemoteError(t *testing.T) {
	err := &RemoteError{Code: codes.Internal, Message: "server error"}
	expected := "config: remote error (Internal): server error"
	if err.Error() != expected {
		t.Errorf("RemoteError.Error() = %q, want %q", err.Error(), expected)
	}
}
