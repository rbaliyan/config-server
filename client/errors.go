package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/rbaliyan/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrPermissionDenied is returned when the server denies access to a resource.
var ErrPermissionDenied = errors.New("config: permission denied")

// PermissionDeniedError provides details about a permission denial.
type PermissionDeniedError struct {
	Message string
}

func (e *PermissionDeniedError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("config: permission denied: %s", e.Message)
	}
	return "config: permission denied"
}

func (e *PermissionDeniedError) Is(target error) bool {
	return target == ErrPermissionDenied
}

// RemoteError wraps an error from the remote config server.
type RemoteError struct {
	Code    codes.Code
	Message string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("config: remote error (%s): %s", e.Code, e.Message)
}

// fromGRPCError converts gRPC status errors to config errors.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch st.Code() {
	case codes.OK:
		return nil

	case codes.NotFound:
		return config.ErrNotFound

	case codes.AlreadyExists:
		return config.ErrKeyExists

	case codes.InvalidArgument:
		return config.ErrInvalidValue

	case codes.FailedPrecondition:
		return config.ErrReadOnly

	case codes.Unavailable:
		return config.ErrStoreNotConnected

	case codes.Unimplemented:
		return config.ErrWatchNotSupported

	case codes.PermissionDenied, codes.Unauthenticated:
		return &PermissionDeniedError{Message: st.Message()}

	case codes.Canceled:
		return context.Canceled

	case codes.DeadlineExceeded:
		return context.DeadlineExceeded

	case codes.Internal, codes.Unknown, codes.DataLoss:
		return &RemoteError{Code: st.Code(), Message: st.Message()}

	case codes.ResourceExhausted:
		return &RemoteError{Code: st.Code(), Message: st.Message()}

	case codes.Aborted:
		// Typically indicates a conflict/retry situation
		return &RemoteError{Code: st.Code(), Message: st.Message()}

	case codes.OutOfRange:
		return config.ErrInvalidValue

	default:
		return &RemoteError{Code: st.Code(), Message: st.Message()}
	}
}
