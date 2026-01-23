package service

import (
	"errors"

	"github.com/rbaliyan/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toGRPCError converts config errors to gRPC status errors.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	// Check for specific error types
	var keyNotFound *config.KeyNotFoundError
	if errors.As(err, &keyNotFound) {
		return status.Errorf(codes.NotFound, "key not found: %s/%s", keyNotFound.Namespace, keyNotFound.Key)
	}

	var keyExists *config.KeyExistsError
	if errors.As(err, &keyExists) {
		return status.Errorf(codes.AlreadyExists, "key already exists: %s/%s", keyExists.Namespace, keyExists.Key)
	}

	var typeMismatch *config.TypeMismatchError
	if errors.As(err, &typeMismatch) {
		return status.Errorf(codes.InvalidArgument, "type mismatch for key %s: expected %s, got %s",
			typeMismatch.Key, typeMismatch.Expected, typeMismatch.Actual)
	}

	var invalidKey *config.InvalidKeyError
	if errors.As(err, &invalidKey) {
		return status.Errorf(codes.InvalidArgument, "invalid key: %s - %s", invalidKey.Key, invalidKey.Reason)
	}

	var storeErr *config.StoreError
	if errors.As(err, &storeErr) {
		return status.Errorf(codes.Internal, "store error: %s", storeErr.Error())
	}

	// Check sentinel errors
	switch {
	case errors.Is(err, config.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, config.ErrKeyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, config.ErrInvalidKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, config.ErrInvalidNamespace):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, config.ErrInvalidValue):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, config.ErrTypeMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, config.ErrReadOnly):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, config.ErrStoreNotConnected):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, config.ErrStoreClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, config.ErrWatchNotSupported):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, config.ErrCodecNotFound):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
