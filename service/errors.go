package service

import (
	"errors"
	"fmt"

	"github.com/rbaliyan/config"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error reason codes attached to gRPC status details as ErrorInfo.Reason.
// The client uses these to reconstruct the correct sentinel error without
// brittle message parsing. The values are part of the wire protocol: do not
// rename them without a coordinated client update.
const (
	reasonNotFound             = "NOT_FOUND"
	reasonKeyExists            = "KEY_EXISTS"
	reasonInvalidKey           = "INVALID_KEY"
	reasonInvalidNamespace     = "INVALID_NAMESPACE"
	reasonInvalidValue         = "INVALID_VALUE"
	reasonTypeMismatch         = "TYPE_MISMATCH"
	reasonReadOnly             = "READ_ONLY"
	reasonStoreNotConnected    = "STORE_NOT_CONNECTED"
	reasonStoreClosed          = "STORE_CLOSED"
	reasonWatchNotSupported    = "WATCH_NOT_SUPPORTED"
	reasonVersionNotFound      = "VERSION_NOT_FOUND"
	reasonVersioningUnsupported = "VERSIONING_NOT_SUPPORTED"
	reasonAliasExists          = "ALIAS_EXISTS"
	reasonAliasSelf            = "ALIAS_SELF"
	reasonAliasChain           = "ALIAS_CHAIN"
	reasonCodecNotFound        = "CODEC_NOT_FOUND"

	// errorInfoDomain identifies the source of the ErrorInfo. Must match on
	// both client and server.
	errorInfoDomain = "config.rbaliyan.com"
)

// toGRPCError converts config errors to gRPC status errors with an attached
// ErrorInfo detail whose Reason field unambiguously identifies the original
// sentinel. Clients should prefer the ErrorInfo reason over message parsing.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	code, reason, msg := classifyError(err)
	st := status.New(code, msg)
	if reason != "" {
		if detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: reason,
			Domain: errorInfoDomain,
		}); detailErr == nil {
			st = detailed
		}
	}
	return st.Err()
}

// classifyError maps a config error to a gRPC code, a machine-readable reason,
// and a human-readable message. Returns ("", codes.Internal, "internal error")
// when the error cannot be classified.
func classifyError(err error) (codes.Code, string, string) {
	// Typed errors first — they carry structured context.
	var keyNotFound *config.KeyNotFoundError
	if errors.As(err, &keyNotFound) {
		return codes.NotFound, reasonNotFound, "key not found: " + keyNotFound.Namespace + "/" + keyNotFound.Key
	}
	var keyExists *config.KeyExistsError
	if errors.As(err, &keyExists) {
		return codes.AlreadyExists, reasonKeyExists, "key already exists: " + keyExists.Namespace + "/" + keyExists.Key
	}
	var typeMismatch *config.TypeMismatchError
	if errors.As(err, &typeMismatch) {
		return codes.InvalidArgument, reasonTypeMismatch,
			fmt.Sprintf("type mismatch for key %s: expected %s, got %s", typeMismatch.Key, typeMismatch.Expected, typeMismatch.Actual)
	}
	var invalidKey *config.InvalidKeyError
	if errors.As(err, &invalidKey) {
		return codes.InvalidArgument, reasonInvalidKey, fmt.Sprintf("invalid key: %s - %s", invalidKey.Key, invalidKey.Reason)
	}
	var versionNotFound *config.VersionNotFoundError
	if errors.As(err, &versionNotFound) {
		return codes.NotFound, reasonVersionNotFound, "version not found: " + versionNotFound.Namespace + "/" + versionNotFound.Key
	}
	var storeErr *config.StoreError
	if errors.As(err, &storeErr) {
		return codes.Internal, "", "internal store error"
	}

	// Sentinel errors — reason identifies them for the client.
	switch {
	case errors.Is(err, config.ErrNotFound):
		return codes.NotFound, reasonNotFound, err.Error()
	case errors.Is(err, config.ErrKeyExists):
		return codes.AlreadyExists, reasonKeyExists, err.Error()
	case errors.Is(err, config.ErrInvalidKey):
		return codes.InvalidArgument, reasonInvalidKey, err.Error()
	case errors.Is(err, config.ErrInvalidNamespace):
		return codes.InvalidArgument, reasonInvalidNamespace, err.Error()
	case errors.Is(err, config.ErrInvalidValue):
		return codes.InvalidArgument, reasonInvalidValue, err.Error()
	case errors.Is(err, config.ErrTypeMismatch):
		return codes.InvalidArgument, reasonTypeMismatch, err.Error()
	case errors.Is(err, config.ErrReadOnly):
		return codes.FailedPrecondition, reasonReadOnly, err.Error()
	case errors.Is(err, config.ErrStoreNotConnected):
		return codes.Unavailable, reasonStoreNotConnected, err.Error()
	case errors.Is(err, config.ErrStoreClosed):
		return codes.Unavailable, reasonStoreClosed, err.Error()
	case errors.Is(err, config.ErrWatchNotSupported):
		return codes.Unimplemented, reasonWatchNotSupported, err.Error()
	case errors.Is(err, config.ErrVersionNotFound):
		return codes.NotFound, reasonVersionNotFound, err.Error()
	case errors.Is(err, config.ErrVersioningNotSupported):
		return codes.Unimplemented, reasonVersioningUnsupported, err.Error()
	case errors.Is(err, config.ErrAliasExists):
		return codes.AlreadyExists, reasonAliasExists, err.Error()
	case errors.Is(err, config.ErrAliasSelf):
		return codes.InvalidArgument, reasonAliasSelf, err.Error()
	case errors.Is(err, config.ErrAliasChain):
		return codes.InvalidArgument, reasonAliasChain, err.Error()
	case errors.Is(err, config.ErrCodecNotFound):
		return codes.InvalidArgument, reasonCodecNotFound, err.Error()
	default:
		return codes.Internal, "", "internal error"
	}
}
