package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rbaliyan/config"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

// Error reason codes recognised on inbound gRPC status details. These values
// must match the service package's wire-format constants. A missing or
// unrecognised reason triggers the legacy code/message-based fallback so that
// older servers without ErrorInfo remain compatible.
const (
	reasonNotFound              = "NOT_FOUND"
	reasonKeyExists             = "KEY_EXISTS"
	reasonInvalidKey            = "INVALID_KEY"
	reasonInvalidNamespace      = "INVALID_NAMESPACE"
	reasonInvalidValue          = "INVALID_VALUE"
	reasonTypeMismatch          = "TYPE_MISMATCH"
	reasonReadOnly              = "READ_ONLY"
	reasonStoreNotConnected     = "STORE_NOT_CONNECTED"
	reasonStoreClosed           = "STORE_CLOSED"
	reasonWatchNotSupported     = "WATCH_NOT_SUPPORTED"
	reasonVersionNotFound       = "VERSION_NOT_FOUND"
	reasonVersioningUnsupported = "VERSIONING_NOT_SUPPORTED"
	reasonAliasExists           = "ALIAS_EXISTS"
	reasonAliasSelf             = "ALIAS_SELF"
	reasonAliasChain            = "ALIAS_CHAIN"
	reasonCodecNotFound         = "CODEC_NOT_FOUND"

	errorInfoDomain = "config.rbaliyan.com"
)

// fromGRPCError converts gRPC status errors to config errors. When the server
// attaches an errdetails.ErrorInfo with a known reason, the mapping is
// unambiguous; otherwise the code + message are inspected as a fallback for
// older servers.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	if mapped, ok := errorFromDetails(st); ok {
		return mapped
	}
	return errorFromCode(st)
}

// errorFromDetails returns the sentinel error carried by an ErrorInfo detail,
// or (nil, false) if no recognised reason is present.
func errorFromDetails(st *status.Status) (error, bool) {
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != errorInfoDomain {
			continue
		}
		switch info.GetReason() {
		case reasonNotFound:
			return config.ErrNotFound, true
		case reasonKeyExists:
			return config.ErrKeyExists, true
		case reasonInvalidKey:
			return config.ErrInvalidKey, true
		case reasonInvalidNamespace:
			return config.ErrInvalidNamespace, true
		case reasonInvalidValue:
			return config.ErrInvalidValue, true
		case reasonTypeMismatch:
			return config.ErrTypeMismatch, true
		case reasonReadOnly:
			return config.ErrReadOnly, true
		case reasonStoreNotConnected:
			return config.ErrStoreNotConnected, true
		case reasonStoreClosed:
			return config.ErrStoreClosed, true
		case reasonWatchNotSupported:
			return config.ErrWatchNotSupported, true
		case reasonVersionNotFound:
			return config.ErrVersionNotFound, true
		case reasonVersioningUnsupported:
			return config.ErrVersioningNotSupported, true
		case reasonAliasExists:
			return config.ErrAliasExists, true
		case reasonAliasSelf:
			return config.ErrAliasSelf, true
		case reasonAliasChain:
			return config.ErrAliasChain, true
		case reasonCodecNotFound:
			return config.ErrCodecNotFound, true
		}
	}
	return nil, false
}

// errorFromCode is the legacy code+message-based mapping, kept for
// compatibility with servers that do not attach ErrorInfo. When the server
// does attach ErrorInfo, errorFromDetails supersedes this.
func errorFromCode(st *status.Status) error {
	switch st.Code() {
	case codes.OK:
		return nil

	case codes.NotFound:
		if strings.Contains(st.Message(), "version not found") {
			return config.ErrVersionNotFound
		}
		return config.ErrNotFound

	case codes.AlreadyExists:
		if strings.Contains(st.Message(), "alias") {
			return config.ErrAliasExists
		}
		return config.ErrKeyExists

	case codes.InvalidArgument:
		return config.ErrInvalidValue

	case codes.FailedPrecondition:
		return config.ErrReadOnly

	case codes.Unavailable:
		return config.ErrStoreNotConnected

	case codes.Unimplemented:
		if strings.Contains(st.Message(), "versioning") {
			return config.ErrVersioningNotSupported
		}
		return config.ErrWatchNotSupported

	case codes.PermissionDenied, codes.Unauthenticated:
		return &PermissionDeniedError{Message: st.Message()}

	case codes.Canceled:
		return context.Canceled

	case codes.DeadlineExceeded:
		return context.DeadlineExceeded

	case codes.OutOfRange:
		return config.ErrInvalidValue

	default:
		return &RemoteError{Code: st.Code(), Message: st.Message()}
	}
}
