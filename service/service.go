package service

import (
	"context"
	"fmt"
	"math"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/codec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements the ConfigService gRPC server.
type Service struct {
	configpb.UnimplementedConfigServiceServer

	store config.Store
	guard SecurityGuard
	opts  serviceOptions
}

// NewService creates a new ConfigService.
// Returns an error if store is nil.
func NewService(store config.Store, opts ...Option) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("config-server: NewService requires a non-nil store")
	}
	o := &serviceOptions{
		guard:              DenyAll(), // Safe default
		maxSnapshotEntries: 10_000,
		maxValueSize:       1 << 20, // 1 MiB
		maxWatchFilters:    100,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Service{
		store: store,
		guard: o.guard,
		opts:  *o,
	}, nil
}

// authorize extracts the Identity from ctx (placed there by AuthInterceptor)
// and calls the guard. If no Identity is in the context (e.g. tests calling
// methods directly without the interceptor), it falls back to
// guard.Authenticate to obtain one.
func (s *Service) authorize(ctx context.Context, action string, resource Resource) error {
	id, ok := IdentityFromContext(ctx)
	if !ok {
		var err error
		id, err = s.guard.Authenticate(ctx)
		if err != nil {
			return err
		}
	}
	decision, err := s.guard.Authorize(ctx, id, action, resource)
	if err != nil {
		return status.Errorf(codes.Internal, "authorization error: %v", err)
	}
	if !decision.Allowed {
		return status.Errorf(codes.PermissionDenied, "%s", decision.Reason)
	}
	return nil
}

// validateNamespaceKey checks that namespace and key are non-empty.
func validateNamespaceKey(namespace, key string) error {
	if namespace == "" {
		return status.Error(codes.InvalidArgument, "namespace is required")
	}
	if key == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	return nil
}

// Get retrieves a configuration value by namespace and key.
func (s *Service) Get(ctx context.Context, req *configpb.GetRequest) (*configpb.GetResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, "read", Resource{Namespace: req.Namespace, Key: req.Key}); err != nil {
		return nil, err
	}

	val, err := s.store.Get(ctx, req.Namespace, req.Key)
	if err != nil {
		return nil, toGRPCError(err)
	}

	entry, err := valueToProto(ctx, req.Namespace, req.Key, val)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &configpb.GetResponse{
		Entry: entry,
	}, nil
}

// Set creates or updates a configuration value.
func (s *Service) Set(ctx context.Context, req *configpb.SetRequest) (*configpb.SetResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, "write", Resource{Namespace: req.Namespace, Key: req.Key}); err != nil {
		return nil, err
	}

	if len(req.Value) > s.opts.maxValueSize {
		return nil, status.Errorf(codes.InvalidArgument,
			"value size %d exceeds maximum allowed size %d", len(req.Value), s.opts.maxValueSize)
	}

	// Build value with write mode
	codecName := req.Codec
	if codecName == "" {
		codecName = "json"
	}

	var opts []config.ValueOption
	switch req.WriteMode {
	case configpb.WriteMode_WRITE_MODE_CREATE:
		opts = append(opts, config.WithValueWriteMode(config.WriteModeCreate))
	case configpb.WriteMode_WRITE_MODE_UPDATE:
		opts = append(opts, config.WithValueWriteMode(config.WriteModeUpdate))
	}

	var val config.Value
	if codec.Get(codecName) != nil {
		var err error
		val, err = config.NewValueFromBytes(ctx, req.Value, codecName, opts...)
		if err != nil {
			return nil, toGRPCError(err)
		}
	} else {
		val = config.NewRawValue(req.Value, codecName, opts...)
	}

	result, err := s.store.Set(ctx, req.Namespace, req.Key, val)
	if err != nil {
		return nil, toGRPCError(err)
	}

	entry, err := valueToProto(ctx, req.Namespace, req.Key, result)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &configpb.SetResponse{
		Entry: entry,
	}, nil
}

// Delete removes a configuration value.
func (s *Service) Delete(ctx context.Context, req *configpb.DeleteRequest) (*configpb.DeleteResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, "delete", Resource{Namespace: req.Namespace, Key: req.Key}); err != nil {
		return nil, err
	}

	if err := s.store.Delete(ctx, req.Namespace, req.Key); err != nil {
		return nil, toGRPCError(err)
	}

	return &configpb.DeleteResponse{}, nil
}

// List returns configuration entries matching a filter.
func (s *Service) List(ctx context.Context, req *configpb.ListRequest) (*configpb.ListResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}
	if err := s.authorize(ctx, "list", Resource{Namespace: req.Namespace}); err != nil {
		return nil, err
	}

	fb := config.NewFilter()
	if req.Prefix != "" {
		fb = fb.WithPrefix(req.Prefix)
	}
	if req.Limit > 0 {
		fb = fb.WithLimit(int(req.Limit))
	}
	if req.Cursor != "" {
		fb = fb.WithCursor(req.Cursor)
	}

	page, err := s.store.Find(ctx, req.Namespace, fb.Build())
	if err != nil {
		return nil, toGRPCError(err)
	}

	entries := make([]*configpb.Entry, 0, len(page.Results()))
	for key, val := range page.Results() {
		entry, err := valueToProto(ctx, req.Namespace, key, val)
		if err != nil {
			return nil, toGRPCError(err)
		}
		entries = append(entries, entry)
	}

	return &configpb.ListResponse{
		Entries:    entries,
		NextCursor: page.NextCursor(),
	}, nil
}

// GetVersions retrieves version history for a configuration key.
func (s *Service) GetVersions(ctx context.Context, req *configpb.GetVersionsRequest) (*configpb.GetVersionsResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, "read", Resource{Namespace: req.Namespace, Key: req.Key}); err != nil {
		return nil, err
	}

	vs, ok := s.store.(config.VersionedStore)
	if !ok {
		return nil, toGRPCError(config.ErrVersioningNotSupported)
	}

	fb := config.NewVersionFilter()
	if req.Version > 0 {
		fb = fb.WithVersion(req.Version)
	}
	if req.Limit > 0 {
		fb = fb.WithLimit(int(req.Limit))
	}
	if req.Cursor != "" {
		fb = fb.WithCursor(req.Cursor)
	}

	page, err := vs.GetVersions(ctx, req.Namespace, req.Key, fb.Build())
	if err != nil {
		return nil, toGRPCError(err)
	}

	entries := make([]*configpb.Entry, 0, len(page.Versions()))
	for _, val := range page.Versions() {
		entry, err := valueToProto(ctx, req.Namespace, req.Key, val)
		if err != nil {
			return nil, toGRPCError(err)
		}
		entries = append(entries, entry)
	}

	return &configpb.GetVersionsResponse{
		Entries:    entries,
		NextCursor: page.NextCursor(),
		Limit:      int32(min(page.Limit(), math.MaxInt32)), // #nosec G115 -- clamped
	}, nil
}

// Watch streams configuration changes in real-time.
func (s *Service) Watch(req *configpb.WatchRequest, stream configpb.ConfigService_WatchServer) error {
	ctx := stream.Context()

	// Authorize watch for each requested namespace, or wildcard if none specified.
	if len(req.Namespaces) == 0 {
		if err := s.authorize(ctx, "watch", Resource{}); err != nil {
			return err
		}
	} else {
		for _, ns := range req.Namespaces {
			if err := s.authorize(ctx, "watch", Resource{Namespace: ns}); err != nil {
				return err
			}
		}
	}

	if len(req.Namespaces) > s.opts.maxWatchFilters {
		return status.Errorf(codes.InvalidArgument, "too many namespaces (max %d, got %d)", s.opts.maxWatchFilters, len(req.Namespaces))
	}
	if len(req.Prefixes) > s.opts.maxWatchFilters {
		return status.Errorf(codes.InvalidArgument, "too many prefixes (max %d, got %d)", s.opts.maxWatchFilters, len(req.Prefixes))
	}

	watchFilter := config.WatchFilter{
		Namespaces: req.Namespaces,
		Prefixes:   req.Prefixes,
	}

	ch, err := s.store.Watch(ctx, watchFilter)
	if err != nil {
		return toGRPCError(err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}

			entry, err := valueToProto(ctx, event.Namespace, event.Key, event.Value)
			if err != nil {
				return toGRPCError(err)
			}

			resp := &configpb.WatchResponse{
				Entry: entry,
			}

			switch event.Type {
			case config.ChangeTypeSet:
				resp.Type = configpb.ChangeType_CHANGE_TYPE_SET
			case config.ChangeTypeDelete:
				resp.Type = configpb.ChangeType_CHANGE_TYPE_DELETE
			case config.ChangeTypeAliasSet:
				resp.Type = configpb.ChangeType_CHANGE_TYPE_ALIAS_SET
			case config.ChangeTypeAliasDelete:
				resp.Type = configpb.ChangeType_CHANGE_TYPE_ALIAS_DELETE
			default:
				// Skip unrecognized change types (e.g. CHANGE_TYPE_UNSPECIFIED)
				continue
			}

			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// CheckAccess verifies the caller's access level for a namespace.
//
// This method uses the SecurityGuard to probe read/write permissions using the
// identity from the context (placed there by AuthInterceptor).
//
// Warning: This endpoint reveals whether a namespace exists and what permissions
// the caller has on it. In production deployments, CheckAccess should be
// rate-limited (e.g., via a gRPC interceptor or API gateway) to prevent
// enumeration of namespace names and permission configurations.
func (s *Service) CheckAccess(ctx context.Context, req *configpb.CheckAccessRequest) (*configpb.CheckAccessResponse, error) {
	resp := &configpb.CheckAccessResponse{}

	id, ok := IdentityFromContext(ctx)
	if !ok {
		return resp, nil
	}

	resource := Resource{Namespace: req.Namespace}
	if decision, err := s.guard.Authorize(ctx, id, "read", resource); err == nil && decision.Allowed {
		resp.CanRead = true
	}
	if decision, err := s.guard.Authorize(ctx, id, "write", resource); err == nil && decision.Allowed {
		resp.CanWrite = true
	}

	return resp, nil
}

// SetAlias creates a new key alias.
func (s *Service) SetAlias(ctx context.Context, req *configpb.SetAliasRequest) (*configpb.SetAliasResponse, error) {
	if req.Alias == "" {
		return nil, status.Error(codes.InvalidArgument, "alias is required")
	}
	if req.Target == "" {
		return nil, status.Error(codes.InvalidArgument, "target is required")
	}

	if err := s.authorize(ctx, "write", Resource{Key: req.Alias}); err != nil {
		return nil, err
	}

	as, ok := s.store.(config.AliasStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "store does not support aliases")
	}

	val, err := as.SetAlias(ctx, req.Alias, req.Target)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &configpb.SetAliasResponse{
		Alias: aliasToProto(req.Alias, req.Target, val),
	}, nil
}

// DeleteAlias removes a key alias.
func (s *Service) DeleteAlias(ctx context.Context, req *configpb.DeleteAliasRequest) (*configpb.DeleteAliasResponse, error) {
	if req.Alias == "" {
		return nil, status.Error(codes.InvalidArgument, "alias is required")
	}
	if err := s.authorize(ctx, "delete", Resource{Key: req.Alias}); err != nil {
		return nil, err
	}

	as, ok := s.store.(config.AliasStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "store does not support aliases")
	}

	if err := as.DeleteAlias(ctx, req.Alias); err != nil {
		return nil, toGRPCError(err)
	}

	return &configpb.DeleteAliasResponse{}, nil
}

// GetAlias retrieves a specific alias.
func (s *Service) GetAlias(ctx context.Context, req *configpb.GetAliasRequest) (*configpb.GetAliasResponse, error) {
	if req.Alias == "" {
		return nil, status.Error(codes.InvalidArgument, "alias is required")
	}
	if err := s.authorize(ctx, "read", Resource{Key: req.Alias}); err != nil {
		return nil, err
	}

	as, ok := s.store.(config.AliasStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "store does not support aliases")
	}

	val, err := as.GetAlias(ctx, req.Alias)
	if err != nil {
		return nil, toGRPCError(err)
	}

	target, _ := val.String()
	return &configpb.GetAliasResponse{
		Alias: aliasToProto(req.Alias, target, val),
	}, nil
}

// ListAliases returns all registered aliases.
func (s *Service) ListAliases(ctx context.Context, req *configpb.ListAliasesRequest) (*configpb.ListAliasesResponse, error) {
	if err := s.authorize(ctx, "list", Resource{}); err != nil {
		return nil, err
	}

	as, ok := s.store.(config.AliasStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "store does not support aliases")
	}

	aliases, err := as.ListAliases(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	result := make([]*configpb.Alias, 0, len(aliases))
	for alias, val := range aliases {
		target, _ := val.String()
		result = append(result, aliasToProto(alias, target, val))
	}

	return &configpb.ListAliasesResponse{
		Aliases: result,
	}, nil
}

// aliasToProto converts an alias to its proto representation.
func aliasToProto(alias, target string, val config.Value) *configpb.Alias {
	a := &configpb.Alias{
		Alias:  alias,
		Target: target,
	}
	if val != nil {
		if meta := val.Metadata(); meta != nil {
			a.Version = meta.Version()
			if !meta.CreatedAt().IsZero() {
				a.CreatedAt = timestamppb.New(meta.CreatedAt())
			}
		}
	}
	return a
}

// valueToProto converts a config.Value to a proto Entry.
// Returns an error if the value cannot be marshaled.
func valueToProto(ctx context.Context, namespace, key string, val config.Value) (*configpb.Entry, error) {
	if val == nil {
		return &configpb.Entry{
			Namespace: namespace,
			Key:       key,
		}, nil
	}

	entry := &configpb.Entry{
		Namespace: namespace,
		Key:       key,
		Codec:     val.Codec(),
		Type:      int32(val.Type()), // #nosec G115 -- Type is a small enum
	}

	// Marshal value to bytes
	data, err := val.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("marshal value %s/%s: %w", namespace, key, err)
	}
	entry.Value = data

	// Add metadata
	if meta := val.Metadata(); meta != nil {
		entry.Version = meta.Version()
		if !meta.CreatedAt().IsZero() {
			entry.CreatedAt = timestamppb.New(meta.CreatedAt())
		}
		if !meta.UpdatedAt().IsZero() {
			entry.UpdatedAt = timestamppb.New(meta.UpdatedAt())
		}
	}

	return entry, nil
}
