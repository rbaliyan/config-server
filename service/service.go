package service

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements the ConfigService gRPC server.
type Service struct {
	configpb.UnimplementedConfigServiceServer

	store      config.Store
	authorizer Authorizer
}

// NewService creates a new ConfigService.
// Panics if store is nil.
func NewService(store config.Store, opts ...Option) *Service {
	if store == nil {
		panic("config-server: NewService requires a non-nil store")
	}
	o := &serviceOptions{
		authorizer: DenyAll(), // Safe default
	}
	for _, opt := range opts {
		opt(o)
	}
	return &Service{
		store:      store,
		authorizer: o.authorizer,
	}
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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Key:       req.Key,
		Operation: OperationRead,
	}); err != nil {
		return nil, err
	}

	val, err := s.store.Get(ctx, req.Namespace, req.Key)
	if err != nil {
		return nil, toGRPCError(err)
	}

	entry, err := valueToProto(req.Namespace, req.Key, val)
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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Key:       req.Key,
		Operation: OperationWrite,
	}); err != nil {
		return nil, err
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

	val, err := config.NewValueFromBytes(req.Value, codecName, opts...)
	if err != nil {
		return nil, toGRPCError(err)
	}

	result, err := s.store.Set(ctx, req.Namespace, req.Key, val)
	if err != nil {
		return nil, toGRPCError(err)
	}

	entry, err := valueToProto(req.Namespace, req.Key, result)
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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Key:       req.Key,
		Operation: OperationDelete,
	}); err != nil {
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

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Operation: OperationList,
	}); err != nil {
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
		entry, err := valueToProto(req.Namespace, key, val)
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

// Watch streams configuration changes in real-time.
func (s *Service) Watch(req *configpb.WatchRequest, stream configpb.ConfigService_WatchServer) error {
	ctx := stream.Context()

	// Authorize each requested namespace individually
	if len(req.Namespaces) == 0 {
		if err := s.authorizer.Authorize(ctx, AuthRequest{
			Operation: OperationWatch,
		}); err != nil {
			return err
		}
	} else {
		for _, ns := range req.Namespaces {
			if err := s.authorizer.Authorize(ctx, AuthRequest{
				Namespace: ns,
				Operation: OperationWatch,
			}); err != nil {
				return err
			}
		}
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

			entry, err := valueToProto(event.Namespace, event.Key, event.Value)
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
func (s *Service) CheckAccess(ctx context.Context, req *configpb.CheckAccessRequest) (*configpb.CheckAccessResponse, error) {
	resp := &configpb.CheckAccessResponse{}

	// Check read access
	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Operation: OperationRead,
	}); err == nil {
		resp.CanRead = true
	}

	// Check write access
	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Operation: OperationWrite,
	}); err == nil {
		resp.CanWrite = true
	}

	return resp, nil
}

// valueToProto converts a config.Value to a proto Entry.
// Returns an error if the value cannot be marshaled.
func valueToProto(namespace, key string, val config.Value) (*configpb.Entry, error) {
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
		Type:      int32(val.Type()),
	}

	// Marshal value to bytes
	data, err := val.Marshal()
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
