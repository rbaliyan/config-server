package service

import (
	"context"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// aliasStore returns the backing store as a config.AliasStore, or an
// Unimplemented error when the store does not support aliases.
func (s *Service) aliasStore() (config.AliasStore, error) {
	as, ok := s.store.(config.AliasStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "store does not support aliases")
	}
	return as, nil
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

	as, err := s.aliasStore()
	if err != nil {
		return nil, err
	}

	val, err := as.SetAlias(ctx, req.Alias, req.Target)
	if err != nil {
		return nil, toGRPCError(err)
	}

	s.record(ctx, AuditEntry{
		Timestamp: time.Now(),
		Identity:  auditIdentity(ctx),
		Operation: "alias_set",
		Key:       req.Alias,
		Metadata:  map[string]string{"target": req.Target},
	})

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

	as, err := s.aliasStore()
	if err != nil {
		return nil, err
	}

	if err := as.DeleteAlias(ctx, req.Alias); err != nil {
		return nil, toGRPCError(err)
	}

	s.record(ctx, AuditEntry{
		Timestamp: time.Now(),
		Identity:  auditIdentity(ctx),
		Operation: "alias_delete",
		Key:       req.Alias,
	})

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

	as, err := s.aliasStore()
	if err != nil {
		return nil, err
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
func (s *Service) ListAliases(ctx context.Context, _ *configpb.ListAliasesRequest) (*configpb.ListAliasesResponse, error) {
	if err := s.authorize(ctx, "list", Resource{}); err != nil {
		return nil, err
	}

	as, err := s.aliasStore()
	if err != nil {
		return nil, err
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
