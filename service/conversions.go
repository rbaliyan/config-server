package service

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// valueToProto converts a config.Value to a proto Entry. Returns an error when
// the value cannot be marshaled.
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

	data, err := val.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("marshal value %s/%s: %w", namespace, key, err)
	}
	entry.Value = data

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
