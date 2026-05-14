package service

import (
	"context"
	"sort"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/codec"
)

// ListCodecs returns the names of all codecs registered on the server.
//
// Clients use this to discover which codecs the server can encode/decode —
// in particular, whether config-crypto's "encrypted:<inner>" wrappers have
// been registered. Names are returned sorted alphabetically for stable UI.
func (s *Service) ListCodecs(ctx context.Context, _ *configpb.ListCodecsRequest) (*configpb.ListCodecsResponse, error) {
	if err := s.authorize(ctx, "list", Resource{}); err != nil {
		return nil, err
	}

	names := codec.Names()
	sort.Strings(names)

	return &configpb.ListCodecsResponse{
		Codecs: names,
	}, nil
}
