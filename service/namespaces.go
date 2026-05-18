package service

import (
	"context"
	"encoding/base64"
	"sort"
	"strings"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultNamespacePageLimit is used when the request omits a limit.
// maxNamespacePageLimit caps the per-page response so a single call cannot
// stream an unbounded list when the underlying enumeration is unpaginated
// (StatsProvider fallback collects every namespace into memory).
const (
	defaultNamespacePageLimit = 50
	maxNamespacePageLimit     = 500
)

// ListNamespaces returns namespace names contained in the store.
//
// Resolution order:
//  1. If the store implements [config.NamespaceLister], delegate to it.
//  2. Else if the store implements [config.StatsProvider], collect the keys of
//     EntriesByNamespace, sort ascending, and paginate in-memory.
//  3. Else return UNIMPLEMENTED.
//
// The local NamespaceLister interface that previously lived in this package
// has moved to [config.NamespaceLister] (config v0.8.0+); callers that wrote
// stores against this package's interface continue to satisfy the relocated
// one without changes because the method signature is identical.
//
// Authorization uses the global "list" action on an empty Resource — the same
// shape used by ListCodecs and ListAliases. Per-namespace authorization is not
// applied to the returned names, so deployments that need to hide namespaces
// from unauthorized callers should gate this RPC at the guard level.
func (s *Service) ListNamespaces(ctx context.Context, req *configpb.ListNamespacesRequest) (*configpb.ListNamespacesResponse, error) {
	if err := s.authorize(ctx, "list", Resource{}); err != nil {
		return nil, err
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = defaultNamespacePageLimit
	}
	if limit > maxNamespacePageLimit {
		limit = maxNamespacePageLimit
	}

	if nl, ok := s.store.(config.NamespaceLister); ok {
		names, next, err := nl.ListNamespaces(ctx, req.Prefix, limit, req.Cursor)
		if err != nil {
			return nil, toGRPCError(err)
		}
		return &configpb.ListNamespacesResponse{
			Namespaces: names,
			NextCursor: next,
		}, nil
	}

	sp, ok := s.store.(config.StatsProvider)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "ListNamespaces is not supported by the configured store")
	}

	// Phase 7: warn (once per Service lifetime) that the underlying store
	// is on the deprecated fallback path. Operators see this in logs and
	// can plan a backend upgrade before the fallback is removed in a
	// future major version.
	s.nsFallbackLogger.logOnce(s.store)

	// Phase 6: route through the singleflight + TTL cache so paginating
	// clients do not pay the O(namespaces) Stats cost per page. The
	// returned *StoreStats is shared and must be treated read-only.
	stats, err := s.nsStatsCache.Get(ctx, sp)
	if err != nil {
		return nil, toGRPCError(err)
	}

	// Collect → filter by prefix → sort. Sorting must happen after filtering
	// so the cursor (last emitted name) advances monotonically against the
	// same sorted view on the next call.
	//
	// config v0.8.0 exposes EntriesByNamespace as an iter.Seq2; namespace
	// count is no longer cheaply available, so the slice grows organically.
	var all []string
	for ns := range stats.EntriesByNamespace() {
		if req.Prefix != "" && !strings.HasPrefix(ns, req.Prefix) {
			continue
		}
		all = append(all, ns)
	}
	sort.Strings(all)

	start := 0
	if req.Cursor != "" {
		after, err := decodeNamespaceCursor(req.Cursor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid cursor: %v", err)
		}
		// First index whose name is strictly greater than the cursor.
		start = sort.SearchStrings(all, after)
		for start < len(all) && all[start] <= after {
			start++
		}
	}

	end := min(start+limit, len(all))
	page := all[start:end]

	var nextCursor string
	if end < len(all) && len(page) > 0 {
		nextCursor = encodeNamespaceCursor(page[len(page)-1])
	}

	return &configpb.ListNamespacesResponse{
		Namespaces: page,
		NextCursor: nextCursor,
	}, nil
}

// Namespace cursors are intentionally opaque to clients but trivially encoded
// here: the last-emitted name, base64-url encoded. base64-url is used so the
// cursor is safe to drop into a URL query string without further escaping.
func encodeNamespaceCursor(name string) string {
	return base64.URLEncoding.EncodeToString([]byte(name))
}

func decodeNamespaceCursor(cursor string) (string, error) {
	b, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
