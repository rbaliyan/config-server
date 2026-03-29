package service

import (
	"crypto/sha256"
	"fmt"
	"sort"

	"context"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Snapshot returns a point-in-time export of all entries in a namespace with ETag caching.
func (s *Service) Snapshot(ctx context.Context, req *configpb.SnapshotRequest) (*configpb.SnapshotResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}

	if err := s.authorizer.Authorize(ctx, AuthRequest{
		Namespace: req.Namespace,
		Operation: OperationRead,
	}); err != nil {
		return nil, err
	}

	entries, err := s.collectAllEntries(ctx, req.Namespace)
	if err != nil {
		// collectAllEntries may return gRPC status errors (e.g., ResourceExhausted)
		// directly; pass them through without double-wrapping.
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, toGRPCError(err)
	}

	etag := computeETag(entries)

	if req.IfNoneMatch != "" && req.IfNoneMatch == etag {
		return &configpb.SnapshotResponse{
			NotModified: true,
			Etag:        etag,
		}, nil
	}

	return &configpb.SnapshotResponse{
		Entries: entries,
		Etag:    etag,
	}, nil
}

// collectAllEntries paginates through store.Find to collect all entries in a namespace.
func (s *Service) collectAllEntries(ctx context.Context, namespace string) ([]*configpb.Entry, error) {
	var all []*configpb.Entry
	cursor := ""
	limit := 500

	for {
		fb := config.NewFilter().WithPrefix("").WithLimit(limit)
		if cursor != "" {
			fb = fb.WithCursor(cursor)
		}

		page, err := s.store.Find(ctx, namespace, fb.Build())
		if err != nil {
			return nil, err
		}

		results := page.Results()
		for key, val := range results {
			entry, err := valueToProto(namespace, key, val)
			if err != nil {
				return nil, err
			}
			all = append(all, entry)
		}

		// Stop if this is the last page: fewer results than requested or no next cursor.
		if len(results) < limit || page.NextCursor() == "" {
			break
		}

		// Guard against unbounded growth that could exhaust server memory.
		if len(all) > s.opts.maxSnapshotEntries {
			return nil, status.Errorf(codes.ResourceExhausted,
				"snapshot exceeds maximum entry count (%d)", s.opts.maxSnapshotEntries)
		}

		cursor = page.NextCursor()
	}

	// Sort by key for deterministic ETag
	sort.Slice(all, func(i, j int) bool {
		return all[i].Key < all[j].Key
	})

	return all, nil
}

// computeETag generates a SHA-256 hash from sorted (key, version) tuples.
func computeETag(entries []*configpb.Entry) string {
	h := sha256.New()
	for _, e := range entries {
		_, _ = fmt.Fprintf(h, "%s:%d\n", e.Key, e.Version)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
