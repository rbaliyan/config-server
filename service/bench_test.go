package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rbaliyan/config"
)

// BenchmarkClassifyError measures the error-classification switch invoked on
// every failed RPC by toGRPCError. The cost depends on how far down the
// errors.As / errors.Is chain a given error matches, so the variants are split
// into sub-benchmarks: a typed error matched early, sentinels matched late, a
// wrapped sentinel, and an unknown error that falls through the entire switch.
func BenchmarkClassifyError(b *testing.B) {
	cases := []struct {
		name string
		err  error
	}{
		{"NotFound", config.ErrNotFound},
		{"KeyExists", config.ErrKeyExists},
		{"InvalidKey", config.ErrInvalidKey},
		{"TypedKeyNotFound", &config.KeyNotFoundError{Namespace: "prod", Key: "db/host"}},
		{"Wrapped", fmt.Errorf("get prod/db: %w", config.ErrInvalidCursor)},
		{"Unknown", errors.New("some unclassified failure")},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _, _ = classifyError(tc.err)
			}
		})
	}
}

// BenchmarkValueToProto measures the conversion of a config.Value to a proto
// Entry: a Marshal call plus up to two timestamppb.New allocations and the
// Entry allocation itself. This runs once per entry on every Get, List, and
// Snapshot response, so it is on the hot path for read-heavy workloads.
func BenchmarkValueToProto(b *testing.B) {
	ctx := context.Background()

	largeString := make([]byte, 4096)
	for i := range largeString {
		largeString[i] = byte('a' + i%26)
	}

	cases := []struct {
		name string
		val  config.Value
	}{
		{"String", config.NewValue("localhost:5432")},
		{"Int", config.NewValue(42)},
		{"Bool", config.NewValue(true)},
		{"LargeString", config.NewValue(string(largeString))},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := valueToProto(ctx, "production", "app/db", tc.val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
