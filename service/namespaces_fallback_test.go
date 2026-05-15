package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

// countingStatsProvider implements config.StatsProvider and records every
// Stats invocation so cache tests can assert how many upstream calls were
// issued. Optionally blocks each Stats call on a release channel so tests
// can hold a flight in-progress while issuing concurrent callers.
type countingStatsProvider struct {
	calls   atomic.Int32
	stats   config.StoreStats
	err     error
	release chan struct{} // if non-nil, Stats blocks until a value is received
}

func (p *countingStatsProvider) Stats(_ context.Context) (config.StoreStats, error) {
	p.calls.Add(1)
	if p.release != nil {
		<-p.release
	}
	return p.stats, p.err
}

// statsWithTotal builds a minimal StoreStats snapshot with the given total
// entry count and no namespace/type breakdown. Tests that only need
// "something non-nil" use this.
func statsWithTotal(total int64) config.StoreStats {
	return config.NewStoreStats(total, nil, nil)
}

// statsWithNamespaces builds a StoreStats snapshot whose EntriesByNamespace
// iterator yields one entry per namespace name (count=1). Used by the
// integration tests to drive ListNamespaces fallback pagination.
func statsWithNamespaces(names ...string) config.StoreStats {
	byNs := make(map[string]int64, len(names))
	for _, n := range names {
		byNs[n] = 1
	}
	return config.NewStoreStats(int64(len(names)), nil, byNs)
}

func newStatsProvider(stats config.StoreStats) *countingStatsProvider {
	return &countingStatsProvider{stats: stats}
}

func TestNamespaceStatsCache_CachesWithinTTL(t *testing.T) {
	c := newNamespaceStatsCache(50 * time.Millisecond)
	p := newStatsProvider(statsWithTotal(1))

	for range 5 {
		if _, err := c.Get(context.Background(), p); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if got := p.calls.Load(); got != 1 {
		t.Errorf("Stats called %d times within TTL window, want 1", got)
	}
}

func TestNamespaceStatsCache_RefreshesAfterTTL(t *testing.T) {
	c := newNamespaceStatsCache(10 * time.Millisecond)
	p := newStatsProvider(statsWithTotal(1))

	if _, err := c.Get(context.Background(), p); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := c.Get(context.Background(), p); err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if got := p.calls.Load(); got != 2 {
		t.Errorf("Stats called %d times across TTL boundary, want 2", got)
	}
}

func TestNamespaceStatsCache_SingleflightCollapsesConcurrent(t *testing.T) {
	c := newNamespaceStatsCache(time.Minute)
	p := &countingStatsProvider{
		stats:   statsWithTotal(1),
		release: make(chan struct{}),
	}

	const concurrent = 16
	var wg sync.WaitGroup
	for range concurrent {
		wg.Go(func() {
			if _, err := c.Get(context.Background(), p); err != nil {
				t.Errorf("Get: %v", err)
			}
		})
	}

	// All goroutines should now be parked waiting for the in-flight Stats
	// call to return. Release exactly one Stats invocation; if more than
	// one goroutine actually called sp.Stats, the extra calls would still
	// be blocked on the channel and the test would hang.
	time.Sleep(20 * time.Millisecond)
	close(p.release)
	wg.Wait()

	if got := p.calls.Load(); got != 1 {
		t.Errorf("Stats called %d times for %d concurrent Gets, want 1", got, concurrent)
	}
}

func TestNamespaceStatsCache_CachesErrors(t *testing.T) {
	// Caching errors prevents thundering-herd retries from amplifying a
	// downstream outage. The cached error must expire on the same TTL as
	// successful values so recovery is bounded.
	c := newNamespaceStatsCache(50 * time.Millisecond)
	sentinel := errors.New("statsboom")
	p := &countingStatsProvider{err: sentinel}

	for range 5 {
		_, err := c.Get(context.Background(), p)
		if !errors.Is(err, sentinel) {
			t.Fatalf("Get returned %v, want %v", err, sentinel)
		}
	}
	if got := p.calls.Load(); got != 1 {
		t.Errorf("Stats called %d times for cached error, want 1", got)
	}
}

func TestNamespaceStatsCache_TTLZeroDisables(t *testing.T) {
	// With TTL=0 every Get re-fetches; singleflight still collapses
	// concurrent in-flight calls, but the cache never returns a stale
	// hit across sequential calls.
	c := newNamespaceStatsCache(0)
	p := newStatsProvider(statsWithTotal(1))

	for range 3 {
		if _, err := c.Get(context.Background(), p); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if got := p.calls.Load(); got != 3 {
		t.Errorf("Stats called %d times with TTL=0, want 3", got)
	}
}

func TestNamespaceStatsCache_NegativeTTLClampedToZero(t *testing.T) {
	c := newNamespaceStatsCache(-time.Second)
	if c.ttl != 0 {
		t.Errorf("ttl = %v, want 0 (negative input must clamp to disabled)", c.ttl)
	}
}

func TestWithNamespaceStatsCacheTTL(t *testing.T) {
	// Default
	svc, _ := setupTestService(t)
	if got := svc.opts.namespaceStatsCacheTTL; got != defaultNamespaceStatsCacheTTL {
		t.Errorf("default ttl = %v, want %v", got, defaultNamespaceStatsCacheTTL)
	}

	// Custom positive
	svc2, err := NewService(svc.store,
		WithSecurityGuard(AllowAll()),
		WithNamespaceStatsCacheTTL(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if got := svc2.opts.namespaceStatsCacheTTL; got != 5*time.Second {
		t.Errorf("custom ttl = %v, want 5s", got)
	}

	// Negative is rejected silently — option leaves the existing default
	// in place rather than panicking, matching the pattern of the other
	// validation-light service options.
	svc3, err := NewService(svc.store,
		WithSecurityGuard(AllowAll()),
		WithNamespaceStatsCacheTTL(-1*time.Hour),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if got := svc3.opts.namespaceStatsCacheTTL; got != defaultNamespaceStatsCacheTTL {
		t.Errorf("negative ttl was applied (%v); want default %v", got, defaultNamespaceStatsCacheTTL)
	}
}

// fallbackOnlyStore implements config.Store + config.StatsProvider but
// intentionally NOT config.NamespaceLister, so Service.ListNamespaces is
// forced down the cached fallback path. Stats is counted via the embedded
// countingStatsProvider so the integration test can assert pagination
// shares one upstream call.
type fallbackOnlyStore struct {
	noStatsStore // satisfies config.Store with stub methods
	*countingStatsProvider
}

func TestService_ListNamespaces_FallbackUsesCacheAcrossPages(t *testing.T) {
	// Build a store that forces the fallback (StatsProvider but no
	// NamespaceLister) and paginate end-to-end through three pages. With
	// the cache wired correctly, all three pages share a single Stats
	// call.
	stats := statsWithNamespaces("alpha", "beta", "gamma", "delta", "epsilon")
	store := &fallbackOnlyStore{countingStatsProvider: newStatsProvider(stats)}

	svc, err := NewService(store,
		WithSecurityGuard(AllowAll()),
		WithNamespaceStatsCacheTTL(time.Minute),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	ctx := context.Background()
	page1, err := svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{Limit: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	page2, err := svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{Limit: 2, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	page3, err := svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{Limit: 2, Cursor: page2.NextCursor})
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}

	if got := store.calls.Load(); got != 1 {
		t.Errorf("Stats called %d times across 3 pages, want 1 (cache must collapse fallback fetches)", got)
	}
	if total := len(page1.Namespaces) + len(page2.Namespaces) + len(page3.Namespaces); total != 5 {
		t.Errorf("paged %d names, want 5 (alpha/beta/delta/epsilon/gamma)", total)
	}
	if page3.NextCursor != "" {
		t.Errorf("final page nextCursor = %q, want empty", page3.NextCursor)
	}
}

func TestService_ListNamespaces_FallbackTTLZeroDoesNotCache(t *testing.T) {
	// With TTL=0 each page should hit Stats again — the optimisation is
	// opt-out, so operators who explicitly disable it must observe the
	// uncached behaviour.
	stats := statsWithNamespaces("a", "b", "c")
	store := &fallbackOnlyStore{countingStatsProvider: newStatsProvider(stats)}

	svc, err := NewService(store,
		WithSecurityGuard(AllowAll()),
		WithNamespaceStatsCacheTTL(0),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	ctx := context.Background()
	p1, _ := svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{Limit: 1})
	p2, _ := svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{Limit: 1, Cursor: p1.NextCursor})
	_, _ = svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{Limit: 1, Cursor: p2.NextCursor})

	if got := store.calls.Load(); got != 3 {
		t.Errorf("Stats called %d times with TTL=0 across 3 pages, want 3", got)
	}
}
