package service

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// seedNamespaces writes one entry into each named namespace so the underlying
// store's StatsProvider reports the namespace.
func seedNamespaces(t *testing.T, store config.Store, namespaces ...string) {
	t.Helper()
	ctx := context.Background()
	for _, ns := range namespaces {
		if _, err := store.Set(ctx, ns, "k", config.NewValue("v")); err != nil {
			t.Fatalf("seed %q: %v", ns, err)
		}
	}
}

func TestService_ListNamespaces_StatsFallback(t *testing.T) {
	svc, store := setupTestService(t)
	seedNamespaces(t, store, "prod", "staging", "dev", "alpha")

	resp, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{})
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if got, want := len(resp.Namespaces), 4; got != want {
		t.Fatalf("len(namespaces) = %d, want %d (%v)", got, want, resp.Namespaces)
	}
	if !sort.StringsAreSorted(resp.Namespaces) {
		t.Errorf("namespaces not sorted: %v", resp.Namespaces)
	}
	if resp.NextCursor != "" {
		t.Errorf("nextCursor = %q, want empty when all results fit on one page", resp.NextCursor)
	}
}

func TestService_ListNamespaces_PrefixFilter(t *testing.T) {
	svc, store := setupTestService(t)
	seedNamespaces(t, store, "team-a", "team-b", "team-c", "other")

	resp, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{Prefix: "team-"})
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	want := []string{"team-a", "team-b", "team-c"}
	if got := resp.Namespaces; !equalStrings(got, want) {
		t.Errorf("namespaces = %v, want %v", got, want)
	}
}

func TestService_ListNamespaces_Pagination(t *testing.T) {
	svc, store := setupTestService(t)
	seedNamespaces(t, store, "a", "b", "c", "d", "e")

	// Page 1
	page1, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{Limit: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if got, want := page1.Namespaces, []string{"a", "b"}; !equalStrings(got, want) {
		t.Fatalf("page 1 = %v, want %v", got, want)
	}
	if page1.NextCursor == "" {
		t.Fatal("page 1 nextCursor is empty but more results remain")
	}

	// Page 2
	page2, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{Limit: 2, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if got, want := page2.Namespaces, []string{"c", "d"}; !equalStrings(got, want) {
		t.Fatalf("page 2 = %v, want %v", got, want)
	}

	// Page 3 (final)
	page3, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{Limit: 2, Cursor: page2.NextCursor})
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}
	if got, want := page3.Namespaces, []string{"e"}; !equalStrings(got, want) {
		t.Fatalf("page 3 = %v, want %v", got, want)
	}
	if page3.NextCursor != "" {
		t.Errorf("final page nextCursor = %q, want empty", page3.NextCursor)
	}
}

func TestService_ListNamespaces_InvalidCursor(t *testing.T) {
	svc, _ := setupTestService(t)

	_, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{Cursor: "!not-base64!"})
	if err == nil {
		t.Fatal("expected error for invalid cursor")
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("error code = %v, want InvalidArgument", code)
	}
}

func TestService_ListNamespaces_NativeLister(t *testing.T) {
	// A store that implements NamespaceLister bypasses the StatsProvider
	// fallback and returns whatever the native implementation produces — the
	// service must not re-sort or re-paginate the result.
	store := &nativeListerStore{
		namespaces: []string{"zeta", "alpha"}, // intentionally unsorted
		next:       "tok-1",
	}
	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	resp, err := svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{Limit: 10, Prefix: "z"})
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if got, want := resp.Namespaces, []string{"zeta", "alpha"}; !equalStrings(got, want) {
		t.Errorf("namespaces = %v, want %v (service must pass through unchanged)", got, want)
	}
	if resp.NextCursor != "tok-1" {
		t.Errorf("nextCursor = %q, want passthrough %q", resp.NextCursor, "tok-1")
	}

	if store.gotPrefix != "z" || store.gotLimit != 10 {
		t.Errorf("native lister received prefix=%q limit=%d, want prefix=z limit=10",
			store.gotPrefix, store.gotLimit)
	}
}

func TestService_ListNamespaces_Unimplemented(t *testing.T) {
	// A store that implements neither NamespaceLister nor StatsProvider must
	// surface UNIMPLEMENTED rather than a generic Internal error.
	svc, err := NewService(noStatsStore{}, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	_, err = svc.ListNamespaces(context.Background(), &configpb.ListNamespacesRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if code := status.Code(err); code != codes.Unimplemented {
		t.Errorf("error code = %v, want Unimplemented", code)
	}
}

func TestService_ListNamespaces_DenyAll(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	t.Cleanup(func() { store.Close(ctx) })

	svc, err := NewService(store, WithSecurityGuard(DenyAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	_, err = svc.ListNamespaces(ctx, &configpb.ListNamespacesRequest{})
	if err == nil {
		t.Fatal("expected error from DenyAll guard")
	}
	switch c := status.Code(err); c {
	case codes.Unauthenticated, codes.PermissionDenied:
		// pass
	default:
		t.Errorf("error code = %v, want Unauthenticated or PermissionDenied", c)
	}
}

// nativeListerStore embeds an unused memory.Store only to satisfy the parts
// of config.Store the service touches; only ListNamespaces is exercised here.
type nativeListerStore struct {
	memory.Store
	namespaces []string
	next       string
	gotPrefix  string
	gotLimit   int
}

func (n *nativeListerStore) ListNamespaces(_ context.Context, prefix string, limit int, _ string) ([]string, string, error) {
	n.gotPrefix = prefix
	n.gotLimit = limit
	return n.namespaces, n.next, nil
}

// noStatsStore is a config.Store that intentionally implements neither
// NamespaceLister nor StatsProvider so the service falls all the way through
// to the Unimplemented return.
type noStatsStore struct{}

func (noStatsStore) Connect(context.Context) error                              { return nil }
func (noStatsStore) Close(context.Context) error                                { return nil }
func (noStatsStore) Get(context.Context, string, string) (config.Value, error)  { return nil, errors.New("nope") }
func (noStatsStore) Set(context.Context, string, string, config.Value) (config.Value, error) {
	return nil, errors.New("nope")
}
func (noStatsStore) Delete(context.Context, string, string) error { return errors.New("nope") }
func (noStatsStore) Find(context.Context, string, config.Filter) (config.Page, error) {
	return nil, errors.New("nope")
}
func (noStatsStore) Watch(context.Context, config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, errors.New("nope")
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
