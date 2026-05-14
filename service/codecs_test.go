package service

import (
	"context"
	"sort"
	"testing"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/codec"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeCodec is a no-op codec used to assert that registry entries surface
// via ListCodecs without depending on which real codecs happen to be linked.
type fakeCodec struct{ name string }

func (f fakeCodec) Name() string                                    { return f.name }
func (f fakeCodec) Encode(_ context.Context, _ any) ([]byte, error) { return nil, nil }
func (f fakeCodec) Decode(_ context.Context, _ []byte, _ any) error { return nil }

func TestService_ListCodecs(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupTestService(t)

	// The codec registry is process-global, so register both a plain and an
	// "encrypted:*" name to assert the response surfaces what's actually
	// registered (rather than asserting a fixed size). Re-registering a name
	// is allowed by codec.Register and replaces any prior codec.
	if err := codec.Register(fakeCodec{name: "test:plain"}); err != nil {
		t.Fatalf("register plain codec: %v", err)
	}
	if err := codec.Register(fakeCodec{name: "test:encrypted:json"}); err != nil {
		t.Fatalf("register encrypted codec: %v", err)
	}

	resp, err := svc.ListCodecs(ctx, &configpb.ListCodecsRequest{})
	if err != nil {
		t.Fatalf("ListCodecs: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Both registered names must appear.
	have := map[string]bool{}
	for _, n := range resp.Codecs {
		have[n] = true
	}
	for _, want := range []string{"test:plain", "test:encrypted:json"} {
		if !have[want] {
			t.Errorf("codec %q missing from ListCodecs result %v", want, resp.Codecs)
		}
	}

	// Result is sorted — important for stable dropdown ordering in the UI.
	if !sort.StringsAreSorted(resp.Codecs) {
		t.Errorf("ListCodecs result is not sorted: %v", resp.Codecs)
	}
}

func TestService_ListCodecs_DenyAll(t *testing.T) {
	// ListCodecs must be gated by the SecurityGuard — the codec set can be
	// sensitive (it tells an attacker which encrypted wrappers are in use).
	// DenyAll's Authenticate fails before Authorize runs, so the expected
	// code is Unauthenticated; either rejection is acceptable, what matters
	// is that the guard chain is invoked.
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	t.Cleanup(func() { store.Close(ctx) })

	svc, err := NewService(store, WithSecurityGuard(DenyAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, err = svc.ListCodecs(ctx, &configpb.ListCodecsRequest{})
	if err == nil {
		t.Fatal("expected error from DenyAll guard, got nil")
	}
	switch c := status.Code(err); c {
	case codes.Unauthenticated, codes.PermissionDenied:
		// pass
	default:
		t.Errorf("error code = %v, want Unauthenticated or PermissionDenied", c)
	}
}
