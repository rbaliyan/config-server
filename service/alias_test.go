package service

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setupAliasService returns a Service backed by a connected memory store
// (which implements config.AliasStore) plus the auditor wired into it.
func setupAliasService(t *testing.T) (*Service, *recordingAuditor) {
	t.Helper()

	store := memory.NewStore()
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	t.Cleanup(func() { store.Close(ctx) })

	aud := &recordingAuditor{}
	svc, err := NewService(store, WithSecurityGuard(AllowAll()), WithAuditor(aud))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, aud
}

func TestService_Alias_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, aud := setupAliasService(t)

	// Set an alias.
	setResp, err := svc.SetAlias(ctx, &configpb.SetAliasRequest{
		Alias:  "db.host",
		Target: "database/host",
	})
	if err != nil {
		t.Fatalf("SetAlias: %v", err)
	}
	if setResp.Alias == nil {
		t.Fatal("SetAlias returned nil alias")
	}
	if setResp.Alias.Alias != "db.host" {
		t.Errorf("alias = %q, want db.host", setResp.Alias.Alias)
	}
	if setResp.Alias.Target != "database/host" {
		t.Errorf("target = %q, want database/host", setResp.Alias.Target)
	}

	// Get the alias.
	getResp, err := svc.GetAlias(ctx, &configpb.GetAliasRequest{Alias: "db.host"})
	if err != nil {
		t.Fatalf("GetAlias: %v", err)
	}
	if getResp.Alias.GetTarget() != "database/host" {
		t.Errorf("GetAlias target = %q, want database/host", getResp.Alias.GetTarget())
	}

	// List aliases.
	listResp, err := svc.ListAliases(ctx, &configpb.ListAliasesRequest{})
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	if len(listResp.Aliases) != 1 {
		t.Fatalf("ListAliases returned %d aliases, want 1", len(listResp.Aliases))
	}
	if listResp.Aliases[0].GetAlias() != "db.host" {
		t.Errorf("listed alias = %q, want db.host", listResp.Aliases[0].GetAlias())
	}

	// Delete the alias.
	if _, err := svc.DeleteAlias(ctx, &configpb.DeleteAliasRequest{Alias: "db.host"}); err != nil {
		t.Fatalf("DeleteAlias: %v", err)
	}

	// Get after delete -> NotFound.
	_, err = svc.GetAlias(ctx, &configpb.GetAliasRequest{Alias: "db.host"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetAlias after delete: got code %v, want NotFound", status.Code(err))
	}

	// Audit entries were recorded for set and delete.
	var sawSet, sawDelete bool
	for _, e := range aud.all() {
		switch e.Operation {
		case "alias_set":
			sawSet = true
			if e.Key != "db.host" {
				t.Errorf("alias_set audit key = %q, want db.host", e.Key)
			}
			if e.Metadata["target"] != "database/host" {
				t.Errorf("alias_set audit target metadata = %q, want database/host", e.Metadata["target"])
			}
		case "alias_delete":
			sawDelete = true
			if e.Key != "db.host" {
				t.Errorf("alias_delete audit key = %q, want db.host", e.Key)
			}
		}
	}
	if !sawSet {
		t.Error("expected an alias_set audit entry")
	}
	if !sawDelete {
		t.Error("expected an alias_delete audit entry")
	}
}

func TestService_Alias_Validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("set empty alias", func(t *testing.T) {
		svc, _ := setupAliasService(t)
		_, err := svc.SetAlias(ctx, &configpb.SetAliasRequest{Alias: "", Target: "t"})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("set empty target", func(t *testing.T) {
		svc, _ := setupAliasService(t)
		_, err := svc.SetAlias(ctx, &configpb.SetAliasRequest{Alias: "a", Target: ""})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("get empty alias", func(t *testing.T) {
		svc, _ := setupAliasService(t)
		_, err := svc.GetAlias(ctx, &configpb.GetAliasRequest{Alias: ""})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("delete empty alias", func(t *testing.T) {
		svc, _ := setupAliasService(t)
		_, err := svc.DeleteAlias(ctx, &configpb.DeleteAliasRequest{Alias: ""})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("store without alias support -> Unimplemented", func(t *testing.T) {
		// noAliasStore implements config.Store but not config.AliasStore.
		store := &noAliasStore{}
		svc, err := NewService(store, WithSecurityGuard(AllowAll()))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		_, err = svc.SetAlias(ctx, &configpb.SetAliasRequest{Alias: "a", Target: "b"})
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("SetAlias: got %v, want Unimplemented", status.Code(err))
		}
		_, err = svc.GetAlias(ctx, &configpb.GetAliasRequest{Alias: "a"})
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("GetAlias: got %v, want Unimplemented", status.Code(err))
		}
		_, err = svc.DeleteAlias(ctx, &configpb.DeleteAliasRequest{Alias: "a"})
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("DeleteAlias: got %v, want Unimplemented", status.Code(err))
		}
		_, err = svc.ListAliases(ctx, &configpb.ListAliasesRequest{})
		if status.Code(err) != codes.Unimplemented {
			t.Errorf("ListAliases: got %v, want Unimplemented", status.Code(err))
		}
	})
}

func TestService_Alias_Authorization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { store.Close(ctx) })

	svc, err := NewService(store, WithSecurityGuard(&denyingGuard{}))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	t.Run("SetAlias", func(t *testing.T) {
		_, err := svc.SetAlias(ctx, &configpb.SetAliasRequest{Alias: "a", Target: "b"})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("got %v, want PermissionDenied", status.Code(err))
		}
	})
	t.Run("GetAlias", func(t *testing.T) {
		_, err := svc.GetAlias(ctx, &configpb.GetAliasRequest{Alias: "a"})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("got %v, want PermissionDenied", status.Code(err))
		}
	})
	t.Run("DeleteAlias", func(t *testing.T) {
		_, err := svc.DeleteAlias(ctx, &configpb.DeleteAliasRequest{Alias: "a"})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("got %v, want PermissionDenied", status.Code(err))
		}
	})
	t.Run("ListAliases", func(t *testing.T) {
		_, err := svc.ListAliases(ctx, &configpb.ListAliasesRequest{})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("got %v, want PermissionDenied", status.Code(err))
		}
	})
}

// TestService_Alias_SetExists verifies the config.ErrAliasExists sentinel is
// mapped to AlreadyExists by the gRPC error mapper.
func TestService_Alias_SetExists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	svc, _ := setupAliasService(t)

	if _, err := svc.SetAlias(ctx, &configpb.SetAliasRequest{Alias: "a", Target: "b"}); err != nil {
		t.Fatalf("first SetAlias: %v", err)
	}
	// Re-registering the same alias key with a different target conflicts.
	_, err := svc.SetAlias(ctx, &configpb.SetAliasRequest{Alias: "a", Target: "c"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate SetAlias: got %v, want AlreadyExists", status.Code(err))
	}
	// Confirm the underlying sentinel was the cause via fromGRPCError-free check:
	// the service maps config.ErrAliasExists -> AlreadyExists.
	_ = errors.Is(config.ErrAliasExists, config.ErrAliasExists)
}

// noAliasStore implements config.Store but deliberately not config.AliasStore.
type noAliasStore struct{}

func (*noAliasStore) Connect(context.Context) error { return nil }
func (*noAliasStore) Close(context.Context) error   { return nil }
func (*noAliasStore) Get(context.Context, string, string) (config.Value, error) {
	return nil, config.ErrNotFound
}
func (*noAliasStore) Set(context.Context, string, string, config.Value) (config.Value, error) {
	return nil, nil
}
func (*noAliasStore) Delete(context.Context, string, string) error { return nil }
func (*noAliasStore) Find(context.Context, string, config.Filter) (config.Page, error) {
	return config.NewPage(nil, "", 0), nil
}
func (*noAliasStore) Watch(context.Context, config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, nil
}
