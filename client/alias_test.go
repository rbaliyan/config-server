package client

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
)

func TestRemoteStore_Alias(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	// SetAlias round-trip.
	val, err := store.SetAlias(ctx, "db.host", "database/host")
	if err != nil {
		t.Fatalf("SetAlias: %v", err)
	}
	if val == nil {
		t.Fatal("SetAlias returned nil value")
	}
	if got, _ := val.String(); got != "database/host" {
		t.Errorf("SetAlias value = %q, want database/host", got)
	}

	// GetAlias decodes via aliasProtoToValue.
	got, err := store.GetAlias(ctx, "db.host")
	if err != nil {
		t.Fatalf("GetAlias: %v", err)
	}
	if s, _ := got.String(); s != "database/host" {
		t.Errorf("GetAlias value = %q, want database/host", s)
	}

	// ListAliases returns the alias map.
	aliases, err := store.ListAliases(ctx)
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	if len(aliases) != 1 {
		t.Fatalf("ListAliases len = %d, want 1", len(aliases))
	}
	v, ok := aliases["db.host"]
	if !ok {
		t.Fatal("ListAliases missing db.host")
	}
	if s, _ := v.String(); s != "database/host" {
		t.Errorf("ListAliases[db.host] = %q, want database/host", s)
	}

	// DeleteAlias then GetAlias -> ErrNotFound.
	if err := store.DeleteAlias(ctx, "db.host"); err != nil {
		t.Fatalf("DeleteAlias: %v", err)
	}
	if _, err := store.GetAlias(ctx, "db.host"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("GetAlias after delete: got %v, want ErrNotFound", err)
	}
}

func TestRemoteStore_Alias_Errors(t *testing.T) {
	store := setupIntegrationTest(t)
	ctx := context.Background()

	// Get a non-existent alias maps to config.ErrNotFound.
	if _, err := store.GetAlias(ctx, "missing"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("GetAlias missing: got %v, want ErrNotFound", err)
	}

	// Delete a non-existent alias maps to config.ErrNotFound.
	if err := store.DeleteAlias(ctx, "missing"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("DeleteAlias missing: got %v, want ErrNotFound", err)
	}
}

func TestAliasProtoToValue(t *testing.T) {
	t.Parallel()

	// nil proto -> nil value.
	if v := aliasProtoToValue(nil); v != nil {
		t.Errorf("aliasProtoToValue(nil) = %v, want nil", v)
	}
}
