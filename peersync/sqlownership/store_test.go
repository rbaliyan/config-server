package sqlownership

import (
	"context"
	"database/sql"
	"sort"
	"testing"

	"github.com/rbaliyan/config-server/peersync"
	_ "modernc.org/sqlite"
)

// compile-time interface check
var _ peersync.OwnershipStore = (*Store)(nil)

func TestCompileTimeCheck(t *testing.T) {
	// This test exists solely to confirm the compile-time assertion above.
}

func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCreateTable_Success(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	if err := s.CreateTable(context.Background()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
}

func TestCreateTable_Idempotent(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	if err := s.CreateTable(context.Background()); err != nil {
		t.Fatalf("first CreateTable: %v", err)
	}
	if err := s.CreateTable(context.Background()); err != nil {
		t.Fatalf("second CreateTable: %v", err)
	}
}

func TestSaveOwner_And_LoadOwned(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	ctx := context.Background()

	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := s.SaveOwner(ctx, "ns-a", "node1"); err != nil {
		t.Fatalf("SaveOwner ns-a: %v", err)
	}
	if err := s.SaveOwner(ctx, "ns-b", "node1"); err != nil {
		t.Fatalf("SaveOwner ns-b: %v", err)
	}

	owned, err := s.LoadOwned(ctx, "node1")
	if err != nil {
		t.Fatalf("LoadOwned: %v", err)
	}
	if len(owned) != 2 {
		t.Fatalf("expected 2 namespaces, got %d: %v", len(owned), owned)
	}
	sort.Strings(owned)
	if owned[0] != "ns-a" || owned[1] != "ns-b" {
		t.Errorf("unexpected namespaces: %v", owned)
	}
}

func TestLoadOwned_EmptyForUnknownNode(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	ctx := context.Background()

	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	owned, err := s.LoadOwned(ctx, "unknown-node")
	if err != nil {
		t.Fatalf("LoadOwned: %v", err)
	}
	if len(owned) != 0 {
		t.Errorf("expected empty slice, got %v", owned)
	}
}

func TestSaveOwner_Upsert(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	ctx := context.Background()

	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := s.SaveOwner(ctx, "ns-x", "node1"); err != nil {
		t.Fatalf("SaveOwner node1: %v", err)
	}
	// Overwrite with a different node.
	if err := s.SaveOwner(ctx, "ns-x", "node2"); err != nil {
		t.Fatalf("SaveOwner node2: %v", err)
	}

	// node1 should no longer own anything.
	owned1, err := s.LoadOwned(ctx, "node1")
	if err != nil {
		t.Fatalf("LoadOwned node1: %v", err)
	}
	if len(owned1) != 0 {
		t.Errorf("node1 should own nothing after upsert, got %v", owned1)
	}

	// node2 should own ns-x.
	owned2, err := s.LoadOwned(ctx, "node2")
	if err != nil {
		t.Fatalf("LoadOwned node2: %v", err)
	}
	if len(owned2) != 1 || owned2[0] != "ns-x" {
		t.Errorf("expected node2 to own [ns-x], got %v", owned2)
	}
}

func TestDeleteOwner_RemovesRecord(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	ctx := context.Background()

	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := s.SaveOwner(ctx, "ns-del", "node1"); err != nil {
		t.Fatalf("SaveOwner: %v", err)
	}
	if err := s.DeleteOwner(ctx, "ns-del"); err != nil {
		t.Fatalf("DeleteOwner: %v", err)
	}

	owned, err := s.LoadOwned(ctx, "node1")
	if err != nil {
		t.Fatalf("LoadOwned: %v", err)
	}
	if len(owned) != 0 {
		t.Errorf("expected empty after delete, got %v", owned)
	}
}

func TestDeleteOwner_Nonexistent(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite")
	ctx := context.Background()

	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := s.DeleteOwner(ctx, "ns-does-not-exist"); err != nil {
		t.Fatalf("DeleteOwner nonexistent: %v", err)
	}
}

func TestWithTable_CustomTableName(t *testing.T) {
	db := openMemDB(t)
	s := New(db, "sqlite", WithTable("custom_ownership"))
	ctx := context.Background()

	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable with custom table: %v", err)
	}

	if err := s.SaveOwner(ctx, "ns-custom", "node-custom"); err != nil {
		t.Fatalf("SaveOwner: %v", err)
	}

	owned, err := s.LoadOwned(ctx, "node-custom")
	if err != nil {
		t.Fatalf("LoadOwned: %v", err)
	}
	if len(owned) != 1 || owned[0] != "ns-custom" {
		t.Errorf("unexpected owned: %v", owned)
	}

	// Verify the custom table name is actually used.
	if s.table != "custom_ownership" {
		t.Errorf("expected table name %q, got %q", "custom_ownership", s.table)
	}
}
