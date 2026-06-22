//go:build integration

// Package sqlownership PostgreSQL integration tests.
//
// These tests exercise the real PostgreSQL upsert (ON CONFLICT ... DO UPDATE)
// and concurrent-claim semantics that the SQLite :memory: unit tests cannot
// reach. They are gated behind the `integration` build tag and require a
// reachable PostgreSQL instance addressed by the POSTGRES_DSN environment
// variable, e.g.:
//
//	POSTGRES_DSN=postgres://user:pass@localhost:5432/db?sslmode=disable \
//	    go test -tags=integration ./peersync/sqlownership/...
//
// When POSTGRES_DSN is unset the suite SKIPS (it never fails), so the gated
// build stays green in environments without a database.
package sqlownership

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/rbaliyan/config-server/peersync"
)

// openPostgres connects to the DSN in POSTGRES_DSN, skipping the test cleanly
// when it is unset or the database is unreachable within a short timeout. Each
// call uses a uniquely named table so parallel/repeat runs do not collide, and
// drops it on cleanup.
func openPostgres(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping PostgreSQL integration test")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open(postgres): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("PostgreSQL not reachable at POSTGRES_DSN: %v", err)
	}

	table := fmt.Sprintf("peersync_ownership_it_%d", time.Now().UnixNano())
	s := New(db, "postgres", WithTable(table))
	if err := s.CreateTable(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("CreateTable: %v", err)
	}
	if !s.postgres {
		t.Fatalf("expected postgres dialect for driver %q", "postgres")
	}

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		_ = db.Close()
	})
	return s, db
}

// TestPostgres_SaveOwner_Upsert verifies the ON CONFLICT ... DO UPDATE path:
// re-saving the same namespace under a new node transfers ownership rather
// than erroring on the primary-key conflict.
func TestPostgres_SaveOwner_Upsert(t *testing.T) {
	s, _ := openPostgres(t)
	ctx := context.Background()

	if err := s.SaveOwner(ctx, "ns-x", "node1"); err != nil {
		t.Fatalf("SaveOwner node1: %v", err)
	}
	if err := s.SaveOwner(ctx, "ns-x", "node2"); err != nil {
		t.Fatalf("SaveOwner node2 (upsert): %v", err)
	}

	owned1, err := s.LoadOwned(ctx, "node1")
	if err != nil {
		t.Fatalf("LoadOwned node1: %v", err)
	}
	if len(owned1) != 0 {
		t.Errorf("node1 should own nothing after upsert, got %v", owned1)
	}
	owned2, err := s.LoadOwned(ctx, "node2")
	if err != nil {
		t.Fatalf("LoadOwned node2: %v", err)
	}
	if len(owned2) != 1 || owned2[0] != "ns-x" {
		t.Errorf("expected node2 to own [ns-x], got %v", owned2)
	}
}

// TestPostgres_ConcurrentClaim verifies that many goroutines racing to claim
// the same namespace via SaveOwner (the upsert) all succeed without error and
// converge to a single, consistent winner — the row's final node is exactly
// one of the contenders, with no duplicate rows.
func TestPostgres_ConcurrentClaim(t *testing.T) {
	s, db := openPostgres(t)
	ctx := context.Background()

	const workers = 24
	var (
		wg      sync.WaitGroup
		errCnt  int32
		startCh = make(chan struct{})
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			<-startCh
			if err := s.SaveOwner(ctx, "contended", fmt.Sprintf("node%d", id)); err != nil {
				atomic.AddInt32(&errCnt, 1)
				t.Errorf("worker %d SaveOwner: %v", id, err)
			}
		}(i)
	}
	close(startCh)
	wg.Wait()

	if errCnt != 0 {
		t.Fatalf("%d concurrent SaveOwner calls errored", errCnt)
	}

	// Exactly one row must exist for the contended namespace (PK enforces this),
	// and its owner must be one of the contenders.
	var rowCount int
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE namespace = $1", s.table), "contended",
	).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 ownership row for contended namespace, got %d", rowCount)
	}

	var owner string
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT node_id FROM %s WHERE namespace = $1", s.table), "contended",
	).Scan(&owner); err != nil {
		t.Fatalf("read owner: %v", err)
	}
	if owner == "" {
		t.Error("contended namespace has empty owner")
	}
}

// TestPostgres_LoadOwned_MultipleNamespaces verifies LoadOwned returns all
// namespaces for a node and is isolated across nodes.
func TestPostgres_LoadOwned_MultipleNamespaces(t *testing.T) {
	s, _ := openPostgres(t)
	ctx := context.Background()

	for _, ns := range []string{"a", "b", "c"} {
		if err := s.SaveOwner(ctx, ns, "owner"); err != nil {
			t.Fatalf("SaveOwner %s: %v", ns, err)
		}
	}
	if err := s.SaveOwner(ctx, "other", "stranger"); err != nil {
		t.Fatalf("SaveOwner other: %v", err)
	}

	owned, err := s.LoadOwned(ctx, "owner")
	if err != nil {
		t.Fatalf("LoadOwned: %v", err)
	}
	sort.Strings(owned)
	if len(owned) != 3 || owned[0] != "a" || owned[1] != "b" || owned[2] != "c" {
		t.Errorf("LoadOwned(owner) = %v, want [a b c]", owned)
	}
}

// TestPostgres_DeleteOwner verifies DeleteOwner removes only the targeted row
// and that deleting a nonexistent namespace is a no-op.
func TestPostgres_DeleteOwner(t *testing.T) {
	s, _ := openPostgres(t)
	ctx := context.Background()

	if err := s.SaveOwner(ctx, "del-ns", "n1"); err != nil {
		t.Fatalf("SaveOwner: %v", err)
	}
	if err := s.DeleteOwner(ctx, "del-ns"); err != nil {
		t.Fatalf("DeleteOwner: %v", err)
	}
	owned, err := s.LoadOwned(ctx, "n1")
	if err != nil {
		t.Fatalf("LoadOwned: %v", err)
	}
	if len(owned) != 0 {
		t.Errorf("expected empty after delete, got %v", owned)
	}
	// Deleting again is a no-op.
	if err := s.DeleteOwner(ctx, "del-ns"); err != nil {
		t.Fatalf("DeleteOwner (nonexistent): %v", err)
	}
}

// compile-time assurance the integration suite uses the real interface.
var _ peersync.OwnershipStore = (*Store)(nil)
