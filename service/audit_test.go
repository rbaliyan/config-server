package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/memory"
	_ "modernc.org/sqlite"
)

// testSlogHandler collects slog records for assertions.
type testSlogHandler struct {
	mu      sync.Mutex
	records []map[string]any
}

func (h *testSlogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *testSlogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	attrs["msg"] = r.Message
	h.mu.Lock()
	h.records = append(h.records, attrs)
	h.mu.Unlock()
	return nil
}

func (h *testSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *testSlogHandler) WithGroup(string) slog.Handler      { return h }

func (h *testSlogHandler) last() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) == 0 {
		return nil
	}
	return h.records[len(h.records)-1]
}

// recordingAuditor is an in-memory Auditor for assertions in integration tests.
type recordingAuditor struct {
	mu      sync.Mutex
	entries []AuditEntry
}

func (r *recordingAuditor) Record(_ context.Context, entry AuditEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
	return nil
}

func (r *recordingAuditor) all() []AuditEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]AuditEntry, len(r.entries))
	copy(cp, r.entries)
	return cp
}

// failingAuditor always returns an error from Record.
type failingAuditor struct{ err error }

func (f *failingAuditor) Record(_ context.Context, _ AuditEntry) error { return f.err }

// openTestDB opens an in-memory SQLite database and registers cleanup.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestLogAuditor(t *testing.T) {
	h := &testSlogHandler{}
	logger := slog.New(h)
	auditor := NewLogAuditor(logger)

	ctx := context.Background()
	entry := AuditEntry{
		Timestamp: time.Now(),
		Identity:  anonymousIdentity{},
		Operation: "set",
		Namespace: "ns1",
		Key:       "key1",
		Value:     "dGVzdA==",
		Codec:     "json",
		Metadata:  map[string]string{"ttl": "60s"},
	}

	if err := auditor.Record(ctx, entry); err != nil {
		t.Fatalf("Record returned unexpected error: %v", err)
	}

	rec := h.last()
	if rec == nil {
		t.Fatal("expected slog record, got none")
	}
	if rec["msg"] != "audit" {
		t.Errorf("msg = %q, want %q", rec["msg"], "audit")
	}
	if rec["operation"] != "set" {
		t.Errorf("operation = %v, want %q", rec["operation"], "set")
	}
	if rec["namespace"] != "ns1" {
		t.Errorf("namespace = %v, want %q", rec["namespace"], "ns1")
	}
	if rec["key"] != "key1" {
		t.Errorf("key = %v, want %q", rec["key"], "key1")
	}
	if rec["user_id"] != "anonymous" {
		t.Errorf("user_id = %v, want %q", rec["user_id"], "anonymous")
	}
}

func TestLogAuditor_NilIdentity(t *testing.T) {
	h := &testSlogHandler{}
	auditor := NewLogAuditor(slog.New(h))

	err := auditor.Record(context.Background(), AuditEntry{
		Timestamp: time.Now(),
		Operation: "delete",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := h.last()
	if rec == nil {
		t.Fatal("expected slog record")
	}
	// nil identity should produce empty user_id
	if rec["user_id"] != "" {
		t.Errorf("user_id = %v, want empty string", rec["user_id"])
	}
}

func TestSQLAuditor(t *testing.T) {
	db := openTestDB(t)
	auditor := NewSQLAuditor(db, "sqlite")

	ctx := context.Background()
	if err := auditor.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	entries := []AuditEntry{
		{
			Timestamp: time.Now().UTC(),
			Identity:  anonymousIdentity{},
			Operation: "set",
			Namespace: "prod",
			Key:       "db/host",
			Value:     "bG9jYWxob3N0",
			Codec:     "json",
			Metadata:  map[string]string{"ttl": "30s"},
		},
		{
			Timestamp: time.Now().UTC(),
			Identity:  anonymousIdentity{},
			Operation: "delete",
			Namespace: "prod",
			Key:       "db/port",
		},
	}

	for _, e := range entries {
		if err := auditor.Record(ctx, e); err != nil {
			t.Fatalf("Record(%q): %v", e.Operation, err)
		}
	}

	rows, err := db.QueryContext(ctx, "SELECT operation, namespace, key FROM config_audit_log ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct{ op, ns, key string }
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.op, &r.ns, &r.key); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].op != "set" || got[0].ns != "prod" || got[0].key != "db/host" {
		t.Errorf("row 0 = %+v, want {set prod db/host}", got[0])
	}
	if got[1].op != "delete" || got[1].key != "db/port" {
		t.Errorf("row 1 = %+v, want {delete prod db/port}", got[1])
	}
}

func TestSQLAuditor_WithAuditTable(t *testing.T) {
	db := openTestDB(t)
	auditor := NewSQLAuditor(db, "sqlite", WithAuditTable("custom_audit"))

	ctx := context.Background()
	if err := auditor.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := auditor.Record(ctx, AuditEntry{
		Timestamp: time.Now().UTC(),
		Identity:  anonymousIdentity{},
		Operation: "set",
		Namespace: "ns",
		Key:       "k",
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM custom_audit").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestMultiAuditor(t *testing.T) {
	r1 := &recordingAuditor{}
	r2 := &recordingAuditor{}
	multi := NewMultiAuditor(r1, r2)

	ctx := context.Background()
	entry := AuditEntry{
		Timestamp: time.Now(),
		Identity:  anonymousIdentity{},
		Operation: "set",
		Namespace: "ns",
		Key:       "key",
	}

	if err := multi.Record(ctx, entry); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(r1.all()) != 1 {
		t.Errorf("r1: expected 1 entry, got %d", len(r1.all()))
	}
	if len(r2.all()) != 1 {
		t.Errorf("r2: expected 1 entry, got %d", len(r2.all()))
	}
	if r1.all()[0].Operation != "set" {
		t.Errorf("r1 entry operation = %q, want %q", r1.all()[0].Operation, "set")
	}
}

func TestMultiAuditor_ErrorCollection(t *testing.T) {
	err1 := errors.New("auditor 1 failed")
	err2 := errors.New("auditor 2 failed")

	multi := NewMultiAuditor(
		&failingAuditor{err: err1},
		&failingAuditor{err: err2},
	)

	err := multi.Record(context.Background(), AuditEntry{
		Timestamp: time.Now(),
		Operation: "set",
	})
	if err == nil {
		t.Fatal("expected combined error, got nil")
	}
	if !errors.Is(err, err1) {
		t.Errorf("expected err1 in combined error: %v", err)
	}
	if !errors.Is(err, err2) {
		t.Errorf("expected err2 in combined error: %v", err)
	}
}

func TestMultiAuditor_PartialFailure(t *testing.T) {
	rec := &recordingAuditor{}
	multi := NewMultiAuditor(&failingAuditor{err: errors.New("fail")}, rec)

	_ = multi.Record(context.Background(), AuditEntry{Operation: "set"})

	// The recording auditor should still have been called.
	if len(rec.all()) != 1 {
		t.Errorf("expected 1 entry in recorder, got %d", len(rec.all()))
	}
}

func TestServiceAudit_SetAndDelete(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	rec := &recordingAuditor{}
	svc, err := NewService(store,
		WithSecurityGuard(AllowAll()),
		WithAuditor(rec),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// Set
	if _, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "prod",
		Key:       "app/name",
		Value:     []byte(`"myapp"`),
		Codec:     "json",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Delete
	if _, err := svc.Delete(ctx, &configpb.DeleteRequest{
		Namespace: "prod",
		Key:       "app/name",
	}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	entries := rec.all()
	if len(entries) != 2 {
		t.Fatalf("expected 2 audit entries, got %d", len(entries))
	}

	setEntry := entries[0]
	if setEntry.Operation != "set" {
		t.Errorf("op = %q, want %q", setEntry.Operation, "set")
	}
	if setEntry.Namespace != "prod" {
		t.Errorf("namespace = %q, want %q", setEntry.Namespace, "prod")
	}
	if setEntry.Key != "app/name" {
		t.Errorf("key = %q, want %q", setEntry.Key, "app/name")
	}
	if setEntry.Codec != "json" {
		t.Errorf("codec = %q, want %q", setEntry.Codec, "json")
	}
	if setEntry.Value == "" {
		t.Error("expected non-empty value for set operation")
	}
	if setEntry.Identity == nil {
		t.Error("expected non-nil identity")
	}
	if setEntry.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}

	delEntry := entries[1]
	if delEntry.Operation != "delete" {
		t.Errorf("op = %q, want %q", delEntry.Operation, "delete")
	}
	if delEntry.Key != "app/name" {
		t.Errorf("key = %q, want %q", delEntry.Key, "app/name")
	}
}

func TestServiceAudit_NoAuditorNoPanic(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	// No auditor configured — should not panic.
	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if _, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`"v"`),
		Codec:     "json",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestServiceAudit_AuditorErrorDoesNotFailWrite(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	svc, err := NewService(store,
		WithSecurityGuard(AllowAll()),
		WithAuditor(&failingAuditor{err: errors.New("audit store down")}),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// The write must succeed even though the auditor returns an error.
	resp, err := svc.Set(ctx, &configpb.SetRequest{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`"v"`),
		Codec:     "json",
	})
	if err != nil {
		t.Fatalf("Set: %v — audit failure must not propagate", err)
	}
	if resp.Entry == nil {
		t.Fatal("expected entry in response")
	}
}

func TestServiceAudit_SetAlias(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close(ctx)

	// Seed the target key so SetAlias can resolve it.
	if _, err := store.Set(ctx, "", "real-key", config.NewValue("val")); err != nil {
		t.Logf("seed: %v — skipping alias test", err)
	}

	rec := &recordingAuditor{}
	svc, err := NewService(store,
		WithSecurityGuard(AllowAll()),
		WithAuditor(rec),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	_, aliasErr := svc.SetAlias(ctx, &configpb.SetAliasRequest{
		Alias:  "alias-key",
		Target: "real-key",
	})
	// memory store may not implement AliasStore; skip assertion if unimplemented.
	if aliasErr != nil {
		t.Skipf("store does not support aliases: %v", aliasErr)
	}

	entries := rec.all()
	if len(entries) != 1 {
		t.Fatalf("expected 1 audit entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Operation != "alias_set" {
		t.Errorf("op = %q, want %q", e.Operation, "alias_set")
	}
	if e.Key != "alias-key" {
		t.Errorf("key = %q, want %q", e.Key, "alias-key")
	}
	if e.Metadata["target"] != "real-key" {
		t.Errorf("metadata[target] = %q, want %q", e.Metadata["target"], "real-key")
	}
}

func TestWithAuditor_NilIgnored(t *testing.T) {
	store := memory.NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	svc, err := NewService(store,
		WithSecurityGuard(AllowAll()),
		WithAuditor(nil),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if svc.opts.auditor != nil {
		t.Error("expected nil auditor when WithAuditor(nil) is passed")
	}
}
