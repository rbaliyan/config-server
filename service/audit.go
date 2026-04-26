package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Auditor records an audit entry. Implementations must be safe for concurrent use.
type Auditor interface {
	Record(ctx context.Context, entry AuditEntry) error
}

// AuditEntry carries all metadata for a single auditable operation.
type AuditEntry struct {
	Timestamp time.Time
	Identity  Identity
	Operation string
	Namespace string
	Key       string
	// For set operations: base64-encoded value bytes. Empty for deletes.
	Value    string
	Codec    string
	Metadata map[string]string
}

// LogAuditor writes audit entries as structured slog records at Info level.
type LogAuditor struct {
	logger *slog.Logger
}

var _ Auditor = (*LogAuditor)(nil)

// NewLogAuditor returns an Auditor that writes each entry to logger at Info level.
func NewLogAuditor(logger *slog.Logger) Auditor {
	return &LogAuditor{logger: logger}
}

func (a *LogAuditor) Record(ctx context.Context, entry AuditEntry) error {
	userID := ""
	if entry.Identity != nil {
		userID = entry.Identity.UserID()
	}
	a.logger.InfoContext(ctx, "audit",
		slog.Time("ts", entry.Timestamp),
		slog.String("user_id", userID),
		slog.String("operation", entry.Operation),
		slog.String("namespace", entry.Namespace),
		slog.String("key", entry.Key),
		slog.String("value", entry.Value),
		slog.String("codec", entry.Codec),
		slog.Any("metadata", entry.Metadata),
	)
	return nil
}

// auditOptions holds configuration for SQLAuditor.
type auditOptions struct {
	table string
}

// AuditOption configures a SQLAuditor.
type AuditOption func(*auditOptions)

// WithAuditTable sets the audit log table name. Default is "config_audit_log".
func WithAuditTable(name string) AuditOption {
	return func(o *auditOptions) {
		if name != "" {
			o.table = name
		}
	}
}

// SQLAuditor persists audit entries to a SQL table. Supports PostgreSQL and SQLite.
type SQLAuditor struct {
	db       *sql.DB
	postgres bool
	table    string
}

var _ Auditor = (*SQLAuditor)(nil)

// NewSQLAuditor creates a SQLAuditor. driverName must match the driver passed to
// sql.Open (e.g. "postgres", "pgx", or "sqlite3"). Call CreateTable before first use.
func NewSQLAuditor(db *sql.DB, driverName string, opts ...AuditOption) *SQLAuditor {
	o := &auditOptions{table: "config_audit_log"}
	for _, opt := range opts {
		opt(o)
	}
	return &SQLAuditor{
		db:       db,
		postgres: isPostgres(driverName),
		table:    o.table,
	}
}

// isPostgres reports whether the driver name denotes PostgreSQL.
func isPostgres(driver string) bool {
	d := strings.ToLower(driver)
	return strings.Contains(d, "postgres") || d == "pgx"
}

// CreateTable creates the audit log table if it does not already exist.
// Safe to call on every startup.
func (a *SQLAuditor) CreateTable(ctx context.Context) error {
	var ddl string
	if a.postgres {
		ddl = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id        BIGSERIAL PRIMARY KEY,
	ts        TIMESTAMPTZ NOT NULL,
	user_id   TEXT NOT NULL,
	operation TEXT NOT NULL,
	namespace TEXT NOT NULL,
	key       TEXT NOT NULL,
	value     TEXT,
	codec     TEXT,
	metadata  TEXT
)`, a.table)
	} else {
		ddl = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id        INTEGER PRIMARY KEY AUTOINCREMENT,
	ts        TEXT NOT NULL,
	user_id   TEXT NOT NULL,
	operation TEXT NOT NULL,
	namespace TEXT NOT NULL,
	key       TEXT NOT NULL,
	value     TEXT,
	codec     TEXT,
	metadata  TEXT
)`, a.table)
	}
	_, err := a.db.ExecContext(ctx, ddl)
	return err
}

// Record inserts an audit entry row. metadata is JSON-encoded.
func (a *SQLAuditor) Record(ctx context.Context, entry AuditEntry) error {
	userID := ""
	if entry.Identity != nil {
		userID = entry.Identity.UserID()
	}

	var metaJSON string
	if len(entry.Metadata) > 0 {
		b, err := json.Marshal(entry.Metadata)
		if err != nil {
			return fmt.Errorf("audit: marshal metadata: %w", err)
		}
		metaJSON = string(b)
	}

	var q string
	if a.postgres {
		q = fmt.Sprintf(
			`INSERT INTO %s (ts, user_id, operation, namespace, key, value, codec, metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			a.table,
		)
	} else {
		q = fmt.Sprintf(
			`INSERT INTO %s (ts, user_id, operation, namespace, key, value, codec, metadata) VALUES (?,?,?,?,?,?,?,?)`,
			a.table,
		)
	}

	_, err := a.db.ExecContext(ctx, q,
		entry.Timestamp,
		userID,
		entry.Operation,
		entry.Namespace,
		entry.Key,
		entry.Value,
		entry.Codec,
		metaJSON,
	)
	return err
}

// MultiAuditor fans out Record calls to all contained auditors.
type MultiAuditor struct {
	auditors []Auditor
}

var _ Auditor = (*MultiAuditor)(nil)

// NewMultiAuditor returns an Auditor that records to every provided auditor.
// Record collects all errors and joins them.
func NewMultiAuditor(auditors ...Auditor) Auditor {
	return &MultiAuditor{auditors: auditors}
}

func (m *MultiAuditor) Record(ctx context.Context, entry AuditEntry) error {
	var errs []error
	for _, a := range m.auditors {
		if err := a.Record(ctx, entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
