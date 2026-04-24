// Package sqlownership provides a SQL-backed implementation of
// peersync.OwnershipStore. It supports PostgreSQL and SQLite via the standard
// database/sql interface.
//
// # Setup
//
//	db, _ := sql.Open("sqlite3", "data.db")
//	os := sqlownership.New(db, "sqlite3")
//	if err := os.CreateTable(ctx); err != nil { ... }
//	store, _ := peersync.New(local, self, transport,
//	    peersync.WithOwnershipStore(os),
//	)
//
// # Driver names
//
// Pass the same driver name you used with sql.Open. The package uses this to
// select the correct upsert dialect:
//   - Any name containing "pg" or "postgres" (e.g. "postgres", "pgx",
//     "postgresql") → PostgreSQL dialect with ON CONFLICT ... DO UPDATE
//   - anything else (e.g. "sqlite3", "sqlite") → SQLite/INSERT OR REPLACE dialect
package sqlownership

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rbaliyan/config-server/peersync"
)

var _ peersync.OwnershipStore = (*Store)(nil)

// Store implements peersync.OwnershipStore backed by a SQL database.
// Create with New; call CreateTable before first use.
type Store struct {
	db       *sql.DB
	driver   string
	postgres bool
	table    string
}

type options struct {
	table string
}

func defaultOptions() options {
	return options{table: "peersync_ownership"}
}

// Option configures a Store.
type Option func(*options)

// WithTable sets the table name. Default is "peersync_ownership".
func WithTable(name string) Option {
	return func(o *options) {
		if name != "" {
			o.table = name
		}
	}
}

// New creates a Store. driverName must match the driver passed to sql.Open
// (e.g. "postgres", "pgx", or "sqlite3"). Call CreateTable before first use.
func New(db *sql.DB, driverName string, opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &Store{
		db:       db,
		driver:   driverName,
		postgres: isPostgres(driverName),
		table:    o.table,
	}
}

// isPostgres reports whether the driver name denotes PostgreSQL. Recognises
// the common names "postgres", "postgresql", and "pgx" case-insensitively.
func isPostgres(driver string) bool {
	d := strings.ToLower(driver)
	return strings.Contains(d, "postgres") || d == "pgx"
}

// CreateTable creates the ownership table if it does not already exist.
// Safe to call on every startup.
func (s *Store) CreateTable(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			namespace TEXT NOT NULL PRIMARY KEY,
			node_id   TEXT NOT NULL
		)`, s.table))
	return err
}

// LoadOwned returns all namespaces owned by nodeID.
func (s *Store) LoadOwned(ctx context.Context, nodeID string) ([]string, error) {
	q := fmt.Sprintf(`SELECT namespace FROM %s WHERE node_id = %s`, s.table, s.placeholder(1))
	rows, err := s.db.QueryContext(ctx, q, nodeID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ns []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		ns = append(ns, n)
	}
	return ns, rows.Err()
}

// SaveOwner records nodeID as the owner of namespace, replacing any previous
// owner record.
func (s *Store) SaveOwner(ctx context.Context, namespace, nodeID string) error {
	var q string
	if s.postgres {
		q = fmt.Sprintf(
			`INSERT INTO %s (namespace, node_id) VALUES ($1, $2) ON CONFLICT (namespace) DO UPDATE SET node_id = EXCLUDED.node_id`,
			s.table,
		)
	} else {
		q = fmt.Sprintf(`INSERT OR REPLACE INTO %s (namespace, node_id) VALUES (?, ?)`, s.table)
	}
	_, err := s.db.ExecContext(ctx, q, namespace, nodeID)
	return err
}

// DeleteOwner removes the ownership record for namespace.
func (s *Store) DeleteOwner(ctx context.Context, namespace string) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE namespace = %s`, s.table, s.placeholder(1))
	_, err := s.db.ExecContext(ctx, q, namespace)
	return err
}

// placeholder returns the SQL positional placeholder for the given 1-based
// argument index. PostgreSQL uses $N; everything else uses ?.
func (s *Store) placeholder(n int) string {
	if s.postgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}
