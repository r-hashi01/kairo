// Package postgres is the PostgreSQL backend of kairo (ADR 0020): the
// shared SQL implementation (store/sqlstore) with the PostgreSQL dialect
// and the pgx driver. The default storage remains the file format.
//
//	db, err := postgres.Open(os.Getenv("KAIRO_PG_DSN"), postgres.Options{})
//	b, err := sqlstore.NewBackend(db, postgres.Dialect, sqlstore.Options{Namespace: "prod"})
//	cfg := engine.Config{}
//	b.Configure(&cfg) // Sinks, DoneLogs, Snapshots, Blobs
//	engine.New(cfg)
package postgres

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"kairo/store/sqlstore"
)

// Dialect is the PostgreSQL dialect.
var Dialect = sqlstore.Dialect{
	Name: "postgres", Placeholder: sqlstore.Dollar,
	IDType: `VARCHAR(512) COLLATE "C"`, BigInt: "BIGINT", BytesType: "BYTEA", // byte order, for prefix ranges (ADR 0025)
	ForUpdate: " FOR UPDATE", MultiRowInsert: true,
	DeleteLimited: func(t, w string, n int) string {
		return fmt.Sprintf("DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE %s LIMIT %d)", t, t, w, n)
	},
	PartSize: 1 << 20, RetireBatch: 10000,
}

type Options struct {
	// MaxOpenConns caps the connection pool (default 16). Databases limit
	// sessions; an unbounded pool can exhaust them under load.
	MaxOpenConns int
	// AllowInsecureTransport permits connections without TLS or without
	// full server verification (sslmode other than verify-full). Only for
	// tests and trusted local setups (ADR 0020).
	AllowInsecureTransport bool
}

// ErrInsecure: the DSN would connect without verified TLS.
var ErrInsecure = errors.New("postgres: connection is not TLS with full verification (use sslmode=verify-full, or Options.AllowInsecureTransport)")

// Open opens a PostgreSQL database from a DSN, refusing transports that
// are not verified TLS. Errors never contain the DSN.
func Open(dsn string, o Options) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("postgres: cannot parse the DSN")
	}
	if !o.AllowInsecureTransport && !verified(cfg) {
		return nil, ErrInsecure
	}
	return sqlstore.Pool(stdlib.OpenDB(*cfg), o.MaxOpenConns), nil
}

// verified: every way the driver may connect (including fallbacks such as
// sslmode=prefer's plaintext retry) is TLS with full verification.
func verified(cfg *pgx.ConnConfig) bool {
	ok := func(t *tls.Config) bool { return t != nil && !t.InsecureSkipVerify }
	if !ok(cfg.TLSConfig) {
		return false
	}
	for _, f := range cfg.Fallbacks {
		if !ok(f.TLSConfig) {
			return false
		}
	}
	return true
}
