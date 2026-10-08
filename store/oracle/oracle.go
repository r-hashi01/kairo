// Package oracle is the Oracle Database backend of kairo (ADR 0020): the
// shared SQL implementation (store/sqlstore) with the pure Go driver
// go-ora (no Instant Client needed). Oracle 19c and later. The default
// storage remains the file format.
//
//	db, err := oracle.Open(os.Getenv("KAIRO_ORACLE_DSN"), oracle.Options{})
//	b, err := sqlstore.NewBackend(db, oracle.Dialect, sqlstore.Options{Namespace: "prod"})
package oracle

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	goora "github.com/sijms/go-ora/v2"

	"github.com/r-hashi01/kairo/store/sqlstore"
)

// Dialect is the Oracle dialect. Tables are created with a plain CREATE
// TABLE (IF NOT EXISTS needs 23ai), ignoring ORA-00955.
var Dialect = sqlstore.Dialect{
	Name: "oracle", Placeholder: sqlstore.Colon,
	IDType: "VARCHAR2(512)", BigInt: "NUMBER(19)", BytesType: "BLOB",
	ForUpdate: " FOR UPDATE", InsertRows: insertAll,
	DeleteLimited: func(t, w string, n int) string {
		return fmt.Sprintf("DELETE FROM %s WHERE %s AND ROWNUM <= %d", t, w, n)
	},
	PartSize: 1 << 20, RetireBatch: 10000,
	NoCreateIfNotExists: true,
	TableExists:         func(err error) bool { return strings.Contains(err.Error(), "ORA-00955") },
}

// insertAll builds INSERT ALL INTO t (cols) VALUES (...) ... SELECT 1 FROM
// DUAL: a multi-row insert that Oracle supports before 23ai.
func insertAll(table, cols string, rows, ncols int, ph func(int) string) string {
	var b strings.Builder
	b.WriteString("INSERT ALL")
	n := 1
	for r := 0; r < rows; r++ {
		fmt.Fprintf(&b, " INTO %s (%s) VALUES (", table, cols)
		for c := 0; c < ncols; c++ {
			if c > 0 {
				b.WriteString(", ")
			}
			b.WriteString(ph(n))
			n++
		}
		b.WriteByte(')')
	}
	b.WriteString(" SELECT 1 FROM DUAL")
	return b.String()
}

type Options struct {
	// MaxOpenConns caps the connection pool (default 16). Databases limit
	// sessions; an unbounded pool can exhaust them under load.
	MaxOpenConns int
	// AllowInsecureTransport permits connections without TLS with server
	// verification (SSL=true and SSL VERIFY=true). Only for tests and
	// trusted local setups (ADR 0020).
	AllowInsecureTransport bool
}

// ErrInsecure: the DSN would connect without verified TLS.
var ErrInsecure = errors.New(`oracle: connection is not TLS with server verification (set SSL=true and keep "SSL VERIFY" on, or Options.AllowInsecureTransport)`)

// Open opens an Oracle database from a go-ora URL
// (oracle://user:pass@host:port/service?SSL=true). It refuses transports
// that are not verified TLS. Errors never contain the DSN.
func Open(dsn string, o Options) (*sql.DB, error) {
	cfg, err := goora.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("oracle: cannot parse the DSN")
	}
	if !o.AllowInsecureTransport && !(cfg.SSL && cfg.SSLVerify) {
		return nil, ErrInsecure
	}
	return sqlstore.Pool(sql.OpenDB(goora.NewConnector(dsn)), o.MaxOpenConns), nil
}
