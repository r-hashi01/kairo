// Package mysql is the MySQL and TiDB backend of kairo (ADR 0020): the
// shared SQL implementation (store/sqlstore) with the go-sql-driver/mysql
// driver. TiDB speaks the MySQL protocol; it has its own dialect for its
// transaction size limits. The default storage remains the file format.
//
//	db, err := mysql.Open(os.Getenv("KAIRO_MYSQL_DSN"), mysql.Options{})
//	b, err := sqlstore.NewBackend(db, mysql.MySQL, sqlstore.Options{Namespace: "prod"})
package mysql

import (
	"database/sql"
	"errors"
	"fmt"

	driver "github.com/go-sql-driver/mysql"

	"kairo/store/sqlstore"
)

func limited(t, w string, n int) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT %d", t, w, n)
}

// MySQL is the MySQL (8.0+) dialect. Names are VARBINARY(512): compared
// as bytes, case-sensitively, whatever the server's default collation
// (the default utf8mb4_0900_ai_ci would merge "Run-A" and "run-a", ADR 0025).
var MySQL = sqlstore.Dialect{
	Name: "mysql", Placeholder: sqlstore.Q,
	IDType: "VARBINARY(512)", BigInt: "BIGINT", BytesType: "LONGBLOB",
	ForUpdate: " FOR UPDATE", MultiRowInsert: true, DeleteLimited: limited,
	PartSize: 1 << 20, RetireBatch: 10000,
}

// TiDB is the TiDB dialect: smaller retire batches and parts, to stay
// under TiDB's entry and transaction size limits.
var TiDB = sqlstore.Dialect{
	Name: "tidb", Placeholder: sqlstore.Q,
	IDType: "VARBINARY(512)", BigInt: "BIGINT", BytesType: "LONGBLOB",
	ForUpdate: " FOR UPDATE", MultiRowInsert: true, DeleteLimited: limited,
	PartSize: 512 << 10, RetireBatch: 2000,
}

type Options struct {
	// MaxOpenConns caps the connection pool (default 16). Databases limit
	// sessions; an unbounded pool can exhaust them under load.
	MaxOpenConns int
	// AllowInsecureTransport permits connections without verified TLS
	// (tls=false, skip-verify, preferred). Only for tests and trusted
	// local setups (ADR 0020).
	AllowInsecureTransport bool
}

// ErrInsecure: the DSN would connect without verified TLS.
var ErrInsecure = errors.New("mysql: connection is not TLS with server verification (use tls=true or a registered config, or Options.AllowInsecureTransport)")

// Open opens a MySQL or TiDB database from a DSN. It refuses transports
// that are not verified TLS, and turns off driver features that weaken
// SQL injection defenses: multiStatements and client-side
// interpolateParams. Errors never contain the DSN.
func Open(dsn string, o Options) (*sql.DB, error) {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, errors.New("mysql: cannot parse the DSN")
	}
	cfg.MultiStatements = false
	cfg.InterpolateParams = false
	if !o.AllowInsecureTransport && (cfg.TLS == nil || cfg.TLS.InsecureSkipVerify || cfg.AllowFallbackToPlaintext) {
		return nil, ErrInsecure
	}
	c, err := driver.NewConnector(cfg)
	if err != nil {
		return nil, errors.New("mysql: invalid configuration")
	}
	return sqlstore.Pool(sql.OpenDB(c), o.MaxOpenConns), nil
}
