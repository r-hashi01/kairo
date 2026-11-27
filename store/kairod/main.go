// kairod runs the engine as a daemon (package kairod), and can keep its
// durable tiers in a SQL database named by KAIRO_STORE, which replicates
// them (ADR 0087):
//
//	KAIRO_STORE='postgres://user:pass@host/db?sslmode=verify-full' kairod -data ./kairo-data
//
// KAIRO_STORE starts with postgres:// (or postgresql://), mysql: or tidb:
// (followed by the driver's DSN), oracle://, or sqlite: (followed by a
// path). This is the kairod released; cmd/kairod keeps its data in files
// only.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/kairod"
	"github.com/r-hashi01/kairo/store/mysql"
	"github.com/r-hashi01/kairo/store/oracle"
	"github.com/r-hashi01/kairo/store/postgres"
	"github.com/r-hashi01/kairo/store/sqlite"
	"github.com/r-hashi01/kairo/store/sqlstore"
)

func main() { kairod.Main(open) }

// errScheme never contains the DSN (it may hold credentials).
var errScheme = errors.New("kairod: KAIRO_STORE must start with postgres://, postgresql://, mysql:, tidb:, oracle:// or sqlite:")

// open opens the store named by dsn.
func open(dsn string, o kairod.StoreOptions) (*kairod.Store, error) {
	db, d, err := openDB(dsn, o.AllowInsecureTransport)
	if err != nil {
		return nil, err
	}
	b, err := sqlstore.NewBackend(db, d, sqlstore.Options{Namespace: o.Namespace})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &kairod.Store{
		Configure: b.Configure,
		Catalog:   b.Store,
		Shards:    func(n int, explicit bool) (int, error) { return pinShards(b.Store, n, explicit) },
		Close:     db.Close,
	}, nil
}

func openDB(dsn string, insecure bool) (*sql.DB, sqlstore.Dialect, error) {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		db, err := postgres.Open(dsn, postgres.Options{AllowInsecureTransport: insecure})
		return db, postgres.Dialect, err
	case strings.HasPrefix(dsn, "mysql:"):
		db, err := mysql.Open(strings.TrimPrefix(dsn, "mysql:"), mysql.Options{AllowInsecureTransport: insecure})
		return db, mysql.MySQL, err
	case strings.HasPrefix(dsn, "tidb:"):
		db, err := mysql.Open(strings.TrimPrefix(dsn, "tidb:"), mysql.Options{AllowInsecureTransport: insecure})
		return db, mysql.TiDB, err
	case strings.HasPrefix(dsn, "oracle://"):
		db, err := oracle.Open(dsn, oracle.Options{AllowInsecureTransport: insecure})
		return db, oracle.Dialect, err
	case strings.HasPrefix(dsn, "sqlite:"):
		db, err := sqlite.Open(strings.TrimPrefix(dsn, "sqlite:"))
		return db, sqlite.Dialect, err
	}
	return nil, sqlstore.Dialect{}, errScheme
}

// shardsKey holds the number of shards, beside the data they route.
const shardsKey = "kairod/SHARDS"

// pinShards returns the number of shards stored in st, storing n if there
// is none. An explicit n other than the stored one is an error: runs are
// routed by hash, so another number would strand them.
func pinShards(st blob.Store, n int, explicit bool) (int, error) {
	b, err := st.Get(shardsKey)
	if errors.Is(err, blob.ErrNotFound) {
		return n, st.Put(shardsKey, []byte(strconv.Itoa(n)))
	}
	if err != nil {
		return 0, err
	}
	got, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || got <= 0 {
		return 0, fmt.Errorf("kairod: the store's %s is not a number of shards", shardsKey)
	}
	if explicit && got != n {
		return 0, fmt.Errorf("kairod: the store was created with %d shards, -shards asks for %d", got, n)
	}
	return got, nil
}
