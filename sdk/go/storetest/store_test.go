// Package storetest runs the Go SDK's suite on real databases (ADR 0058):
// SQLite always, PostgreSQL when KAIRO_SDK_PG_DSN is set. It is a module
// of its own, for the drivers (kairo itself depends on none).
package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	kairo "github.com/r-hashi01/kairo/sdk/go"
	"github.com/r-hashi01/kairo/sdk/go/kairotest"
)

// openSQLite opens a new handle on the database file (as a new process
// would).
func openSQLite(t *testing.T, path string) kairo.Store {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s, err := kairo.NewSQLStore(db, kairo.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSQLite(t *testing.T) {
	kairotest.Run(t, func(t *testing.T) kairotest.Opener {
		path := filepath.Join(t.TempDir(), "kairo.db")
		return func() kairo.Store { return openSQLite(t, path) }
	})
}

var pgSeq atomic.Int64

func TestPostgres(t *testing.T) {
	dsn := os.Getenv("KAIRO_SDK_PG_DSN")
	if dsn == "" {
		t.Skip("KAIRO_SDK_PG_DSN not set")
	}
	kairotest.Run(t, func(t *testing.T) kairotest.Opener {
		// A database of its own: tables under a prefix of their own.
		prefix := fmt.Sprintf("kgo%d_%d_", os.Getpid(), pgSeq.Add(1))
		t.Cleanup(func() {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				return
			}
			defer db.Close()
			for _, tbl := range []string{"run", "event", "timer", "lease"} {
				db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+prefix+tbl)
			}
		})
		return func() kairo.Store {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			s, err := kairo.NewSQLStore(db, kairo.Postgres, prefix)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
	})
}
