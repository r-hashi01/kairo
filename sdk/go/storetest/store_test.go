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
	"time"

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

// A transaction whose context ends while it runs (the process closes)
// still ends: it does not leave SQLite's write lock held from the other
// processes on the file. The context ends at moments spread over the
// transaction, beginning and commit included.
func TestSQLiteTransactionEndsWithItsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo.db")
	a, b := openSQLite(t, path), openSQLite(t, path)
	for _, s := range []kairo.Store{a, b} {
		if err := s.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	timers := func(run string) func(*kairo.RunRow) (*kairo.Changes, error) {
		return func(*kairo.RunRow) (*kairo.Changes, error) {
			ts := make([]kairo.TimerRow, 20)
			for i := range ts {
				ts[i] = kairo.TimerRow{Run: run, Timer: uint32(i + 1), Act: 1, At: 1}
			}
			return &kairo.Changes{SetTimers: ts}, nil
		}
	}
	for i := range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(i%40)*25*time.Microsecond, cancel)
		_ = a.WithRun(ctx, "r", timers("r"))
		cancel()
		done := make(chan error, 1)
		go func() { done <- b.WithRun(context.Background(), "s", timers("s")) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("after %d: another process could not write: %v", i, err)
			}
		case <-time.After(4 * time.Second): // under the store's busy timeout (5s)
			t.Fatalf("after %d: another process waits for a lock left held", i)
		}
	}
}
