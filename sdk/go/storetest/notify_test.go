package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
	"github.com/r-hashi01/kairo/sdk/go/pgnotify"
)

// A run settled in another process wakes the wait here at once, through
// LISTEN/NOTIFY: no tick, no polling.
func TestPostgresNotifyWakesWaits(t *testing.T) {
	dsn := os.Getenv("KAIRO_SDK_PG_DSN")
	if dsn == "" {
		t.Skip("KAIRO_SDK_PG_DSN not set")
	}
	prefix := fmt.Sprintf("kgon%d_", os.Getpid())
	openK := func() *kairo.Kairo {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		store, err := kairo.NewSQLStore(db, kairo.Postgres, prefix)
		if err != nil {
			t.Fatal(err)
		}
		k, err := kairo.Open(context.Background(), kairo.Options{Store: pgnotify.Wrap(store, dsn)})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Workflow(k, "approve", func(ctx *kairo.Context, _ any) (string, error) { return kairo.WaitFor[string](ctx, "ok") })
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	t.Cleanup(func() {
		db, _ := sql.Open("pgx", dsn)
		defer db.Close()
		for _, tbl := range []string{"run", "event", "timer", "lease"} {
			db.Exec("DROP TABLE IF EXISTS " + prefix + tbl)
		}
	})
	a, b := openK(), openK()
	defer a.Close()
	defer b.Close()
	out := make(chan string, 1)
	go func() {
		v, err := kairo.Run[string](context.Background(), a, "approve", nil, kairo.WithID("n-1"))
		if err != nil {
			t.Error(err)
		}
		out <- v
	}()
	// The other process delivers the signal (once the wait is there).
	deadline := time.Now().Add(10 * time.Second)
	for b.Signal(context.Background(), "n-1", "ok", "by b") != nil {
		if time.Now().After(deadline) {
			t.Fatal("no wait to signal")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case v := <-out:
		if v != "by b" {
			t.Fatalf("%q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait here was not woken by the run settled in the other process")
	}
}
