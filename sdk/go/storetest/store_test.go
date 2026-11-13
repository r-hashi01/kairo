// Package storetest runs the Go SDK's suite on real databases (ADR 0058):
// SQLite always, PostgreSQL when KAIRO_SDK_PG_DSN is set. It is a module
// of its own, for the drivers (kairo itself depends on none).
package storetest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
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

func TestSQLiteSharedDestinationRates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo.db")
	a := openSQLite(t, path)
	b := openSQLite(t, path)
	for _, store := range []kairo.Store{a, b} {
		if err := store.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ra := a.(kairo.SharedRateStore)
	rb := b.(kairo.SharedRateStore)
	req := kairo.SharedRateRequest{Destination: "provider", RequestsPerMin: 600}
	if wait, err := ra.AdmitDestination(context.Background(), req); err != nil || wait != 0 {
		t.Fatalf("first admission = %v, %v", wait, err)
	}
	if wait, err := rb.AdmitDestination(context.Background(), req); err != nil || wait <= 0 {
		t.Fatalf("other process admission = %v, %v; want a wait", wait, err)
	}

	if err := a.WithRun(context.Background(), "missing", func(*kairo.RunRow) (*kairo.Changes, error) {
		return &kairo.Changes{SharedRates: []kairo.SharedRateSettlement{{Destination: "weighted", WeightPerMin: 60, Weight: 11}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if wait, err := rb.AdmitDestination(context.Background(), kairo.SharedRateRequest{Destination: "weighted", WeightPerMin: 60}); err != nil || wait < 900*time.Millisecond {
		t.Fatalf("actual weight debt = %v, %v; want about one second", wait, err)
	}

	if err := a.WithRun(context.Background(), "missing", func(*kairo.RunRow) (*kairo.Changes, error) {
		return &kairo.Changes{SharedRates: []kairo.SharedRateSettlement{{Destination: "paused", RetryAfter: time.Second}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if wait, err := rb.AdmitDestination(context.Background(), kairo.SharedRateRequest{Destination: "paused"}); err != nil || wait < 900*time.Millisecond {
		t.Fatalf("shared Retry-After = %v, %v; want about one second", wait, err)
	}
}

func TestSQLiteSharedLimitsAcrossRuntimes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo.db")
	var starts []time.Time
	var mu sync.Mutex
	open := func() *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: openSQLite(t, path)})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "shared-scope", kairo.Unprotected, func(*kairo.TaskContext, any) (any, error) { return nil, nil },
			kairo.Destination("provider"), kairo.SharedLimits())
		kairo.Action(k, "limited", kairo.Unprotected, func(*kairo.TaskContext, any) (any, error) {
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
			return nil, nil
		}, kairo.Destination("provider"), kairo.Rate(600))
		kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (any, error) {
			return kairo.Call[any](ctx, "limited", nil)
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { k.Close() })
		return k
	}
	a, b := open(), open()
	errCh := make(chan error, 2)
	for i, k := range []*kairo.Kairo{a, b} {
		go func(i int, k *kairo.Kairo) {
			_, err := kairo.Run[any](context.Background(), k, "w", nil, kairo.WithID(fmt.Sprintf("shared-%d", i)))
			errCh <- err
		}(i, k)
	}
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	gap := starts[1].Sub(starts[0])
	mu.Unlock()
	if gap < 90*time.Millisecond {
		t.Fatalf("starts were only %v apart; shared rate was not enforced", gap)
	}
}

type ambiguousRateStore struct {
	kairo.Store
	rates  kairo.SharedRateStore
	failed atomic.Bool
}

func (s *ambiguousRateStore) WithRun(ctx context.Context, id string, fn func(*kairo.RunRow) (*kairo.Changes, error)) error {
	settled := false
	err := s.Store.WithRun(ctx, id, func(row *kairo.RunRow) (*kairo.Changes, error) {
		changes, err := fn(row)
		settled = changes != nil && len(changes.SharedRates) > 0
		return changes, err
	})
	if err == nil && settled && s.failed.CompareAndSwap(false, true) {
		return errors.New("commit answer lost")
	}
	return err
}

func (s *ambiguousRateStore) AdmitDestination(ctx context.Context, request kairo.SharedRateRequest) (time.Duration, error) {
	return s.rates.AdmitDestination(ctx, request)
}

func (s *ambiguousRateStore) PruneDestinationRates(ctx context.Context, limit int) (int, error) {
	return s.rates.PruneDestinationRates(ctx, limit)
}

func TestSharedSettlementIsIdempotentAfterAmbiguousCommit(t *testing.T) {
	base := openSQLite(t, filepath.Join(t.TempDir(), "kairo.db"))
	store := &ambiguousRateStore{Store: base, rates: base.(kairo.SharedRateStore)}
	k, err := kairo.Open(context.Background(), kairo.Options{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "weighted", kairo.Unprotected, func(ctx *kairo.TaskContext, _ any) (any, error) {
		ctx.SetWeight(11)
		return nil, nil
	}, kairo.WeightRate(60), kairo.SharedLimits())
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (any, error) { return kairo.Call[any](ctx, "weighted", nil) })
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := kairo.Run[any](context.Background(), k, "w", nil, kairo.WithID("ambiguous")); err != nil {
		t.Fatal(err)
	}
	wait, err := store.AdmitDestination(context.Background(), kairo.SharedRateRequest{Destination: "weighted", WeightPerMin: 60})
	if err != nil {
		t.Fatal(err)
	}
	if wait > 2*time.Second {
		t.Fatalf("weight was settled twice after an ambiguous commit: wait %v", wait)
	}
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
			for _, tbl := range []string{"run", "event", "timer", "lease", "rate"} {
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
