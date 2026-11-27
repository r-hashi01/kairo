package kairo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// marks stands for the marks the database or the OS keeps: an owner's is
// there while its process lives.
type marks struct {
	mu   sync.Mutex
	held map[string]bool // owner -> alive (absent: never held one)
}

type fakeLiveness struct {
	m     *marks
	owner string
}

func (l *fakeLiveness) Hold(_ context.Context, owner string) error {
	l.m.mu.Lock()
	defer l.m.mu.Unlock()
	l.m.held[owner], l.owner = true, owner
	return nil
}

func (l *fakeLiveness) Gone(_ context.Context, owner string) bool {
	l.m.mu.Lock()
	defer l.m.mu.Unlock()
	alive, known := l.m.held[owner]
	return known && !alive
}

func (l *fakeLiveness) Lost() bool { return false }

func (l *fakeLiveness) Close() error {
	l.m.mu.Lock()
	defer l.m.mu.Unlock()
	delete(l.m.held, l.owner)
	return nil
}

// liveStore is a MemStore shared by processes, with liveness; dead stops
// its process's writes, as for a process that stopped.
type liveStore struct {
	*MemStore
	m    *marks
	dead atomic.Bool
}

var errStopped = errors.New("the process stopped")

func (s *liveStore) Liveness() Liveness { return &fakeLiveness{m: s.m} }

func (s *liveStore) LeaseOwners(_ context.Context, now int64) ([]string, error) {
	s.MemStore.mu.Lock()
	defer s.MemStore.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, acts := range s.MemStore.leases {
		for _, l := range acts {
			if l.Until >= now && l.Owner != "" && !seen[l.Owner] {
				seen[l.Owner] = true
				out = append(out, l.Owner)
			}
		}
	}
	return out, nil
}

func (s *liveStore) WithRun(ctx context.Context, id string, fn func(*RunRow) (*Changes, error)) error {
	if s.dead.Load() {
		return errStopped
	}
	return s.MemStore.WithRun(ctx, id, fn)
}

func (s *liveStore) RenewLeases(ctx context.Context, owner string, until int64) error {
	if s.dead.Load() {
		return errStopped
	}
	return s.MemStore.RenewLeases(ctx, owner, until)
}

func (s *liveStore) ExpiredLeases(ctx context.Context, now int64, limit int) ([]LeaseRow, error) {
	if s.dead.Load() {
		return nil, errStopped
	}
	return s.MemStore.ExpiredLeases(ctx, now, limit)
}

func (s *liveStore) HandOver(ctx context.Context, l LeaseRow) error {
	if s.dead.Load() {
		return errStopped
	}
	return s.MemStore.HandOver(ctx, l)
}

func (s *liveStore) Close() error { return nil } // shared

// A process that is gone (its mark gone) has its workflow taken up long
// before its lease expires; the real call it made is not made again. One
// that lives is not taken for gone (ADR 0086).
func TestGoneOwnerIsTakenUpBeforeItsLeaseExpires(t *testing.T) {
	shared, m := NewMemStore(), &marks{held: map[string]bool{}}
	var writes atomic.Int32
	start := func(s *liveStore, owner string, hang bool) *Kairo {
		k, err := Open(context.Background(), Options{Store: s, Owner: owner, Lease: 30 * time.Second, OwnerCheck: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		Action(k, "write", Real, func(_ *TaskContext, p string) (string, error) {
			writes.Add(1)
			return "wrote " + p, nil
		})
		Workflow(k, "w", func(ctx *Context, p string) (string, error) {
			w, err := Call[string](ctx, "write", p)
			if err != nil || hang {
				<-ctx.Done()
				return "", ctx.Err()
			}
			return w + "!", nil
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	s1 := &liveStore{MemStore: shared, m: m}
	k1 := start(s1, "p1", true)
	defer k1.Close()
	if _, err := k1.Submit(context.Background(), "w", "f", WithID("gone-1")); err != nil {
		t.Fatal(err)
	}
	waitFor := func(cond func() bool) {
		t.Helper()
		for i := 0; i < 400 && !cond(); i++ {
			time.Sleep(25 * time.Millisecond)
		}
		if !cond() {
			t.Fatal("condition not reached")
		}
	}
	waitFor(func() bool {
		cs, _ := k1.Children(context.Background(), "gone-1")
		return len(cs) == 1 && cs[0].Status == "completed"
	})

	// While p1 lives, another process does not take its work.
	s2 := &liveStore{MemStore: shared, m: m}
	k2 := start(s2, "p2", false)
	defer k2.Close()
	if took, err := k2.rt.checkOwners(context.Background()); err != nil || took {
		t.Fatalf("a live owner taken for gone: %v %v", took, err)
	}

	s1.dead.Store(true) // stops: its leases are not renewed
	m.mu.Lock()
	m.held["p1"] = false // and its mark goes, as with its process
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := Await[string](ctx, k2, "gone-1") // well within the 30s lease
	if err != nil || out != "wrote f!" {
		t.Fatalf("%q %v", out, err)
	}
	if writes.Load() != 1 {
		t.Fatalf("wrote %d times", writes.Load())
	}
}
