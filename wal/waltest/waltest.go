// Package waltest is a conformance suite for wal.Sink implementations
// (ADR 0016, 0018). Every sink the engine may use should pass it.
package waltest

import (
	"fmt"
	"testing"

	"kairo/wal"
)

// Opener opens the sink stored in dir. Opening the same dir again must
// resume the same log (if Durable).
type Opener func(t testing.TB, dir string) wal.Sink

type Options struct {
	// Durable: the log survives Close and a later open of the same dir.
	Durable bool
	// Fenced: opening the same log again takes it over; the earlier
	// handle's appends fail from then on (ADR 0020).
	Fenced bool
}

func batch(from, n int) []byte {
	var b []byte
	for i := from; i < from+n; i++ {
		b = wal.Frame(b, []byte(fmt.Sprintf("rec-%04d", i)))
	}
	return b
}

func read(t *testing.T, s wal.Sink) (lsns []uint64, recs []string) {
	t.Helper()
	if err := s.ReadAll(func(l uint64, r []byte) error {
		lsns = append(lsns, l)
		recs = append(recs, string(r))
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return
}

func continuous(t *testing.T, lsns []uint64, recs []string) {
	t.Helper()
	for i := range lsns {
		if i > 0 && lsns[i] != lsns[i-1]+1 {
			t.Fatalf("LSN gap: %d after %d", lsns[i], lsns[i-1])
		}
		if want := fmt.Sprintf("rec-%04d", lsns[i]); recs[i] != want {
			t.Fatalf("LSN %d holds %q, want %q", lsns[i], recs[i], want)
		}
	}
}

// Run runs the suite.
func Run(t *testing.T, open Opener, o Options) {
	t.Run("AppendRead", func(t *testing.T) {
		s := open(t, t.TempDir())
		defer s.Close()
		if s.Next() != 1 {
			t.Fatalf("empty log: Next = %d, want 1", s.Next())
		}
		for _, n := range []int{1, 5, 30} {
			start := int(s.Next())
			if err := s.Append(batch(start, n)); err != nil {
				t.Fatal(err)
			}
		}
		lsns, recs := read(t, s)
		if len(lsns) != 36 || lsns[0] != 1 || s.Next() != 37 {
			t.Fatalf("read %d records from %v, Next %d", len(lsns), lsns[:1], s.Next())
		}
		continuous(t, lsns, recs)
	})

	if o.Durable {
		t.Run("Reopen", func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir)
			s.Append(batch(1, 10))
			s.Close()
			s = open(t, dir)
			if s.Next() != 11 {
				t.Fatalf("after reopen: Next = %d, want 11", s.Next())
			}
			s.Append(batch(11, 5))
			lsns, recs := read(t, s)
			if len(lsns) != 15 {
				t.Fatalf("after reopen: %d records", len(lsns))
			}
			continuous(t, lsns, recs)
			s.Close()
		})
	}

	if o.Fenced {
		t.Run("Fencing", func(t *testing.T) {
			dir := t.TempDir()
			old := open(t, dir)
			defer old.Close()
			if err := old.Append(batch(1, 3)); err != nil {
				t.Fatal(err)
			}
			cur := open(t, dir)
			defer cur.Close()
			if err := old.Append(batch(4, 1)); err == nil {
				t.Fatal("a superseded owner could still append")
			}
			if err := cur.Append(batch(4, 2)); err != nil {
				t.Fatalf("new owner: %v", err)
			}
			if r, ok := old.(wal.Retirer); ok {
				if err := r.Retire(3); err == nil {
					t.Fatal("a superseded owner could still retire")
				}
			}
			lsns, recs := read(t, cur)
			if len(lsns) != 5 {
				t.Fatalf("%d records after takeover", len(lsns))
			}
			continuous(t, lsns, recs)
		})
	}

	t.Run("Retire", func(t *testing.T) {
		dir := t.TempDir()
		s := open(t, dir)
		r, ok := s.(wal.Retirer)
		if !ok {
			s.Close()
			t.Skip("not a Retirer")
		}
		for i := 1; i <= 100; i += 10 {
			s.Append(batch(i, 10))
		}
		if err := r.Retire(42); err != nil {
			t.Fatal(err)
		}
		lsns, recs := read(t, s)
		// Records >= 42 must all be there; older ones may or may not be.
		if len(lsns) == 0 || lsns[0] > 42 || lsns[len(lsns)-1] != 100 {
			t.Fatalf("after Retire(42): %v..%v", lsns[:1], lsns[len(lsns)-1:])
		}
		continuous(t, lsns, recs)
		// Retire everything: numbering continues, also across a reopen.
		r.Retire(s.Next())
		if s.Next() != 101 {
			t.Fatalf("Next after full retire = %d", s.Next())
		}
		s.Append(batch(101, 1))
		if o.Durable {
			s.Close()
			s = open(t, dir)
			if s.Next() != 102 {
				t.Fatalf("Next after full retire and reopen = %d, want 102", s.Next())
			}
		}
		lsns, recs = read(t, s)
		if len(lsns) == 0 || lsns[len(lsns)-1] != 101 {
			t.Fatalf("after full retire: %v", lsns)
		}
		continuous(t, lsns, recs)
		s.Close()
	})
}
