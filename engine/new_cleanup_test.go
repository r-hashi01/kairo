package engine

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/r-hashi01/kairo/wal"
)

type newCleanupSink struct {
	*wal.MemSink
	closes atomic.Int32
}

func trackedNewSink() *newCleanupSink { return &newCleanupSink{MemSink: &wal.MemSink{}} }

func (s *newCleanupSink) Close() error {
	s.closes.Add(1)
	return s.MemSink.Close()
}

func requireClosedOnce(t *testing.T, sinks []*newCleanupSink) {
	t.Helper()
	for i, sink := range sinks {
		if got := sink.closes.Load(); got != 1 {
			t.Errorf("sink %d closed %d times, want 1", i, got)
		}
	}
}

func TestNewFailureDoesNotLeakProcessGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	mismatch := t.TempDir()
	if err := os.WriteFile(filepath.Join(mismatch, "SHARDS"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if e, err := New(Config{Shards: 2, DataDir: mismatch, NoSync: true}); err == nil {
		e.Close()
		t.Fatal("accepted a different shard count")
	}

	badOrder := t.TempDir()
	if err := os.WriteFile(filepath.Join(badOrder, "SHARDS"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badOrder, "CREATION_ORDER"), []byte("invalid"), 0o644); err != nil {
		t.Fatal(err)
	}
	if e, err := New(Config{Shards: 1, DataDir: badOrder, NoSync: true}); err == nil {
		e.Close()
		t.Fatal("accepted an invalid creation order")
	}

	waitFor(t, func() bool { return runtime.NumGoroutine() <= before+2 })
}

func TestNewClosesOrdinarySinksAfterLaterOpenFailure(t *testing.T) {
	var opened []*newCleanupSink
	_, err := New(Config{Shards: 2, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		if tier == TierFile && shard == 1 {
			return nil, errors.New("injected sink failure")
		}
		if tier == TierMemory || tier == TierFile {
			sink := trackedNewSink()
			opened = append(opened, sink)
			return sink, nil
		}
		return nil, nil
	}})
	if err == nil {
		t.Fatal("New succeeded")
	}
	if len(opened) != 3 {
		t.Fatalf("opened %d sinks, want 3", len(opened))
	}
	requireClosedOnce(t, opened)
}

func TestNewRejectsPartialTierAcrossShards(t *testing.T) {
	var memory []*newCleanupSink
	partial := trackedNewSink()
	e, err := New(Config{Shards: 2, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		switch tier {
		case TierMemory:
			sink := trackedNewSink()
			memory = append(memory, sink)
			return sink, nil
		case TierFile:
			if shard == 0 {
				return partial, nil
			}
		}
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if e.tiers[TierFile] {
		t.Fatal("partial file tier is available")
	}
	for i, shard := range e.shards {
		if shard.logs[TierFile] != nil {
			t.Errorf("shard %d retained the partial file tier", i)
		}
	}
	if got := partial.closes.Load(); got != 1 {
		t.Fatalf("partial sink closed %d times before Engine.Close, want 1", got)
	}
	for i, sink := range memory {
		if got := sink.closes.Load(); got != 0 {
			t.Fatalf("memory sink %d closed before Engine.Close: %d", i, got)
		}
	}

	e.Close()
	if got := partial.closes.Load(); got != 1 {
		t.Fatalf("partial sink closed %d times after Engine.Close, want 1", got)
	}
	requireClosedOnce(t, memory)
}

func TestNewClosesOrdinaryAndDoneSinksAfterDoneOpenFailure(t *testing.T) {
	var ordinary []*newCleanupSink
	var done []*newCleanupSink
	_, err := New(Config{
		Shards: 2,
		Sinks: func(tier Tier, _ int) (wal.Sink, error) {
			if tier != TierMemory && tier != TierFile {
				return nil, nil
			}
			sink := trackedNewSink()
			ordinary = append(ordinary, sink)
			return sink, nil
		},
		DoneLogs: func(shard int) (wal.Sink, error) {
			if shard == 1 {
				return nil, errors.New("injected done sink failure")
			}
			sink := trackedNewSink()
			done = append(done, sink)
			return sink, nil
		},
	})
	if err == nil {
		t.Fatal("New succeeded")
	}
	if len(ordinary) != 4 || len(done) != 1 {
		t.Fatalf("opened ordinary=%d done=%d, want 4 and 1", len(ordinary), len(done))
	}
	requireClosedOnce(t, ordinary)
	requireClosedOnce(t, done)
}

func TestNewTransfersSinkOwnershipOnlyAfterSuccess(t *testing.T) {
	var ordinary []*newCleanupSink
	var done []*newCleanupSink
	e, err := New(Config{
		Shards: 1,
		Sinks: func(tier Tier, _ int) (wal.Sink, error) {
			if tier != TierMemory && tier != TierFile {
				return nil, nil
			}
			sink := trackedNewSink()
			ordinary = append(ordinary, sink)
			return sink, nil
		},
		DoneLogs: func(int) (wal.Sink, error) {
			sink := trackedNewSink()
			done = append(done, sink)
			return sink, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, sink := range append(append([]*newCleanupSink(nil), ordinary...), done...) {
		if got := sink.closes.Load(); got != 0 {
			t.Fatalf("sink closed before Engine.Close: %d", got)
		}
	}
	e.Close()
	requireClosedOnce(t, ordinary)
	requireClosedOnce(t, done)
}
