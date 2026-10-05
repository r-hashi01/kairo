package engine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kairo/wal"
)

// ADR 0027: a finished run is still recognized after a restart, once its
// records are retired and only its marker is left.
func TestIdempotencyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Shards: 1, DataDir: dir, NoSync: true, SegmentSize: 4 << 10, CompactEvery: 64}
	ft := TierFile
	start := func() *Engine {
		e := newEngine(t, cfg)
		mustPlan(t, e, fiveNodes)
		e.RegisterExecutor([]string{"llm"}, 8, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	e1 := start()
	in := json.RawMessage(`{"q":"x"}`)
	if _, err := submit(e1, SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "Done-1"}); err != nil {
		t.Fatal(err)
	}
	wait(t, e1, "Done-1")
	waitFor(t, func() bool {
		id, _ := submit(e1, SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft})
		wait(t, e1, id)
		segs := walSegments(t, dir)
		return !strings.HasSuffix(filepath.Base(segs[0]), ".L00000000000000000001.wal")
	})
	e1.Close()

	e2 := start()
	defer e2.Close()
	r, err := e2.Submit(context.Background(), SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "Done-1"})
	if err != nil || !r.Existing {
		t.Fatalf("resubmitted after restart: %+v %v", r, err)
	}
	ri, err := e2.Get(context.Background(), "Done-1")
	if err != nil || !ri.Trimmed || ri.Status != "completed" || ri.FinishedAt.IsZero() {
		t.Fatalf("Get: %+v %v", ri, err)
	}
	if wr := wait(t, e2, "Done-1"); !wr.Trimmed {
		t.Fatalf("Wait: %+v", wr)
	}
	if st := e2.Stats(); st.Active != 0 {
		t.Fatalf("%d runs started", st.Active)
	}
}

// Markers expire after IdempotencyTTL; the done log is retired behind them.
func TestMarkersExpireAndRetire(t *testing.T) {
	defer func(n int) { markerRetireEvery = n }(markerRetireEvery)
	markerRetireEvery = 1
	var skew atomic.Int64
	sink := &wal.MemSink{}
	ft := TierFile
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, RecentRuns: 1,
		DoneLogs: func(int) (wal.Sink, error) { return sink, nil },
		Now:      func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }})
	defer e.Close()
	mustPlan(t, e, fiveNodes)
	e.RegisterExecutor([]string{"llm"}, 8, echoExec())
	e.Start()
	in := json.RawMessage(`{"q":"x"}`)
	run := func(id string) SubmitResult {
		r, err := e.Submit(context.Background(), SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: id})
		if err != nil {
			t.Fatal(err)
		}
		wait(t, e, id)
		return r
	}
	records := func() (n int) {
		sink.ReadAll(func(uint64, []byte) error { n++; return nil })
		return
	}
	for _, id := range []string{"a", "b", "c"} {
		run(id)
	}
	run("d") // moves "a" out of the in-memory cache
	if r := run("a"); !r.Existing {
		t.Fatal("a run within IdempotencyTTL started again")
	}
	waitFor(t, func() bool { return records() == 4 })
	skew.Store(int64(25 * time.Hour))
	if r := run("e"); r.Existing {
		t.Fatal(r)
	}
	waitFor(t, func() bool { return records() == 1 })
	if r := run("a"); r.Existing {
		t.Fatal("a marker past IdempotencyTTL still counted")
	}
}

// A run that finished but whose marker never became durable (the done log
// failed) keeps its snapshot and records; the next start writes the
// marker before cleaning up.
func TestMarkerWrittenOnRecovery(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	plan := `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`
	broken := &wal.MemSink{Delay: func() { panic("unreachable") }}
	failing := failingSink{broken}
	e1 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond,
		DoneLogs: func(int) (wal.Sink, error) { return failing, nil }})
	mustPlan(t, e1, plan)
	e1.Start()
	if _, err := submit(e1, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft, RunID: "x"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, err := e1.snaps.Get("snap/x"); return err == nil })
	e1.Signal("x", "go", nil)
	wait(t, e1, "x")
	waitFor(t, func() bool { return e1.Stats().FailedLogs == 1 })
	if _, err := e1.snaps.Get("snap/x"); err != nil {
		t.Fatalf("snapshot deleted before the marker was durable: %v", err)
	}
	e1.Close()

	e2 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true})
	defer e2.Close()
	mustPlan(t, e2, plan)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	if r, _ := e2.Submit(context.Background(), SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft, RunID: "x"}); !r.Existing {
		t.Fatal("finished run started again")
	}
	if _, err := e2.snaps.Get("snap/x"); err == nil {
		t.Fatal("snapshot left behind")
	}
}

type failingSink struct{ *wal.MemSink }

func (failingSink) Append([]byte) error { return errors.New("disk on fire") }

// Beyond IdempotencyMax the oldest markers are forgotten first.
func TestMarkerCap(t *testing.T) {
	ft := TierFile
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, RecentRuns: 1, IdempotencyMax: 3})
	defer e.Close()
	mustPlan(t, e, fiveNodes)
	e.RegisterExecutor([]string{"llm"}, 8, echoExec())
	e.Start()
	in := json.RawMessage(`{"q":"x"}`)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		submit(e, SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: id})
		wait(t, e, id)
	}
	if r, _ := e.Submit(context.Background(), SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "e"}); !r.Existing {
		t.Fatal("newest marker dropped")
	}
	if r, _ := e.Submit(context.Background(), SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "a"}); r.Existing {
		t.Fatal("oldest marker kept beyond IdempotencyMax")
	}
}

// Markers are encrypted like every other log (ADR 0021).
func TestEncryptedMarkers(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	keys := testKeys(t, 1)
	e1 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, Keys: keys})
	mustPlan(t, e1, fiveNodes)
	e1.RegisterExecutor([]string{"llm"}, 2, echoExec())
	e1.Start()
	submit(e1, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft, RunID: "run-visible-name"})
	wait(t, e1, "run-visible-name")
	e1.Close()
	if segs, _ := filepath.Glob(filepath.Join(dir, "wal", "done-*.wal")); len(segs) == 0 {
		t.Fatal("no done log written")
	}
	if hits := filesContain(t, dir, "run-visible-name"); len(hits) > 0 {
		t.Fatalf("run id stored in clear in %v", hits)
	}
	e2 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, Keys: keys})
	defer e2.Close()
	mustPlan(t, e2, fiveNodes)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	if ri, err := e2.Get(context.Background(), "run-visible-name"); err != nil || !ri.Trimmed {
		t.Fatalf("%+v %v", ri, err)
	}
}

// A run whose records are still in the log at a restart after
// IdempotencyTTL is not marked again: the TTL counts from its finish.
func TestRecoveryKeepsFinishTime(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	var skew atomic.Int64
	cfg := Config{Shards: 1, DataDir: dir, NoSync: true, Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }}
	start := func() *Engine {
		e := newEngine(t, cfg)
		mustPlan(t, e, fiveNodes)
		e.RegisterExecutor([]string{"llm"}, 2, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	in := json.RawMessage(`{"q":"x"}`)
	e1 := start()
	submit(e1, SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "x"})
	wait(t, e1, "x")
	e1.Close()

	e2 := start() // records of x still in the log, marker fresh
	if r, _ := e2.Submit(context.Background(), SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "x"}); !r.Existing {
		t.Fatal("started again within IdempotencyTTL")
	}
	e2.Close()

	skew.Store(int64(25 * time.Hour))
	e3 := start()
	defer e3.Close()
	if r, _ := e3.Submit(context.Background(), SubmitRequest{Plan: "five", Input: in, Tenant: "t", Tier: &ft, RunID: "x"}); r.Existing {
		t.Fatal("recovery re-stamped an expired run's marker")
	}
}
