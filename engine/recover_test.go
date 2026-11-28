package engine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/blob"
)

const (
	approvePlan = `{"name":"approve","root":{"kind":"seq","nodes":[{"kind":"wait","id":"w","signal":"go"},{"kind":"step","id":"a","action":"llm","input":{"by":"w.payload"}}]}}`
	onePlan     = `{"name":"one","root":{"kind":"step","id":"s","action":"llm","input":{"q":"$input.q"}}}`
)

// retiredStartEngine starts an engine in dir with a waiting run whose start
// record has been retired (ADR 0016): the run lives on in its snapshot and
// checkpoint record alone. The run has a blob in its group. It returns the
// running engine, the run's id and its blob key.
func retiredStartEngine(t *testing.T, cfg func() Config) (*Engine, string, string) {
	t.Helper()
	dir := cfg().DataDir
	e := newEngine(t, cfg())
	mustPlan(t, e, approvePlan)
	mustPlan(t, e, onePlan)
	e.RegisterExecutor([]string{"llm"}, 4, echoExec())
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	id, err := submit(e, SubmitRequest{Plan: "approve", Tenant: "t", Tier: &ft})
	if err != nil {
		t.Fatal(err)
	}
	key, err := blob.PutRunContent(e.blobs, id, []byte("a blob of the waiting run"))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { ri, _ := e.Get(context.Background(), id); return ri.Evicted })
	// Other runs grow the log until compaction has moved the waiting run's
	// checkpoint forward and retired the oldest segment, which holds its
	// start record. Bounded by a count of runs, not by time.
	retired := func() bool {
		segs := walSegments(t, dir)
		return len(segs) > 0 && !strings.HasSuffix(filepath.Base(segs[0]), ".L00000000000000000001.wal")
	}
	for i := 0; !retired(); i++ {
		if i == 2000 {
			t.Fatalf("the start record was not retired after %d runs: %v", i, walSegments(t, dir))
		}
		rid, err := submit(e, SubmitRequest{Plan: "one", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		if err != nil {
			t.Fatal(err)
		}
		if ri := wait(t, e, rid); ri.Status != "completed" {
			t.Fatalf("%+v", ri)
		}
	}
	return e, id, key
}

func compactingConfig(dir string, ttl time.Duration) func() Config {
	return func() Config {
		return Config{Shards: 1, DataDir: dir, NoSync: true, SegmentSize: 1 << 10, CompactEvery: 8,
			EvictAfter: time.Millisecond, IdempotencyTTL: ttl}
	}
}

// A waiting run whose start record was retired lives on in its snapshot.
// If that snapshot is gone or unreadable, nothing in the records says the
// run finished: recovery must refuse to start instead of taking it for
// finished, and must leave its blobs. With the snapshot back, the run
// recovers as if nothing had happened.
func TestMissingSnapshotRefusesToStart(t *testing.T) {
	for _, damage := range []string{"removed", "unreadable"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			cfg := compactingConfig(dir, 0)
			e1, id, key := retiredStartEngine(t, cfg)
			e1.Close()

			snaps, err := blob.NewDir(filepath.Join(dir, "snapshots"), true)
			if err != nil {
				t.Fatal(err)
			}
			snap, err := snaps.Get("snap/" + id)
			if err != nil {
				t.Fatalf("the waiting run's snapshot: %v", err)
			}
			if damage == "removed" {
				err = snaps.Delete("snap/" + id)
			} else {
				err = snaps.Put("snap/"+id, []byte("not a snapshot"))
			}
			if err != nil {
				t.Fatal(err)
			}

			e2 := newEngine(t, cfg())
			mustPlan(t, e2, approvePlan)
			mustPlan(t, e2, onePlan)
			err = e2.Start()
			e2.Close()
			if !errors.Is(err, errRunHistoryMissing) || !strings.Contains(err.Error(), id) {
				t.Fatalf("Start with the snapshot %s: %v, want %v naming run %s", damage, err, errRunHistoryMissing, id)
			}
			blobs, err := blob.NewDir(filepath.Join(dir, "blobs"), true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := blobs.Get(key); err != nil {
				t.Fatalf("the run's blob after the refused start: %v", err)
			}

			// Nothing was given up: with the snapshot restored, the run is
			// still there and finishes.
			if err := snaps.Put("snap/"+id, snap); err != nil {
				t.Fatal(err)
			}
			e3 := newEngine(t, cfg())
			defer e3.Close()
			mustPlan(t, e3, approvePlan)
			mustPlan(t, e3, onePlan)
			e3.RegisterExecutor([]string{"llm"}, 4, echoExec())
			if err := e3.Start(); err != nil {
				t.Fatal(err)
			}
			e3.Signal(id, "go", json.RawMessage(`"bob"`))
			if ri := wait(t, e3, id); ri.Status != "completed" || !strings.Contains(string(ri.Output), `"by":"bob"`) {
				t.Fatalf("restored run: %+v", ri)
			}
		})
	}
}

// The other side: a run that finished after its start record was retired
// has its snapshot deleted while its later records may stay in the log
// (an idle engine retires nothing). That is no missing snapshot: the log
// holds the run's records after its last checkpoint, so the restart goes
// on and the run stays finished, with or without finished-run markers
// (ADR 0027).
func TestFinishedRunWithRetiredStartRecovers(t *testing.T) {
	for _, ttl := range []time.Duration{0, -1} {
		t.Run(map[bool]string{true: "markers", false: "no markers"}[ttl >= 0], func(t *testing.T) {
			dir := t.TempDir()
			cfg := compactingConfig(dir, ttl)
			e1, id, key := retiredStartEngine(t, cfg)
			segs := walSegments(t, dir)
			e1.Signal(id, "go", json.RawMessage(`"bob"`))
			if ri := wait(t, e1, id); ri.Status != "completed" {
				t.Fatalf("%+v", ri)
			}
			snaps, err := blob.NewDir(filepath.Join(dir, "snapshots"), true)
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { _, err := snaps.Get("snap/" + id); return errors.Is(err, blob.ErrNotFound) })
			e1.Close()
			if now := walSegments(t, dir); len(now) == 0 || now[0] != segs[0] {
				t.Fatalf("the run's records were retired (%v, then %v): the test needs them in the log", segs, now)
			}

			e2 := newEngine(t, cfg())
			defer e2.Close()
			mustPlan(t, e2, approvePlan)
			mustPlan(t, e2, onePlan)
			e2.RegisterExecutor([]string{"llm"}, 4, echoExec())
			if err := e2.Start(); err != nil {
				t.Fatal(err)
			}
			if st := e2.Stats(); st.Active != 0 {
				t.Fatalf("recovered %d active runs, want 0 (the finished run must stay finished)", st.Active)
			}
			blobs, err := blob.NewDir(filepath.Join(dir, "blobs"), true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := blobs.Get(key); !errors.Is(err, blob.ErrNotFound) {
				t.Fatalf("the finished run's blob: %v, want it deleted", err)
			}
		})
	}
}
