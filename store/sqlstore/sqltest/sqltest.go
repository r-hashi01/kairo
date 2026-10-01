// Package sqltest runs the storage conformance suites and the engine's
// durability scenarios against a SQL database, for every product module
// (ADR 0020).
package sqltest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"kairo/blob/blobtest"
	"kairo/engine"
	"kairo/ir"
	"kairo/store/sqlstore"
	"kairo/task"
	"kairo/wal"
	"kairo/wal/waltest"
)

// namespace makes each test (dir) use its own rows in the shared tables.
func namespace(s string) string {
	h := sha256.Sum256([]byte(s))
	return "t-" + hex.EncodeToString(h[:8])
}

// Run runs everything against db.
func Run(t *testing.T, db *sql.DB, d sqlstore.Dialect) {
	t.Run("Sink", func(t *testing.T) {
		waltest.Run(t, func(t testing.TB, dir string) wal.Sink {
			s, err := sqlstore.OpenSink(db, d, "log", sqlstore.Options{Namespace: namespace(dir)})
			if err != nil {
				t.Fatal(err)
			}
			return s
		}, waltest.Options{Durable: true, Fenced: true})
	})
	t.Run("Store", func(t *testing.T) {
		s, err := sqlstore.OpenStore(db, d, sqlstore.Options{Namespace: namespace(t.TempDir())})
		if err != nil {
			t.Fatal(err)
		}
		blobtest.Run(t, s, blobtest.Options{ArbitraryNames: true})
	})
	t.Run("EngineCompactionAndRecovery", func(t *testing.T) { engineScenario(t, db, d) })
}

const fiveNodes = `{"name":"five","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"n1","action":"llm","input":{"q":"$input.q"}},
  {"kind":"step","id":"n2","action":"llm","input":{"p":"n1.in.q"}},
  {"kind":"step","id":"n3","action":"llm","input":{"p":"n2.in.p"}},
  {"kind":"step","id":"n4","action":"llm","input":{"p":"n3.in.p"}},
  {"kind":"step","id":"n5","action":"llm","input":{"p":"n4.in.p"}}]}}`

const approve = `{"name":"approve","root":{"kind":"seq","nodes":[
  {"kind":"wait","id":"w","signal":"go"},
  {"kind":"step","id":"a","action":"llm","input":{"by":"w.payload"}}]}}`

// Engine starts an engine on db with the test plans and an echo executor.
func Engine(t testing.TB, db *sql.DB, d sqlstore.Dialect, o sqlstore.Options, cfg engine.Config) *engine.Engine {
	t.Helper()
	b, err := sqlstore.NewBackend(db, d, o)
	if err != nil {
		t.Fatal(err)
	}
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "llm", Effect: ir.EffectUnprotected})
	cfg.Registry, cfg.Sinks, cfg.Snapshots, cfg.Blobs = reg, b.Sinks, b.Store, b.Store
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{fiveNodes, approve} {
		def, _ := ir.ParseDefinition([]byte(p))
		if _, err := e.RegisterPlan(def); err != nil {
			t.Fatal(err)
		}
	}
	e.RegisterExecutor([]string{"llm"}, 64, engine.ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		out, _ := json.Marshal(map[string]json.RawMessage{"in": tk.Input})
		return task.Result{Output: out}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	return e
}

func wait(t testing.TB, e *engine.Engine, id string) engine.RunInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ri, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatalf("wait %s: %v", id, err)
	}
	return ri
}

func waitFor(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// engineScenario: a waiting run is evicted, many runs finish and the log
// is compacted, the engine restarts on the compacted log, and the waiting
// run resumes; finished runs stay finished.
func engineScenario(t *testing.T, db *sql.DB, d sqlstore.Dialect) {
	o := sqlstore.Options{Namespace: namespace(t.Name() + time.Now().String())}
	ft := engine.TierFile
	e1 := Engine(t, db, d, o, engine.Config{Shards: 2, CompactEvery: 64, EvictAfter: 50 * time.Millisecond})
	waiting, _ := e1.Submit(engine.SubmitRequest{Plan: "approve", Tenant: "t", Tier: &ft})
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), waiting); return ri.Evicted })
	for i := 0; i < 200; i++ {
		id, _ := e1.Submit(engine.SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		wait(t, e1, id)
	}
	rows := func() (n int) {
		// Read without taking ownership (that would fence the engine).
		for shard := 0; shard < 2; shard++ {
			if err := sqlstore.ReadLog(db, d, fmt.Sprintf("shard-%03d/file", shard), o, func(uint64, []byte) error { n++; return nil }); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	waitFor(t, func() bool {
		id, _ := e1.Submit(engine.SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"y"}`), Tenant: "t", Tier: &ft})
		wait(t, e1, id)
		return rows() < 600
	})
	t.Logf("log rows left after ~%d records: %d", 200*13, rows())
	e1.Close()

	e2 := Engine(t, db, d, o, engine.Config{Shards: 2})
	defer e2.Close()
	if st := e2.Stats(); st.Active != 1 {
		t.Fatalf("recovered %d active runs, want 1", st.Active)
	}
	e2.Signal(waiting, "go", json.RawMessage(`"bob"`))
	if ri := wait(t, e2, waiting); ri.Status != "completed" || !strings.Contains(string(ri.Output), `"by":"bob"`) {
		t.Fatalf("%+v", ri)
	}
}
