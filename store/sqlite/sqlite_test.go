package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/blob/blobtest"
	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/store/sqlstore/sqltest"
	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/wal"
	"github.com/r-hashi01/kairo/wal/waltest"
)

func TestSinkConformance(t *testing.T) {
	waltest.Run(t, func(t testing.TB, dir string) wal.Sink {
		s, err := OpenSink(filepath.Join(dir, "log.db"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, waltest.Options{Durable: true, Fenced: true})
}

func TestStoreConformance(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "kv.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	blobtest.Run(t, s, blobtest.Options{ArbitraryNames: true})
}

// The shared SQL suite (ADR 0020), on one SQLite database.
func TestMeasure(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "measure.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqltest.Measure(t, db, Dialect)
}

func TestSQLSuite(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "suite.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqltest.Run(t, db, Dialect)
}

// --- the engine on SQLite --------------------------------------------------

const fiveNodes = `{"name":"five","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"n1","action":"llm","input":{"q":"$input.q"}},
  {"kind":"step","id":"n2","action":"llm","input":{"p":"n1.in.q"}},
  {"kind":"step","id":"n3","action":"llm","input":{"p":"n2.in.p"}},
  {"kind":"step","id":"n4","action":"llm","input":{"p":"n3.in.p"}},
  {"kind":"step","id":"n5","action":"llm","input":{"p":"n4.in.p"}}]}}`

const approve = `{"name":"approve","root":{"kind":"seq","nodes":[
  {"kind":"wait","id":"w","signal":"go"},
  {"kind":"step","id":"a","action":"llm","input":{"by":"w.payload"}}]}}`

type env struct {
	e     *engine.Engine
	store *Store
}

// start runs an engine on the SQLite backend in dir.
func start(t testing.TB, dir string, cfg engine.Config) *env {
	t.Helper()
	st, err := OpenStore(filepath.Join(dir, "objects.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Sinks = Sinks(filepath.Join(dir, "wal"))
	cfg.DoneLogs = DoneLogs(filepath.Join(dir, "wal"))
	cfg.Snapshots, cfg.Blobs = st, st
	x := startWith(t, cfg)
	x.store = st
	return x
}

// startWith runs an engine with cfg's storage, the test plans and an echo
// executor.
func startWith(t testing.TB, cfg engine.Config) *env {
	t.Helper()
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "llm", Effect: ir.EffectUnprotected})
	cfg.Registry = reg
	if cfg.Shards == 0 {
		cfg.Shards = 2
	}
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{fiveNodes, approve} {
		d, _ := ir.ParseDefinition([]byte(p))
		if _, err := e.RegisterPlan(d); err != nil {
			t.Fatal(err)
		}
	}
	e.RegisterExecutor([]string{"llm"}, 64, engine.ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		b, _ := json.Marshal(map[string]json.RawMessage{"in": tk.Input})
		return task.Result{Output: b}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	return &env{e: e}
}

func (x *env) close() {
	x.e.Close()
	if x.store != nil {
		x.store.Close()
	}
}

func wait(t testing.TB, e *engine.Engine, id string) engine.RunInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ri, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatalf("wait %s: %v", id, err)
	}
	return ri
}

func waitFor(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func logRows(t testing.TB, dir string) (rows int) {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "wal", "*.db"))
	for _, f := range files {
		db, err := Open(f)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		db.QueryRow(`SELECT count(*) FROM kairo_log`).Scan(&n)
		db.Close()
		rows += n
	}
	return rows
}

// The same scenario as engine's TestLogCompaction, on SQLite: compaction
// deletes finished runs' rows, and a waiting (evicted) run survives a
// restart of the compacted log.
func TestEngineCompactionAndRecovery(t *testing.T) {
	dir := t.TempDir()
	ft := engine.TierFile
	x := start(t, dir, engine.Config{Shards: 1, CompactEvery: 64, EvictAfter: 50 * time.Millisecond})
	waiting, _ := submit(x.e, engine.SubmitRequest{Plan: "approve", Tenant: "t", Tier: &ft})
	waitFor(t, func() bool { ri, _ := x.e.Get(context.Background(), waiting); return ri.Evicted })
	for i := 0; i < 400; i++ {
		id, _ := submit(x.e, engine.SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		wait(t, x.e, id)
	}
	waitFor(t, func() bool {
		id, _ := submit(x.e, engine.SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"y"}`), Tenant: "t", Tier: &ft})
		wait(t, x.e, id)
		return logRows(t, dir) < 1000
	})
	t.Logf("log rows after ~%d records: %d", 400*13, logRows(t, dir))
	x.close()

	y := start(t, dir, engine.Config{Shards: 1})
	defer y.close()
	if st := y.e.Stats(); st.Active != 1 {
		t.Fatalf("recovered %d active runs, want 1", st.Active)
	}
	y.e.Signal(waiting, "go", json.RawMessage(`"bob"`))
	if ri := wait(t, y.e, waiting); ri.Status != "completed" || !strings.Contains(string(ri.Output), `"by":"bob"`) {
		t.Fatalf("%+v", ri)
	}
}

// submit is Submit for tests that only need the run id.
func submit(e *engine.Engine, req engine.SubmitRequest) (string, error) {
	r, err := e.Submit(context.Background(), req)
	return r.RunID, err
}
