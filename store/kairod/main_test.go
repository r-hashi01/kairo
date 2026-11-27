package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/kairod"
	"github.com/r-hashi01/kairo/task"
)

// The number of shards is kept with the data: a later kairod gets the
// stored number, and an explicit other number is refused.
func TestShardsArePinnedInTheStore(t *testing.T) {
	st := blob.NewMem()
	if n, err := pinShards(st, 4, false); err != nil || n != 4 {
		t.Fatalf("first: %d %v", n, err)
	}
	if n, err := pinShards(st, 8, false); err != nil || n != 4 {
		t.Fatalf("default on another machine: %d %v", n, err)
	}
	if _, err := pinShards(st, 8, true); err == nil || !strings.Contains(err.Error(), "created with 4 shards") {
		t.Fatalf("explicit other: %v", err)
	}
}

// A DSN of no known kind is refused without echoing it (it may hold
// credentials).
func TestUnknownStoreIsRefusedWithoutTheDSN(t *testing.T) {
	_, err := open("redis://user:secret@host", kairod.StoreOptions{})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("%v", err)
	}
}

// With KAIRO_STORE, the durable tiers live in the database: a kairod that
// lost its data directory (a disk that failed) takes up the runs from the
// database alone, finished and waiting ones (ADR 0087).
func TestRunsOutliveTheDataDirectory(t *testing.T) {
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "kairo.db")
	var plan ir.Definition
	if err := json.Unmarshal([]byte(`{"name":"p","root":{"kind":"seq","nodes":[
		{"kind":"step","id":"a","action":"act"},
		{"kind":"wait","id":"w","signal":"go"}]}}`), &plan); err != nil {
		t.Fatal(err)
	}
	start := func(dataDir string) (*engine.Engine, func()) {
		st, err := open(dsn, kairod.StoreOptions{Namespace: "test"})
		if err != nil {
			t.Fatal(err)
		}
		n, err := st.Shards(2, false)
		if err != nil {
			t.Fatal(err)
		}
		cfg := engine.Config{Shards: n, DataDir: dataDir, DefaultTier: engine.TierFile}
		st.Configure(&cfg)
		e, err := engine.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.RegisterPlan(&plan); err != nil {
			t.Fatal(err)
		}
		e.RegisterExecutor([]string{"act"}, 1, engine.ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
			return task.Result{Output: json.RawMessage(`"done"`)}
		}))
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e, func() { e.Close(); st.Close() }
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e1, stop1 := start(t.TempDir())
	for _, id := range []string{"finished", "waiting"} {
		if _, err := e1.Submit(ctx, engine.SubmitRequest{Plan: "p", Tenant: "t", RunID: id}); err != nil {
			t.Fatal(err)
		}
	}
	e1.Signal("finished", "go", json.RawMessage(`"now"`))
	if ri, err := e1.Wait(ctx, "finished"); err != nil || ri.Status != "completed" {
		t.Fatalf("finished: %+v %v", ri, err)
	}
	stop1()

	// Another data directory: as on another machine.
	e2, stop2 := start(t.TempDir())
	defer stop2()
	if ri, err := e2.Get(ctx, "finished"); err != nil || ri.Status != "completed" {
		t.Fatalf("finished, after: %+v %v", ri, err)
	}
	e2.Signal("waiting", "go", json.RawMessage(`"later"`))
	if ri, err := e2.Wait(ctx, "waiting"); err != nil || ri.Status != "completed" {
		t.Fatalf("waiting, after: %+v %v", ri, err)
	}
}
