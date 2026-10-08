package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/task"
)

const bigPlan = `{"name":"big","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"gen","action":"llm"},
  {"kind":"wait","id":"w","signal":"go"},
  {"kind":"step","id":"last","action":"llm"}]}}`

var big = strings.Repeat("x", 50_000)

func bigExec() Executor {
	return ExecutorFunc(func(_ context.Context, t *task.Task, _ func([]byte)) task.Result {
		return task.Result{Output: json.RawMessage(`{"doc":"` + big + `","step":"` + t.StepID + `"}`)}
	})
}

// ADR 0024: a run's blobs live until it finishes; a large final output is
// inlined before they go; other runs' blobs are untouched.
func TestBlobsDeletedWhenRunFinishes(t *testing.T) {
	blobs := blob.NewMem()
	e := newEngine(t, Config{Shards: 1, BlobThreshold: 1024, Blobs: blobs})
	defer e.Close()
	mustPlan(t, e, bigPlan)
	e.RegisterExecutor([]string{"llm"}, 2, bigExec())
	e.Start()
	a, _ := submit(e, SubmitRequest{Plan: "big", Tenant: "t"})
	b, _ := submit(e, SubmitRequest{Plan: "big", Tenant: "t"})
	waitFor(t, func() bool { return blobs.Len() == 2 })
	e.Signal(a, "go", nil)
	ri := wait(t, e, a)
	if !strings.Contains(string(ri.Output), big) || strings.Contains(string(ri.Output), `"$blob"`) {
		t.Fatalf("final output not inlined (%d bytes)", len(ri.Output))
	}
	waitFor(t, func() bool { return blobs.Len() == 1 }) // only b's blob left
	if got, err := e.Get(context.Background(), a); err != nil || !strings.Contains(string(got.Output), big) {
		t.Fatalf("Get after the blobs are gone: %v", err)
	}
	e.Signal(b, "go", nil)
	wait(t, e, b)
	waitFor(t, func() bool { return blobs.Len() == 0 })
}

// Blobs left behind by a crash between finishing and deleting are removed
// when recovery finds the run finished.
func TestLeftoverBlobsDeletedOnRecovery(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	e1 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, BlobThreshold: 1024})
	mustPlan(t, e1, bigPlan)
	e1.RegisterExecutor([]string{"llm"}, 2, bigExec())
	e1.Start()
	id, _ := submit(e1, SubmitRequest{Plan: "big", Tenant: "t", Tier: &ft})
	e1.Signal(id, "go", nil)
	wait(t, e1, id)
	e1.Close()
	// Simulate the crash: the run's blob is still there.
	st, _ := blob.NewDir(dir+"/blobs", true)
	leftover := blob.RunGroup(id) + "/sha256:leftover"
	st.Put(leftover, []byte("x"))

	e2 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true})
	defer e2.Close()
	mustPlan(t, e2, bigPlan)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(leftover); err == nil {
		t.Fatal("leftover blob of a finished run survived recovery")
	}
}
