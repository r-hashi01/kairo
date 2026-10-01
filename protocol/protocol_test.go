package protocol

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kairo/engine"
	"kairo/ir"
	"kairo/task"
)

func setup(t *testing.T) (*engine.Engine, string) {
	t.Helper()
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "code.run", Effect: ir.EffectUnprotected})
	reg.Register(ir.NodeSpec{Action: "send", Effect: ir.EffectReal})
	e, err := engine.New(engine.Config{Shards: 2, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	dir, _ := os.MkdirTemp("", "kairo")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "w.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go (&Server{E: e}).Serve(l)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e, sock
}

func plan(t *testing.T, e *engine.Engine, js string) {
	d, _ := ir.ParseDefinition([]byte(js))
	if _, err := e.RegisterPlan(d); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteWorker(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"map","id":"m","over":"$input.xs","body":
	  {"kind":"step","id":"sq","action":"code.run","input":{"x":"$item"}}}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := &Worker{Name: "w1", Actions: []string{"code.run"}, Concurrency: 4, Handler: func(_ context.Context, tk *task.Task, emit func([]byte)) task.Result {
		var in struct{ X int }
		json.Unmarshal(tk.Input, &in)
		emit([]byte("working"))
		out, _ := json.Marshal(in.X * in.X)
		return task.Result{Output: out}
	}}
	go wk.Run(ctx, "unix", sock)
	id, err := e.Submit(engine.SubmitRequest{Plan: "p", Tenant: "t", Input: json.RawMessage(`{"xs":[1,2,3,4,5,6,7,8,9,10]}`)})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wc := context.WithTimeout(context.Background(), 5*time.Second)
	defer wc()
	ri, err := e.Wait(wctx, id)
	if err != nil || string(ri.Output) != "[1,4,9,16,25,36,49,64,81,100]" {
		t.Fatalf("%v %+v", err, ri)
	}
}

// A worker dying mid-task: the unprotected task is retried elsewhere; the
// real one has an unknown outcome and stops for review.
func TestWorkerDisconnect(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"calc","action":"code.run"},{"kind":"step","id":"send","action":"send"}]}}`)
	ctx1, kill := context.WithCancel(context.Background())
	var got atomic.Int32
	crashed := make(chan struct{})
	t.Cleanup(func() { close(crashed) })
	// The handler never returns: the process "crashes" with both tasks in hand.
	hang := &Worker{Name: "dying", Actions: []string{"code.run", "send"}, Concurrency: 2, Handler: func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		got.Add(1)
		<-crashed
		return task.Result{Err: "crashed", Unknown: true}
	}}
	go hang.Run(ctx1, "unix", sock)
	id, _ := e.Submit(engine.SubmitRequest{Plan: "p", Tenant: "t"})
	for got.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	kill()
	ctx2, stop := context.WithCancel(context.Background())
	defer stop()
	good := &Worker{Name: "good", Actions: []string{"code.run"}, Concurrency: 1, Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
		return task.Result{Output: json.RawMessage(`42`)}
	}}
	go good.Run(ctx2, "unix", sock)
	deadline := time.Now().Add(5 * time.Second)
	for {
		ri, _ := e.Get(context.Background(), id)
		if ri.Status == "blocked" && len(ri.Reviews) == 1 && strings.HasPrefix(ri.Reviews[0].StepID, "send") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not block for review: %+v", ri)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
