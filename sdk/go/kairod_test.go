package kairo_test

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/api"
	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/protocol"
	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// kairod, in this process: its engine, HTTP API and worker socket, as
// cmd/kairod assembles them.
func startKairod(t *testing.T) (url, sock string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "kd") // short: a unix socket's path is
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	e, err := engine.New(engine.Config{Shards: 2, DataDir: filepath.Join(dir, "data"), NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	sock = filepath.Join(dir, "w.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	go (&protocol.Server{E: e}).Serve(l)
	srv := httptest.NewServer((&api.API{E: e}).Handler())
	t.Cleanup(func() {
		srv.Close()
		l.Close()
		e.Close()
	})
	return srv.URL, sock
}

// The same workflow code on kairod (Connect, ADR 0058): calls, a sleep, a
// signal, a resume by another process that runs nothing twice, and a
// version read back from kairod (GetInput, ADR 0060).
func TestOnKairod(t *testing.T) {
	url, sock := startKairod(t)
	var writes, llms atomic.Int32
	var hang atomic.Bool
	unknownCalls := make(chan string, 2)
	hang.Store(true)
	connect := func(versions ...string) *kairo.Kairo {
		k, err := kairo.Connect(context.Background(), url, sock, kairo.Options{})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) {
			llms.Add(1)
			return strings.ToUpper(q), nil
		})
		kairo.Action(k, "write", kairo.Real, func(_ *kairo.TaskContext, p string) (string, error) {
			writes.Add(1)
			return "wrote " + p, nil
		})
		kairo.Action(k, "lost", kairo.Real, func(tc *kairo.TaskContext, _ any) (string, error) {
			unknownCalls <- tc.RunID
			return "", kairo.Unknown(errors.New("connection lost"))
		})
		for i, v := range versions {
			opts := []kairo.WorkflowOption{kairo.WorkflowVersion(v)}
			if i < len(versions)-1 {
				opts = append(opts, kairo.Draining())
			}
			kairo.Workflow(k, "edit", func(ctx *kairo.Context, f string) (string, error) {
				a, err := kairo.Call[string](ctx, "llm", f)
				if err != nil {
					return "", err
				}
				if err := ctx.Sleep(50 * time.Millisecond); err != nil {
					return "", err
				}
				w, err := kairo.Call[string](ctx, "write", f)
				if err != nil {
					return "", err
				}
				if hang.Load() {
					<-ctx.Done() // as a process that stops here
					return "", ctx.Err()
				}
				by, err := kairo.WaitFor[string](ctx, "approve")
				return v + ": " + a + ", " + w + ", by " + by, err
			}, opts...)
		}
		kairo.Workflow(k, "uncertain", func(ctx *kairo.Context, _ any) (string, error) {
			out, err := kairo.Call[string](ctx, "lost", nil)
			if err == nil {
				return out, nil
			}
			var ce *kairo.CallError
			if errors.As(err, &ce) {
				return "failed: " + ce.Message, nil
			}
			return "", err
		})
		kairo.Workflow(k, "tree-hold", func(ctx *kairo.Context, _ any) (string, error) {
			<-ctx.Done()
			return "stopped", ctx.Err()
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	ctx := context.Background()

	k1 := connect("1")
	if _, err := k1.Submit(ctx, "edit", "a", kairo.WithID("kd-1"), kairo.WithMeta(map[string]any{"owner": "alice"})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for writes.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("not written")
		}
		time.Sleep(5 * time.Millisecond)
	}
	k1.Close()

	hang.Store(false)
	k2 := connect("1", "2")
	defer k2.Close()
	res := make(chan string, 1)
	go func() {
		out, err := kairo.Run[string](ctx, k2, "edit", "a", kairo.WithID("kd-1"))
		if err != nil {
			t.Error(err)
		}
		res <- out
	}()
	for k2.Signal(ctx, "kd-1", "approve", "alice") != nil {
		time.Sleep(10 * time.Millisecond)
	}
	if out := <-res; out != "1: A, wrote a, by alice" {
		t.Fatalf("%q", out)
	}
	if writes.Load() != 1 || llms.Load() != 1 {
		t.Fatalf("ran again: write %d, llm %d", writes.Load(), llms.Load())
	}
	type outcome struct {
		out string
		err error
	}
	runUncertain := func(id string) <-chan outcome {
		ch := make(chan outcome, 1)
		go func() {
			out, err := kairo.Run[string](ctx, k2, "uncertain", nil, kairo.WithID(id))
			ch <- outcome{out: out, err: err}
		}()
		return ch
	}
	nextCall := func() string {
		select {
		case id := <-unknownCalls:
			return id
		case <-time.After(10 * time.Second):
			t.Fatal("unknown call did not run")
			return ""
		}
	}

	succeeded := runUncertain("kd-resolve-ok")
	call := nextCall()
	deadline = time.Now().Add(10 * time.Second)
	for {
		ri, err := k2.Get(ctx, call)
		if err == nil && ri.Status == "blocked" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("call did not stop for review: %+v %v", ri, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := k2.Resolve(ctx, call, "confirmed"); err != nil {
		t.Fatal(err)
	}
	if got := <-succeeded; got.err != nil || got.out != "confirmed" {
		t.Fatalf("Resolve: %q %v", got.out, got.err)
	}
	if err := k2.Resolve(ctx, call, "confirmed"); err != nil {
		t.Fatalf("repeated Resolve: %v", err)
	}

	failed := runUncertain("kd-resolve-failed")
	call = nextCall()
	deadline = time.Now().Add(10 * time.Second)
	for {
		ri, err := k2.Get(ctx, call)
		if err == nil && ri.Status == "blocked" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("call did not stop for review: %+v %v", ri, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := k2.ResolveFailed(ctx, call, "not paid"); err != nil {
		t.Fatal(err)
	}
	if got := <-failed; got.err != nil || !strings.Contains(got.out, "not paid") {
		t.Fatalf("ResolveFailed: %q %v", got.out, got.err)
	}
	if _, err := k2.Submit(ctx, "tree-hold", nil, kairo.WithID("kd-tree-parent")); err != nil {
		t.Fatal(err)
	}
	if _, err := k2.Submit(ctx, "tree-hold", nil, kairo.WithID("kd-tree-child"), kairo.Parent("kd-tree-parent")); err != nil {
		t.Fatal(err)
	}
	children, err := k2.Children(ctx, "kd-tree-parent")
	if err != nil || len(children) != 1 || children[0].RunID != "kd-tree-child" || children[0].Parent != "kd-tree-parent" {
		t.Fatalf("remote children: %+v %v", children, err)
	}
	if _, err := k2.Submit(ctx, "tree-hold", nil, kairo.WithID("kd-orphan"), kairo.Parent("kd-missing")); !errors.Is(err, kairo.ErrParentUnavailable) {
		t.Fatalf("missing remote parent: %v", err)
	}
	if err := k2.Cancel(ctx, "kd-tree-parent"); err != nil {
		t.Fatal(err)
	}
	if child, err := k2.Get(ctx, "kd-tree-child"); err != nil || child.Status != "cancelled" {
		t.Fatalf("remote child after tree cancellation: %+v %v", child, err)
	}
	rows, err := k2.List(ctx, kairo.Filter{Workflow: "edit", Status: "completed"})
	if err != nil || len(rows) != 1 || rows[0].RunID != "kd-1" || rows[0].Workflow != "edit" || string(rows[0].Meta) != `{"owner":"alice"}` || rows[0].Created.IsZero() || rows[0].Updated.IsZero() {
		t.Fatalf("List on kairod: %+v %v", rows, err)
	}
	if page, err := k2.List(ctx, kairo.Filter{Workflow: "edit", After: "kd-1", Limit: 1}); err != nil || len(page) != 0 {
		t.Fatalf("List page on kairod: %+v %v", page, err)
	}
}

func TestDrainOnKairodWaitsForRunningAction(t *testing.T) {
	url, sock := startKairod(t)
	k, err := kairo.Connect(context.Background(), url, sock, kairo.Options{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	kairo.Action(k, "held", kairo.Real, func(_ *kairo.TaskContext, _ any) (string, error) {
		close(started)
		<-release
		return "done", nil
	})
	kairo.Workflow(k, "one", func(ctx *kairo.Context, _ any) (string, error) {
		return kairo.Call[string](ctx, "held", nil)
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Submit(context.Background(), "one", nil, kairo.WithID("remote-drain")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("action did not start")
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- k.Drain(ctx)
	}()
	select {
	case err := <-done:
		t.Fatalf("Drain returned before action result: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSuspendOnKairodWaitsForQuiescenceAndTicks(t *testing.T) {
	url, sock := startKairod(t)
	wakes := make(chan time.Time, 4)
	k, err := kairo.Connect(context.Background(), url, sock, kairo.Options{
		Mode: kairo.Suspend,
		Wake: func(at time.Time) error {
			wakes <- at
			return nil
		},
		Lease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var calls atomic.Int32
	kairo.Action(k, "upper", kairo.Unprotected, func(_ *kairo.TaskContext, in string) (string, error) {
		time.Sleep(40 * time.Millisecond)
		calls.Add(1)
		return strings.ToUpper(in), nil
	})
	kairo.Workflow(k, "delayed", func(ctx *kairo.Context, in string) (string, error) {
		out, err := kairo.Call[string](ctx, "upper", in)
		if err != nil {
			return "", err
		}
		if err := ctx.Sleep(120 * time.Millisecond); err != nil {
			return "", err
		}
		by, err := kairo.WaitFor[string](ctx, "approve")
		return out + ":" + by, err
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := kairo.Run[string](context.Background(), k, "delayed", "hi", kairo.WithID("remote-suspend")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("first run: %v", err)
	}
	if time.Since(started) < 35*time.Millisecond || calls.Load() != 1 {
		t.Fatalf("returned before the in-flight action settled: elapsed=%v calls=%d", time.Since(started), calls.Load())
	}
	var wake time.Time
	select {
	case wake = <-wakes:
	case <-time.After(time.Second):
		t.Fatal("Wake was not called")
	}
	if !wake.After(time.Now()) {
		t.Fatalf("wake is not the child timer deadline: %v", wake)
	}
	if next, ok, err := k.Tick(context.Background()); err != nil || !ok || !next.Equal(wake) {
		t.Fatalf("early tick: next=%v ok=%v err=%v wake=%v", next, ok, err, wake)
	}
	time.Sleep(time.Until(wake) + 40*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, _, err = k.Tick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, awaitErr := kairo.Await[string](context.Background(), k, "remote-suspend")
		if errors.Is(awaitErr, kairo.ErrSuspended) {
			break
		}
		if awaitErr != nil || time.Now().After(deadline) {
			t.Fatalf("await: %v", awaitErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := k.Signal(context.Background(), "remote-suspend", "approve", "alice"); err != nil {
		t.Fatal(err)
	}
	if out, err := kairo.Await[string](context.Background(), k, "remote-suspend"); err != nil || out != "HI:alice" {
		t.Fatalf("after signal: out=%q err=%v", out, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("action ran %d times", calls.Load())
	}
}
