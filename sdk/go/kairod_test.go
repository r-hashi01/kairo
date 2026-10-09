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
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	ctx := context.Background()

	k1 := connect("1")
	if _, err := k1.Submit(ctx, "edit", "a", kairo.WithID("kd-1")); err != nil {
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
	if _, err := k2.List(ctx, kairo.Filter{}); !errors.Is(err, kairo.ErrNeedsEmbedded) {
		t.Fatalf("List on kairod: %v", err)
	}
}
