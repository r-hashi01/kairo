package n8n

import (
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/engine"
	"kairo/ir"
	"kairo/protocol"
)

// The TypeScript worker SDK (sdk/ts) against the runtime's protocol
// server: a graph's n8n nodes run in a Node.js worker.
func TestTypeScriptWorker(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	reg := ir.NewRegistry()
	Spec(reg)
	dir := t.TempDir()
	e, err := engine.New(engine.Config{Shards: 1, Registry: reg, DataDir: dir, DefaultTier: engine.TierFile, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "w.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go (&protocol.Server{E: e, Token: "tok"}).Serve(ln)
	cmd := exec.Command(node, "testdata/worker.ts", sock, "tok")
	out := &logBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()

	var g Graph
	json.Unmarshal([]byte(diamondGraph), &g)
	def, err := Convert("wf", &g)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterPlan(def); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := e.Submit(ctx, engine.SubmitRequest{Plan: "wf", Tenant: "t", Input: json.RawMessage(`{"trigger":[[{"json":{}}]]}`)})
	if err != nil {
		t.Fatal(err)
	}
	ri, err := e.Wait(ctx, res.RunID)
	if err != nil {
		t.Fatalf("%v (worker: %s)", err, out)
	}
	if ri.Status != "completed" {
		t.Fatalf("%s %s (worker: %s)", ri.Status, ri.Error, out)
	}
	// The worker asked for run ends: it hears of this one (ADR 0044).
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(out.String(), "run-end "+res.RunID) {
		if time.Now().After(deadline) {
			t.Fatalf("no run end (worker: %s)", out)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type logBuf struct {
	mu sync.Mutex
	b  []byte
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b = append(l.b, p...)
	return len(p), nil
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.b)
}
