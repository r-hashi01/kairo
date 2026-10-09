package storetest

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// A workflow begun by one SDK is finished by another, on one SQLite file
// (ADR 0058): the call ids, the plans and the tables are the same, so the
// calls that finished are not run again, and the real one runs once.

type editOut struct {
	Answers []string `json:"answers"`
	Wrote   string   `json:"wrote"`
	Again   string   `json:"again"`
}

type editRuns struct {
	mu          sync.Mutex
	llm, writes int
}

// openEdit opens the Go side of testdata/edit.ts's workflow.
func openEdit(t *testing.T, db string, hang bool, runs *editRuns) *kairo.Kairo {
	t.Helper()
	k, err := kairo.Open(context.Background(), kairo.Options{Store: openSQLite(t, db)})
	if err != nil {
		t.Fatal(err)
	}
	kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) {
		runs.mu.Lock()
		runs.llm++
		runs.mu.Unlock()
		return strings.ToUpper(q), nil
	})
	kairo.Action(k, "write", kairo.Real, func(_ *kairo.TaskContext, p string) (string, error) {
		runs.mu.Lock()
		runs.writes++
		runs.mu.Unlock()
		return "wrote " + p, nil
	})
	kairo.Workflow(k, "edit", func(ctx *kairo.Context, files []string) (editOut, error) {
		a, err := kairo.Call[string](ctx, "llm", files[0])
		if err != nil {
			return editOut{}, err
		}
		b, err := kairo.Call[string](ctx, "llm", files[1])
		if err != nil {
			return editOut{}, err
		}
		wrote, err := kairo.Call[string](ctx, "write", files[0])
		if err != nil {
			return editOut{}, err
		}
		if hang {
			<-ctx.Done() // as a process that stops here
			return editOut{}, ctx.Err()
		}
		again, err := kairo.Call[string](ctx, "llm", "done")
		return editOut{Answers: []string{a, b}, Wrote: wrote, Again: again}, err
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return k
}

// node runs testdata/edit.ts; skips the test without node or go.
func node(t *testing.T, args ...string) []byte {
	t.Helper()
	nodeBin, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Join(filepath.Dir(file), "..", "..", "..")
	wasm := os.Getenv("KAIRO_WASM")
	if wasm == "" {
		wasm = filepath.Join(t.TempDir(), "kairo.wasm")
		b := exec.Command("go", "build", "-buildmode=c-shared", "-o", wasm, "./cmd/kairo-wasm")
		b.Dir, b.Env = repo, append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
		if out, err := b.CombinedOutput(); err != nil {
			t.Fatalf("building kairo.wasm: %v\n%s", err, out)
		}
	}
	cmd := exec.Command(nodeBin, append([]string{"--no-warnings", filepath.Join(filepath.Dir(file), "testdata", "edit.ts"),
		filepath.Join(repo, "sdk", "ts", "src"), wasm}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = ee.Stderr
		}
		t.Fatalf("node: %v\n%s", err, stderr)
	}
	return out
}

func TestGoBeginsTypeScriptFinishes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "x.db")
	runs := &editRuns{}
	k := openEdit(t, db, true, runs)
	go kairo.Run[editOut](context.Background(), k, "edit", []string{"a", "b"}, kairo.WithID("x-1"))
	deadline := time.Now().Add(10 * time.Second)
	for {
		runs.mu.Lock()
		done := runs.writes == 1
		runs.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the Go side did not write")
		}
		time.Sleep(5 * time.Millisecond)
	}
	k.Close()

	var res struct {
		Runs struct{ LLM, Write int } `json:"runs"`
		Out  editOut                  `json:"out"`
	}
	if err := json.Unmarshal(node(t, db, "finish", "x-1"), &res); err != nil {
		t.Fatal(err)
	}
	want := editOut{Answers: []string{"A", "B"}, Wrote: "wrote a", Again: "DONE"}
	if !sameJSON(res.Out, want) {
		t.Fatalf("TypeScript finished with %+v", res.Out)
	}
	if res.Runs.Write != 0 || res.Runs.LLM != 1 {
		t.Fatalf("TypeScript ran calls again: %+v", res.Runs)
	}
}

func TestTypeScriptBeginsGoFinishes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "x.db")
	var started struct {
		Runs struct{ LLM, Write int } `json:"runs"`
	}
	if err := json.Unmarshal(node(t, db, "start", "x-2"), &started); err != nil {
		t.Fatal(err)
	}
	if started.Runs.Write != 1 || started.Runs.LLM != 2 {
		t.Fatalf("the TypeScript side ran %+v", started.Runs)
	}
	runs := &editRuns{}
	k := openEdit(t, db, false, runs)
	defer k.Close()
	out, err := kairo.Run[editOut](context.Background(), k, "edit", []string{"a", "b"}, kairo.WithID("x-2"))
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(out, editOut{Answers: []string{"A", "B"}, Wrote: "wrote a", Again: "DONE"}) {
		t.Fatalf("Go finished with %+v", out)
	}
	if runs.writes != 0 || runs.llm != 1 {
		t.Fatalf("Go ran calls again: llm %d, write %d", runs.llm, runs.writes)
	}
}

func sameJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
