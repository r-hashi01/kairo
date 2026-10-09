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
// (ADR 0058), Go with TypeScript and with Python: the call ids, the plans
// and the tables are the same, so the calls that finished are not run
// again, and the real one runs once.

type editOut struct {
	Answers []string `json:"answers"`
	Wrote   string   `json:"wrote"`
	Again   string   `json:"again"`
}

type editRuns struct {
	mu          sync.Mutex
	llm, writes int
}

// ask is llm's input: a number with no fraction (Python writes 1.0 as
// such), keys whose order differs by code point and by UTF-16 unit. Every
// SDK must give it the same canonical text, so the same call ids.
func ask(q string) map[string]any {
	return map[string]any{"q": q, "temperature": 1.0, "😀": 1, "｡": 2}
}

// openEdit opens the Go side of testdata/edit.ts's workflow.
func openEdit(t *testing.T, db string, hang bool, runs *editRuns) *kairo.Kairo {
	t.Helper()
	k, err := kairo.Open(context.Background(), kairo.Options{Store: openSQLite(t, db)})
	if err != nil {
		t.Fatal(err)
	}
	kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, p struct{ Q string }) (string, error) {
		runs.mu.Lock()
		runs.llm++
		runs.mu.Unlock()
		return strings.ToUpper(p.Q), nil
	})
	kairo.Action(k, "write", kairo.Real, func(_ *kairo.TaskContext, p string) (string, error) {
		runs.mu.Lock()
		runs.writes++
		runs.mu.Unlock()
		return "wrote " + p, nil
	})
	kairo.Workflow(k, "edit", func(ctx *kairo.Context, files []string) (editOut, error) {
		a, err := kairo.Call[string](ctx, "llm", ask(files[0]))
		if err != nil {
			return editOut{}, err
		}
		b, err := kairo.Call[string](ctx, "llm", ask(files[1]))
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
		again, err := kairo.Call[string](ctx, "llm", ask("done"))
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
	wasm := wasmFile(t, repo)
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

// wasmFile is kairo.wasm: KAIRO_WASM, or built from this repository.
func wasmFile(t *testing.T, repo string) string {
	t.Helper()
	if w := os.Getenv("KAIRO_WASM"); w != "" {
		return w
	}
	w := filepath.Join(t.TempDir(), "kairo.wasm")
	b := exec.Command("go", "build", "-buildmode=c-shared", "-o", w, "./cmd/kairo-wasm")
	b.Dir, b.Env = repo, append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if out, err := b.CombinedOutput(); err != nil {
		t.Fatalf("building kairo.wasm: %v\n%s", err, out)
	}
	return w
}

// python runs testdata/edit.py with KAIRO_PYTHON (default python3); skips
// the test without it or without wasmtime.
func python(t *testing.T, args ...string) []byte {
	t.Helper()
	bin := os.Getenv("KAIRO_PYTHON")
	if bin == "" {
		bin = "python3"
	}
	if exec.Command(bin, "-c", "import wasmtime").Run() != nil {
		t.Skip(bin + " with wasmtime not found (KAIRO_PYTHON)")
	}
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Join(filepath.Dir(file), "..", "..", "..")
	cmd := exec.Command(bin, append([]string{filepath.Join(filepath.Dir(file), "testdata", "edit.py"),
		filepath.Join(repo, "sdk", "python"), wasmFile(t, repo)}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = ee.Stderr
		}
		t.Fatalf("python: %v\n%s", err, stderr)
	}
	return out
}

// other runs the workflow's other side: TypeScript or Python.
func other(t *testing.T, lang string, args ...string) []byte {
	if lang == "python" {
		return python(t, args...)
	}
	return node(t, args...)
}

func TestGoBeginsTypeScriptFinishes(t *testing.T) { goBegins(t, "typescript") }
func TestTypeScriptBeginsGoFinishes(t *testing.T) { goFinishes(t, "typescript") }
func TestGoBeginsPythonFinishes(t *testing.T)     { goBegins(t, "python") }
func TestPythonBeginsGoFinishes(t *testing.T)     { goFinishes(t, "python") }

func goBegins(t *testing.T, lang string) {
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
	if err := json.Unmarshal(other(t, lang, db, "finish", "x-1"), &res); err != nil {
		t.Fatal(err)
	}
	want := editOut{Answers: []string{"A", "B"}, Wrote: "wrote a", Again: "DONE"}
	if !sameJSON(res.Out, want) {
		t.Fatalf("%s finished with %+v", lang, res.Out)
	}
	if res.Runs.Write != 0 || res.Runs.LLM != 1 {
		t.Fatalf("%s ran calls again: %+v", lang, res.Runs)
	}
}

func goFinishes(t *testing.T, lang string) {
	db := filepath.Join(t.TempDir(), "x.db")
	var started struct {
		Runs struct{ LLM, Write int } `json:"runs"`
	}
	if err := json.Unmarshal(other(t, lang, db, "start", "x-2"), &started); err != nil {
		t.Fatal(err)
	}
	if started.Runs.Write != 1 || started.Runs.LLM != 2 {
		t.Fatalf("the %s side ran %+v", lang, started.Runs)
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
