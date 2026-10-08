package jev

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	jevapi "github.com/mattn/go-jev"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/httpaction"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/protocol"
	"github.com/r-hashi01/kairo/task"
)

// asked is a request to the fake Jev API.
type asked struct {
	State     any `json:"state"`
	Questions map[string]struct {
		Type         string          `json:"type"`
		Instructions any             `json:"instructions"`
		Criteria     json.RawMessage `json:"criteria"`
	} `json:"questions"`
}

// fakeJev answers with answer(request) -> (status, the answer of "q").
func fakeJev(t *testing.T, answer func(asked) (int, string)) (*Executor, *[]asked) {
	t.Helper()
	var mu sync.Mutex
	var seen []asked
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var a asked
		json.NewDecoder(r.Body).Decode(&a)
		mu.Lock()
		seen = append(seen, a)
		mu.Unlock()
		status, body := answer(a)
		w.WriteHeader(status)
		if status == 200 {
			body = `{"model":"m","answers":{"q":` + body + `},"usage":{"input_tokens":1,"output_tokens":1}}`
		}
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return New(jevapi.NewClient(jevapi.WithURL(srv.URL), jevapi.WithMaxRetries(0))), &seen
}

func run(x *Executor, action, params, input string) task.Result {
	tk := &task.Task{Action: action, Input: json.RawMessage(input)}
	if params != "" {
		tk.Params = json.RawMessage(params)
	}
	return x.Execute(context.Background(), tk, nil)
}

func TestKinds(t *testing.T) {
	x, seen := fakeJev(t, func(a asked) (int, string) {
		switch a.Questions["q"].Type {
		case "noul":
			return 200, `{"type":"noul","noul":0.92,"confidence":0.8}`
		case "score":
			return 200, `{"type":"score","score":7.5,"confidence":0.6}`
		default:
			return 200, `{"type":"choice","choice":"technical","probabilities":{"technical":0.7,"billing":0.3},"confidence":0.7}`
		}
	})
	for _, c := range []struct{ action, input, want string }{
		{"jev.noul", `{"state":"s","instructions":"urgent?"}`, `{"confidence":0.8,"yes":0.92}`},
		{"jev.score", `{"state":"s","instructions":"how urgent, 0-10?"}`, `{"confidence":0.6,"score":7.5}`},
		{"jev.choice.route", `{"state":"s","instructions":"which team?","criteria":["billing","technical"]}`,
			`{"choice":"technical","confidence":0.7,"probabilities":{"billing":0.3,"technical":0.7}}`},
	} {
		r := run(x, c.action, "", c.input)
		if r.Err != "" || string(r.Output) != c.want {
			t.Errorf("%s: %+v (output %s)", c.action, r, r.Output)
		}
	}
	if (*seen)[2].Questions["q"].Type != "choice" || (*seen)[0].State != "s" {
		t.Errorf("asked %+v", *seen)
	}
}

func TestQuestionFromParamsAndInput(t *testing.T) {
	x, seen := fakeJev(t, func(asked) (int, string) { return 200, `{"type":"noul","noul":0.1}` })
	// The params give the question; the input gives the state, and wins.
	run(x, "jev.noul", `{"instructions":"from params","state":"p"}`, `{"state":"from input"}`)
	run(x, "jev.noul", `{"instructions":"from params"}`, `{"state":"s","instructions":"from input"}`)
	got := *seen
	if got[0].State != "from input" || got[0].Questions["q"].Instructions != "from params" || got[1].Questions["q"].Instructions != "from input" {
		t.Fatalf("%+v", got)
	}
	if r := run(x, "jev.noul", "", `{"state":"s"}`); r.Err == "" || r.Retryable || r.Unknown {
		t.Fatalf("no instructions: %+v", r)
	}
	if r := run(x, "jev.other", "", `{"instructions":"q"}`); r.Err == "" {
		t.Fatalf("not a jev action: %+v", r)
	}
}

func TestChoiceOptions(t *testing.T) {
	choice := "b"
	x, seen := fakeJev(t, func(asked) (int, string) { return 200, `{"type":"choice","choice":"` + choice + `"}` })
	for _, criteria := range []string{`["a","b"]`, `[{"name":"a","desc":"A"},{"name":"b"}]`, `{"a":"A","b":null}`} {
		if r := run(x, "jev.choice", "", `{"instructions":"q","criteria":`+criteria+`}`); r.Err != "" {
			t.Errorf("%s: %+v", criteria, r)
		}
	}
	// The options go as an object, in their order.
	if c := string((*seen)[2].Questions["q"].Criteria); c != `{"a":"A","b":null}` {
		t.Errorf("criteria sent: %s", c)
	}
	choice = "c"
	if r := run(x, "jev.choice", "", `{"instructions":"q","criteria":["a","b"]}`); r.ErrType != "invalid_answer" || r.Retryable || r.Unknown {
		t.Errorf("an answer not among the options: %+v", r)
	}
	if r := run(x, "jev.choice", "", `{"instructions":"q"}`); r.Err == "" {
		t.Errorf("a choice without options: %+v", r)
	}
}

func TestFailures(t *testing.T) {
	status := 429
	x, _ := fakeJev(t, func(asked) (int, string) { return status, `{"error":"x"}` })
	if r := run(x, "jev.noul", "", `{"instructions":"q"}`); !r.Retryable || !r.RateLimited || r.Unknown {
		t.Errorf("429: %+v", r)
	}
	status = 422
	if r := run(x, "jev.noul", "", `{"instructions":"q"}`); r.Retryable || r.Unknown || r.ErrType != "http_422" {
		t.Errorf("422: %+v", r)
	}
	status = 500
	if r := run(x, "jev.noul", "", `{"instructions":"q"}`); !r.Unknown {
		t.Errorf("500: %+v", r)
	}
	gone := New(jevapi.NewClient(jevapi.WithURL("http://127.0.0.1:1"), jevapi.WithTimeout(2*time.Second)))
	if r := run(gone, "jev.noul", "", `{"instructions":"q"}`); !r.Unknown {
		t.Errorf("not connected: %+v", r)
	}
}

// kairod with kairo-jev as its worker: a plan branches on the decision.
func TestWorkerBranchesOnTheDecision(t *testing.T) {
	x, _ := fakeJev(t, func(a asked) (int, string) {
		team := "billing"
		if strings.Contains(a.State.(string), "outage") {
			team = "technical"
		}
		return 200, `{"type":"choice","choice":"` + team + `"}`
	})
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "jev.choice.route", Effect: ir.EffectUnprotected, Destination: Destination, Branch: "choice",
		Outputs: map[string]ir.FieldType{"choice": {Type: ir.FieldEnum, Values: []string{"billing", "technical"}}}})
	e, err := engine.New(engine.Config{Shards: 2, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(t.TempDir(), "w.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go (&protocol.Server{E: e}).Serve(l)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	d, err := ir.ParseDefinition([]byte(`{"name":"route","root":{"kind":"graph","id":"g",
	  "nodes":[
	    {"kind":"step","id":"cls","action":"jev.choice.route","input":{"state":"$input.text"},
	     "params":{"instructions":"Which team should handle this?","criteria":{"billing":"Payments","technical":"Bugs, outages"}}},
	    {"kind":"step","id":"b","action":"kairo.pass","input":{"by":"cls"}},
	    {"kind":"step","id":"t","action":"kairo.pass","input":{"by":"cls"}}],
	  "edges":[{"from":"cls","handle":"billing","to":"b"},{"from":"cls","handle":"technical","to":"t"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterPlan(d); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := &protocol.Worker{Name: "kairo-jev", Actions: []string{"jev.choice.route"}, Concurrency: 4, Handler: x.Execute}
	go wk.Run(ctx, "unix", sock)
	// The arm taken has an output; the other is skipped (null).
	for text, arm := range map[string]string{
		"My payouts failed":     "b",
		"The API has an outage": "t",
	} {
		in, _ := json.Marshal(map[string]string{"text": text})
		r, err := e.Submit(ctx, engine.SubmitRequest{Plan: "route", Tenant: "t", Input: in})
		if err != nil {
			t.Fatal(err)
		}
		wctx, wc := context.WithTimeout(ctx, 10*time.Second)
		ri, err := e.Wait(wctx, r.RunID)
		wc()
		var out map[string]json.RawMessage
		json.Unmarshal(ri.Output, &out)
		other := map[string]string{"b": "t", "t": "b"}[arm]
		if err != nil || len(out[arm]) == 0 || string(out[arm]) == "null" || (len(out[other]) > 0 && string(out[other]) != "null") {
			t.Errorf("%q: %v %s %s", text, err, ri.Status, ri.Output)
		}
	}
}

// The TypeScript SDK's embedded runtime calls the decision over HTTP
// (kairo-jev -http): needs node and go (for kairo.wasm).
func TestEmbeddedCallsOverHTTP(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Join(filepath.Dir(file), "..", "..")
	dir := t.TempDir()
	wasm := os.Getenv("KAIRO_WASM")
	if wasm == "" {
		wasm = filepath.Join(dir, "kairo.wasm")
		b := exec.Command("go", "build", "-buildmode=c-shared", "-o", wasm, "./cmd/kairo-wasm")
		b.Dir, b.Env = repo, append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
		if out, err := b.CombinedOutput(); err != nil {
			t.Fatalf("building kairo.wasm: %v\n%s", err, out)
		}
	}
	x, _ := fakeJev(t, func(asked) (int, string) { return 200, `{"type":"noul","noul":0.95,"confidence":0.9}` })
	srv := httptest.NewServer(httpaction.Handler("s3cret", x.Execute))
	defer srv.Close()

	sdk := filepath.Join(repo, "sdk", "ts", "src")
	script := `import { EmbeddedBackend } from ` + q(filepath.Join(sdk, "backend.ts")) + `;
import { SQLiteStore } from ` + q(filepath.Join(sdk, "store.ts")) + `;
import { Kairo } from ` + q(filepath.Join(sdk, "workflow.ts")) + `;
const [wasm, db, url] = process.argv.slice(2);
const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore(db), wasm }), secret: 's3cret', callbackUrl: 'http://127.0.0.1:1/cb' });
k.defineAction('jev.noul', { effect: 'unprotected', url, handler: async () => null });
k.workflow('triage', async (ctx, text) => {
	const d = await ctx.call('jev.noul', { state: text, instructions: 'Is this urgent?' });
	return d.yes > 0.5 ? 'page someone' : 'later';
});
await k.start();
process.stdout.write(JSON.stringify(await k.run('triage', 'payouts failing for 3 days', { id: 't-1' })));
await k.close();`
	f := filepath.Join(dir, "triage.ts")
	if err := os.WriteFile(f, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, "--no-warnings", f, wasm, filepath.Join(dir, "t.db"), srv.URL+"/kairo/action")
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("node: %v\n%s", err, stderr)
	}
	if string(out) != `"page someone"` {
		t.Fatalf("got %s", out)
	}
}

func q(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
