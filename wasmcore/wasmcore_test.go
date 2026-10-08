package wasmcore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/ir"
)

const specs = `[{"action":"llm","effect":"unprotected"},{"action":"send","effect":"real"}]`

// A workflow that takes the paths the tests need: a map, a real step, a
// signal and a step whose result waits (ADR 0045).
const definition = `{"name":"w","root":{"kind":"seq","nodes":[
  {"kind":"map","id":"m","over":"$input.xs","body":{"kind":"step","id":"sq","action":"llm","input":{"x":"$item"}}},
  {"kind":"step","id":"pause","action":"llm"},
  {"kind":"wait","id":"ok","signal":"approve"},
  {"kind":"step","id":"send","action":"send","input":{"by":"ok.payload"}}]}}`

// script drives a run as a host does: it keeps the commands outstanding
// and answers them one at a time (a dispatch with a result, a real one
// after its intent, the paused step with a wait, a timer when it is due),
// and sends the signal once nothing else is outstanding. It returns the
// events applied, in order.
func script(t *testing.T, apply func(ev Event) Result) []Event {
	t.Helper()
	var log []Event
	at := int64(1000)
	var pending []Command
	step := func(ev Event) Result {
		ev.At = at
		at++
		log = append(log, ev)
		r := apply(ev)
		for _, c := range r.Commands {
			switch c.Kind {
			case "dispatch", "timer":
				pending = append(pending, c)
			case "cancel_timer":
				for i, p := range pending {
					if p.Kind == "timer" && p.Timer == c.Timer {
						pending = append(pending[:i], pending[i+1:]...)
						break
					}
				}
			}
		}
		return r
	}
	r := step(Event{Kind: "start", Data: json.RawMessage(`{"xs":[1,2,3]}`)})
	signalled := false
	for i := 0; i < 100 && r.Status == "running"; i++ {
		if len(pending) == 0 {
			if signalled {
				t.Fatalf("stuck: %+v", r)
			}
			signalled = true
			r = step(Event{Kind: "signal", Name: "approve", Data: json.RawMessage(`"alice"`)})
			continue
		}
		c := pending[0]
		pending = pending[1:]
		switch {
		case c.Kind == "timer":
			if c.At > at {
				at = c.At
			}
			r = step(Event{Kind: "timer", Act: c.Act, Timer: c.Timer})
		case c.Node == "pause" && c.Attempt == 1 && c.Kind == "dispatch":
			r = step(Event{Kind: "step_wait", Act: c.Act, Attempt: c.Attempt, Deadline: at + 500, Data: json.RawMessage(`"woke"`)})
		case c.Effect == "real":
			step(Event{Kind: "intent", Act: c.Act, Attempt: c.Attempt})
			r = step(Event{Kind: "step_ok", Act: c.Act, Attempt: c.Attempt, Data: json.RawMessage(`{"sent":true}`)})
		default:
			r = step(Event{Kind: "step_ok", Act: c.Act, Attempt: c.Attempt, Data: json.RawMessage(`{"y":1}`)})
		}
	}
	if r.Status != "completed" {
		t.Fatalf("run %s: %s", r.Status, r.Error)
	}
	seen := map[string]bool{}
	for _, e := range log {
		seen[e.Kind] = true
	}
	for _, k := range []string{"start", "step_ok", "step_wait", "timer", "signal", "intent"} {
		if !seen[k] {
			t.Fatalf("the run took no %s event", k)
		}
	}
	return log
}

func newCore(t *testing.T) (*Core, Compiled) {
	t.Helper()
	c := New()
	if err := c.Register([]byte(specs)); err != nil {
		t.Fatal(err)
	}
	cp, err := c.Compile([]byte(definition))
	if err != nil {
		t.Fatal(err)
	}
	return c, cp
}

// The core through wasmcore makes the same states as the core called
// directly, byte for byte, and a stale event is reported, not applied.
func TestApplyMatchesTheCore(t *testing.T) {
	c, cp := newCore(t)
	var state []byte
	var states [][]byte
	log := script(t, func(ev Event) Result {
		b, _ := json.Marshal(ev)
		s, r, err := c.Apply(cp.Plan, "run1", state, b, true)
		if err != nil {
			t.Fatal(err)
		}
		state = s
		states = append(states, s)
		return r
	})
	reg := ir.NewRegistry()
	var ss []ir.NodeSpec
	json.Unmarshal([]byte(specs), &ss)
	for _, s := range ss {
		reg.Register(s)
	}
	def, _ := ir.ParseDefinition([]byte(definition))
	p, _ := ir.Compile(def, reg)
	s := core.NewState("run1")
	for i, e := range log {
		ev := core.Event{Kind: eventKinds[e.Kind], At: e.At, Act: e.Act, Attempt: e.Attempt, Timer: e.Timer, Name: e.Name,
			Data: e.Data, Deadline: e.Deadline}
		if _, err := core.Apply(p, s, &ev, nil); err != nil {
			t.Fatalf("event %d (%s): %v", i, e.Kind, err)
		}
		if !bytes.Equal(s.Encode(nil), states[i]) {
			t.Fatalf("event %d (%s): states differ", i, e.Kind)
		}
	}
	// The run is over: an event now is ignored, the state unchanged.
	b, _ := json.Marshal(Event{Kind: "signal", Name: "approve", At: 9999})
	again, r, err := c.Apply(cp.Plan, "run1", state, b, false)
	if err != nil || !r.Ignored || !bytes.Equal(again, state) {
		t.Fatalf("after the end: %+v %v", r, err)
	}
}

func TestErrors(t *testing.T) {
	c, cp := newCore(t)
	if _, err := c.Compile([]byte(`{"name":"x","root":{"kind":"step","id":"a","action":"llm","input":{"q":"nope.x"}}}`)); err == nil {
		t.Fatal("a bad reference compiled")
	}
	if _, _, err := c.Apply(cp.Plan+1, "r", nil, []byte(`{"kind":"start"}`), false); err == nil {
		t.Fatal("no such plan")
	}
	if _, _, err := c.Apply(cp.Plan, "r", nil, []byte(`{"kind":"what"}`), false); err == nil {
		t.Fatal("unknown event kind")
	}
	if _, _, err := c.Apply(cp.Plan, "r", []byte("garbage"), []byte(`{"kind":"start"}`), false); err == nil {
		t.Fatal("a corrupt state")
	}
}

// The WASM module (cmd/kairo-wasm), run by Node, makes the same states as
// the native core, byte for byte (ADR 0051). Needs node.
func TestWASMMatchesNative(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a WASM module")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	dir := t.TempDir()
	wasm := filepath.Join(dir, "kairo.wasm")
	build := exec.Command("go", "build", "-buildmode=c-shared", "-o", wasm, "./cmd/kairo-wasm")
	build.Dir = ".."
	build.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the WASM module: %v\n%s", err, out)
	}
	// The native run: its events and states.
	c, cp := newCore(t)
	var state []byte
	var states []string
	log := script(t, func(ev Event) Result {
		b, _ := json.Marshal(ev)
		s, r, err := c.Apply(cp.Plan, "run1", state, b, false)
		if err != nil {
			t.Fatal(err)
		}
		state = s
		states = append(states, hex.EncodeToString(s))
		return r
	})
	events, _ := json.Marshal(log)
	js := filepath.Join(dir, "run.mjs")
	os.WriteFile(js, []byte(nodeScript), 0o644)
	cmd := exec.Command(node, "--no-warnings", js, wasm, specs, definition, string(events))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	got := strings.Fields(strings.TrimSpace(string(out)))
	if len(got) != len(states) {
		t.Fatalf("WASM made %d states, native %d\n%s", len(got), len(states), out)
	}
	for i := range got {
		if got[i] != states[i] {
			t.Fatalf("event %d (%s): WASM and native states differ", i, log[i].Kind)
		}
	}
}

// nodeScript applies events with the WASM module and prints each state in
// hex: node run.mjs <wasm> <specs> <definition> <events>.
const nodeScript = `
import { readFileSync } from 'node:fs';
import { WASI } from 'node:wasi';
const [wasmPath, specs, definition, events] = process.argv.slice(2);
const wasi = new WASI({ version: 'preview1' });
const inst = await WebAssembly.instantiate(await WebAssembly.compile(readFileSync(wasmPath)), wasi.getImportObject());
wasi.initialize(inst);
const x = inst.exports;
const enc = new TextEncoder(), dec = new TextDecoder();
const put = (b) => { const p = x.kairo_alloc(b.length); new Uint8Array(x.memory.buffer, p, b.length).set(b); return [p, b.length]; };
const res = (n) => {
  const b = new Uint8Array(x.memory.buffer, x.kairo_result(), Math.abs(n)).slice();
  if (n < 0) throw new Error(dec.decode(b));
  return b;
};
res(x.kairo_register(...put(enc.encode(specs))));
const plan = JSON.parse(dec.decode(res(x.kairo_compile(...put(enc.encode(definition)))))).plan;
let state = new Uint8Array();
const [rp, rn] = put(enc.encode('run1'));
const lines = [];
for (const ev of JSON.parse(events)) {
  const [sp, sn] = state.length ? put(state) : [0, 0];
  const [ep, en] = put(enc.encode(JSON.stringify(ev)));
  const b = res(x.kairo_apply(plan, rp, rn, sp, sn, ep, en, 1));
  if (sp) x.kairo_free(sp);
  x.kairo_free(ep);
  const n = new DataView(b.buffer).getUint32(0, true);
  state = b.subarray(4, 4 + n);
  JSON.parse(dec.decode(b.subarray(4 + n)));
  lines.push(Buffer.from(state).toString('hex'));
}
console.log(lines.join('\n'));
`
