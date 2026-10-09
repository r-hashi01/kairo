package kairo

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// Call ids are the TypeScript SDK's: sdk/testdata/callids.jsonl (shared
// with the Python SDK's tests) was made by its
// canonical and callId (sdk/ts/src/workflow.ts), on inputs at the edges
// (key order with astral characters, <>&, U+2028, -0, 1e21, nesting).
func TestCallIDsMatchTypeScript(t *testing.T) {
	f, err := os.Open("../testdata/callids.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var c struct {
			Input     any    `json:"input"`
			Canonical string `json:"canonical"`
			ID        string `json:"id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		canon, err := canonical(c.Input)
		if err != nil {
			t.Fatal(err)
		}
		if canon != c.Canonical {
			t.Errorf("canonical(%v) = %s, TypeScript: %s", c.Input, canon, c.Canonical)
		}
		if id := callID("wf-1", "kairo.call/llm", canon, 2); id != c.ID {
			t.Errorf("callID(%v) = %s, TypeScript: %s", c.Input, id, c.ID)
		}
		n++
	}
	if n < 19 {
		t.Fatalf("%d cases", n)
	}
}

func TestKeepFinishedSetting(t *testing.T) {
	for _, c := range []struct {
		opt  time.Duration
		env  string
		want time.Duration
	}{{0, "", 24 * time.Hour}, {0, "7d", 7 * 24 * time.Hour}, {0, "30m", 30 * time.Minute}, {0, "forever", Forever}, {time.Second, "7d", time.Second}} {
		if got, err := keepFinished(c.opt, c.env); err != nil || got != c.want {
			t.Errorf("%v %q: %v %v", c.opt, c.env, got, err)
		}
	}
	if _, err := keepFinished(0, "a week"); err == nil {
		t.Error("a bad value was taken")
	}
}
