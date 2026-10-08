package dify

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/ir"
)

// Environment variables' values come with each run, never in the plan:
// a secret must not be stored with it (ADR 0037).
func TestEnvironmentValuesAreNotInThePlan(t *testing.T) {
	wf := `{"graph":{"nodes":[
	  {"id":"start","data":{"type":"start","title":"Start","variables":[]}},
	  {"id":"ans","data":{"type":"answer","title":"A","answer":"key={{#env.api_key#}}"}}],
	  "edges":[{"source":"start","target":"ans"}]},
	  "environment_variables":[{"name":"api_key","value_type":"secret","value":"s3cret"}]}`
	var w Workflow
	if err := json.Unmarshal([]byte(wf), &w); err != nil {
		t.Fatal(err)
	}
	conv, err := Convert("env", &w)
	if err != nil {
		t.Fatal(err)
	}
	def, _ := json.Marshal(conv.Definition)
	if strings.Contains(string(def), "s3cret") {
		t.Fatalf("the secret is in the plan: %s", def)
	}
	p, err := ir.Compile(conv.Definition, ir.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(map[string]any{"sys": map[string]any{}, EnvInput: EnvValues(&w)})
	s, _ := run(t, p, input, mockCase{})
	if s.Status != core.StatusCompleted || !strings.Contains(string(s.Output), "key=s3cret") {
		t.Fatalf("%v %s %s", s.Status, s.Error, s.Output)
	}
}
