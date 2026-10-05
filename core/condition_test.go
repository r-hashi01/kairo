package core

import (
	"encoding/json"
	"strings"
	"testing"

	"kairo/ir"
)

// --- ADR 0031: kairo.switch ------------------------------------------------

// runSwitch evaluates one case of conditions over the run input and
// returns the chosen handle, or the error of a failed evaluation.
func runSwitch(t *testing.T, input, logic string, conds ...string) (string, string) {
	t.Helper()
	in := map[string]string{}
	var vars map[string]json.RawMessage
	json.Unmarshal([]byte(input), &vars)
	for k := range vars {
		in[k] = "$input." + k
	}
	inJSON, _ := json.Marshal(in)
	js := `{"name":"sw","root":{"kind":"step","id":"s","action":"kairo.switch","input":` + string(inJSON) +
		`,"params":{"cases":[{"id":"yes","logical_operator":"` + logic + `","conditions":[` + strings.Join(conds, ",") + `]}]}}}`
	d, err := ir.ParseDefinition([]byte(js))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ir.Compile(d, ir.NewRegistry())
	if err != nil {
		t.Fatalf("%s: %v", js, err)
	}
	x := newSim(t, p)
	x.run(input)
	x.checkReplay()
	if x.s.Status != StatusCompleted {
		return "", x.s.Error
	}
	var out struct{ Handle string }
	json.Unmarshal(x.s.Output, &out)
	return out.Handle, ""
}

func cond(v, op string, value ...string) string {
	c := `{"var":"` + v + `","operator":"` + op + `"`
	if len(value) > 0 {
		c += `,"value":` + value[0]
	}
	return c + "}"
}

func TestSwitchConditions(t *testing.T) {
	for _, tc := range []struct {
		input, logic string
		conds        []string
		want, err    string
	}{
		// From graphon's tests (tests/utils/test_condition_processor.py).
		{`{"zone":0}`, "or", []string{cond("zone", "≤", `"0.95"`)}, "yes", ""},
		{`{"one":1}`, "or", []string{cond("one", "≥", `"0.95"`)}, "yes", ""},
		{`{"x":1.1}`, "or", []string{cond("x", "≥", `"0.95"`)}, "yes", ""},
		{`{"x":1.1}`, "or", []string{cond("x", ">", `"0"`)}, "yes", ""},
		{`{"enabled":false}`, "and", []string{cond("enabled", "is", `"false"`)}, "yes", ""},
		{`{"text":"graphon"}`, "and", []string{cond("text", "contains", `"pho"`)}, "yes", ""},
		{`{"tags":["a","b"]}`, "and", []string{cond("tags", "contains", `"a"`)}, "yes", ""},
		{`{"text":"graphon","needle":"pho"}`, "and", []string{cond("text", "contains", `"{{#needle#}}"`)}, "yes", ""},
		// Strings.
		{`{"s":"Hello"}`, "and", []string{cond("s", "start with", `"He"`), cond("s", "end with", `"lo"`)}, "yes", ""},
		{`{"s":""}`, "and", []string{cond("s", "start with", `"He"`)}, "false", ""},
		{`{"s":"x"}`, "and", []string{cond("s", "not contains", `"y"`)}, "yes", ""},
		{`{"s":""}`, "and", []string{cond("s", "not contains", `"y"`)}, "yes", ""},
		{`{"s":"a"}`, "and", []string{cond("s", "is", `"a"`), cond("s", "is not", `"b"`)}, "yes", ""},
		{`{"s":null}`, "or", []string{cond("s", "is", `"a"`), cond("s", "is not", `"a"`)}, "false", ""},
		{`{"s":"True"}`, "and", []string{cond("s", "contains", `true`)}, "yes", ""}, // str(True)
		{`{"n":3}`, "and", []string{cond("n", "is", `"3"`)}, "", "string or boolean"},
		// Emptiness and existence.
		{`{"s":"","l":[],"o":{},"z":0}`, "and", []string{cond("s", "empty"), cond("l", "empty"), cond("o", "empty"), cond("z", "empty")}, "yes", ""},
		{`{"s":null}`, "and", []string{cond("s", "null"), cond("s", "not exists")}, "yes", ""},
		{`{"s":"x"}`, "and", []string{cond("s", "not null"), cond("s", "exists"), cond("s", "not empty")}, "yes", ""},
		// Numbers: int(expected) truncates for int values, floats compare.
		{`{"n":3}`, "and", []string{cond("n", "=", `"3"`)}, "yes", ""},
		{`{"n":3}`, "and", []string{cond("n", "=", `"3.5"`)}, "", "invalid literal for int()"},
		{`{"f":3.5}`, "and", []string{cond("f", "=", `"3.5"`), cond("f", "≠", `"3"`)}, "yes", ""},
		{`{"n":2}`, "and", []string{cond("n", "<", `"2.5"`), cond("n", "≤", `"2"`)}, "yes", ""},
		{`{"n":"2"}`, "and", []string{cond("n", ">", `"1"`)}, "", "Invalid actual value type: number"},
		{`{"n":"2"}`, "and", []string{cond("n", "=", `"2"`)}, "", "number or boolean"},
		{`{"n":null}`, "or", []string{cond("n", ">", `"1"`), cond("n", "=", `"1"`)}, "false", ""},
		{`{"b":true}`, "and", []string{cond("b", "=", `"anything"`)}, "", "invalid JSON"}, // bool expected must parse
		{`{"b":true}`, "and", []string{cond("b", "=", `"1"`)}, "yes", ""},
		// Lists.
		{`{"s":"b"}`, "and", []string{cond("s", "in", `["a","b"]`)}, "yes", ""},
		{`{"s":""}`, "and", []string{cond("s", "not in", `["a"]`)}, "yes", ""},
		{`{"s":"b"}`, "and", []string{cond("s", "in", `"b"`)}, "", "expected value type: array"},
		{`{"l":["a","b","c"]}`, "and", []string{cond("l", "all of", `["a","c"]`)}, "yes", ""},
		{`{"l":["a"]}`, "and", []string{cond("l", "all of", `["a","c"]`)}, "false", ""},
		{`{"s":"abc"}`, "and", []string{cond("s", "all of", `["a","bc"]`)}, "yes", ""},
		{`{"l":[true,false]}`, "and", []string{cond("l", "all of", `["true"]`)}, "yes", ""},
		{`{"l":[1,2]}`, "and", []string{cond("l", "contains", `"1"`)}, "false", ""}, // "1" != 1
		// Short circuit: the second condition's error is not raised.
		{`{"s":"a","n":"x"}`, "or", []string{cond("s", "is", `"a"`), cond("n", ">", `"1"`)}, "yes", ""},
		{`{"s":"a","n":"x"}`, "and", []string{cond("s", "is", `"b"`), cond("n", ">", `"1"`)}, "false", ""},
	} {
		got, err := runSwitch(t, tc.input, tc.logic, tc.conds...)
		if tc.err != "" {
			if !strings.Contains(err, tc.err) {
				t.Errorf("%s %v: error %q, want %q", tc.input, tc.conds, err, tc.err)
			}
			continue
		}
		if err != "" || got != tc.want {
			t.Errorf("%s %v: got %q (%s), want %q", tc.input, tc.conds, got, err, tc.want)
		}
	}
}

// A missing variable (a node that was skipped) fails the condition, as in
// graphon; a present null does not.
func TestSwitchMissingVariable(t *testing.T) {
	p := compileGraph(t, `{"name":"m","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"cls","action":"classify"},
	    {"kind":"step","id":"r","action":"llm"},
	    {"kind":"step","id":"sw","action":"kairo.switch","input":{"v":"r"},
	     "params":{"cases":[{"id":"c","logical_operator":"and","conditions":[{"var":"v","operator":"null"}]}]}}],
	  "edges":[{"from":"cls","handle":"refund","to":"r"},{"from":"cls","handle":"other","to":"sw"},{"from":"r","to":"sw"}]}}`)
	x := newSim(t, p)
	x.handler = label("other")
	x.run(`{}`)
	if x.s.Status != StatusFailed || !strings.Contains(x.s.Error, "not found") {
		t.Fatalf("%v %s", x.s.Status, x.s.Error)
	}
}

// File arrays are tested by attribute.
func TestSwitchFileConditions(t *testing.T) {
	files := `{"fs":[{"dify_model_identity":"__dify__file__","filename":"a.pdf","extension":"pdf","size":10},
	                 {"dify_model_identity":"__dify__file__","filename":"b.png","extension":".png","size":20}]}`
	sub := func(logic string, subs ...string) string {
		return `{"var":"fs","operator":"contains","sub":{"logical_operator":"` + logic + `","conditions":[` + strings.Join(subs, ",") + `]}}`
	}
	for _, tc := range []struct {
		cond, want string
	}{
		{sub("and", `{"key":"extension","operator":"is","value":"pdf"}`), "yes"},
		{sub("and", `{"key":"extension","operator":"is","value":".doc"}`), "false"},
		{sub("and", `{"key":"name","operator":"start with","value":"b"}`), "yes"},
		{sub("and", `{"key":"extension","operator":"is not","value":"pdf"}`), "false"}, // "not": every file must hold
	} {
		got, err := runSwitch(t, files, "and", tc.cond)
		if err != "" || got != tc.want {
			t.Errorf("%s: %q %s, want %q", tc.cond, got, err, tc.want)
		}
	}
}

// Cases are tried in order; the step's handles are its case ids and
// "false", and on_error applies to an evaluation error.
func TestSwitchCasesAndErrors(t *testing.T) {
	p := compileGraph(t, `{"name":"c","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"sw","action":"kairo.switch","input":{"n":"$input.n"},
	     "on_error":{"strategy":"fail-branch"},
	     "params":{"cases":[
	       {"id":"big","logical_operator":"and","conditions":[{"var":"n","operator":">","value":"10"}]},
	       {"id":"small","logical_operator":"and","conditions":[{"var":"n","operator":">","value":"0"}]}]}},
	    {"kind":"step","id":"b","action":"kairo.pass"},
	    {"kind":"step","id":"s","action":"kairo.pass"},
	    {"kind":"step","id":"z","action":"kairo.pass"},
	    {"kind":"step","id":"e","action":"kairo.pass","input":{"t":"sw.error_type"}}],
	  "edges":[{"from":"sw","handle":"big","to":"b"},{"from":"sw","handle":"small","to":"s"},
	           {"from":"sw","handle":"false","to":"z"},{"from":"sw","handle":"fail-branch","to":"e"}]}}`)
	for _, tc := range []struct{ in, out string }{
		{`{"n":20}`, `{"b":{}}`},
		{`{"n":5}`, `{"s":{}}`},
		{`{"n":-1}`, `{"z":{}}`},
		{`{"n":"x"}`, `{"e":{"t":"ValueError"}}`},
	} {
		x := newSim(t, p)
		x.run(tc.in)
		if x.s.Status != StatusCompleted || string(x.s.Output) != tc.out {
			t.Fatalf("%s: %v %s %s", tc.in, x.s.Status, x.s.Error, x.s.Output)
		}
		x.checkReplay()
	}
	// Handles must match the cases; unknown operators and inputs are
	// rejected.
	for _, js := range []string{
		`{"kind":"step","id":"sw","action":"kairo.switch","input":{"n":"$input.n"},"handles":["a"],"params":{"cases":[{"id":"x","logical_operator":"and","conditions":[]}]}}`,
		`{"kind":"step","id":"sw","action":"kairo.switch","input":{"n":"$input.n"},"params":{"cases":[{"id":"x","logical_operator":"and","conditions":[{"var":"n","operator":"~="}]}]}}`,
		`{"kind":"step","id":"sw","action":"kairo.switch","params":{"cases":[{"id":"x","logical_operator":"and","conditions":[{"var":"n","operator":"is"}]}]}}`,
		`{"kind":"step","id":"sw","action":"llm","handles":["a"]}`,
	} {
		d, _ := ir.ParseDefinition([]byte(`{"name":"x","root":` + js + `}`))
		if _, err := ir.Compile(d, graphRegistry()); err == nil {
			t.Errorf("accepted %s", js)
		}
	}
}

// A branching worker step (question-classifier) declares its handles where
// it is placed.
func TestPerNodeHandles(t *testing.T) {
	reg := graphRegistry()
	reg.Register(ir.NodeSpec{Action: "qc", Effect: ir.EffectUnprotected, Branch: "class_id", Outputs: map[string]ir.FieldType{"class_id": {Type: ir.FieldEnum}}})
	d, _ := ir.ParseDefinition([]byte(`{"name":"q","root":{"kind":"graph",
	  "nodes":[{"kind":"step","id":"q","action":"qc","handles":["c1","c2"]},{"kind":"step","id":"a","action":"llm"},{"kind":"step","id":"b","action":"llm"}],
	  "edges":[{"from":"q","handle":"c1","to":"a"},{"from":"q","handle":"c2","to":"b"}]}}`))
	p, err := ir.Compile(d, reg)
	if err != nil {
		t.Fatal(err)
	}
	x := newSim(t, p)
	x.handler = func(c Command, n *ir.Node) Event {
		if n.ID == "q" {
			return ok(`{"class_id":"c2"}`)
		}
		return ok(`"` + n.ID + `"`)
	}
	x.run(`{}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"b":"b"}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
	// Without handles there is nothing to branch to.
	d, _ = ir.ParseDefinition([]byte(`{"name":"q","root":{"kind":"step","id":"q","action":"qc"}}`))
	if _, err := ir.Compile(d, reg); err == nil || !strings.Contains(err.Error(), "no handles") {
		t.Fatalf("%v", err)
	}
}
