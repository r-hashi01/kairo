package ir

import "testing"

func TestFreezeThawKeepsHash(t *testing.T) {
	reg := NewRegistry()
	reg.Register(NodeSpec{Action: "llm", Effect: EffectUnprotected, Outputs: map[string]FieldType{"ok": {Type: FieldBool}}})
	d, _ := ParseDefinition([]byte(`{"name":"x","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"cond","if":{"field":"a.ok","op":"eq","value":true},"then":{"kind":"step","id":"b","action":"undeclared"}}]}}`))
	p, err := Compile(d, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !p.HasReal {
		t.Fatal("undeclared action must be treated as real")
	}
	// The registry changes afterwards; the frozen plan must not.
	reg.Register(NodeSpec{Action: "llm", Effect: EffectReal})
	q, err := p.Freeze().Thaw()
	if err != nil {
		t.Fatal(err)
	}
	if q.Hash != p.Hash {
		t.Fatalf("hash changed: %s vs %s", p.Hash, q.Hash)
	}
}

func TestRefScoping(t *testing.T) {
	reg := NewRegistry()
	bad := []string{
		// $item outside a map
		`{"name":"x","root":{"kind":"step","id":"a","action":"kairo.pass","input":{"v":"$item"}}}`,
		// reference into a map body from outside it
		`{"name":"x","root":{"kind":"seq","nodes":[
		  {"kind":"map","over":"$input.xs","body":{"kind":"step","id":"in","action":"kairo.pass"}},
		  {"kind":"step","id":"out","action":"kairo.pass","input":{"v":"in"}}]}}`,
		// unknown node
		`{"name":"x","root":{"kind":"step","id":"a","action":"kairo.pass","input":{"v":"nope"}}}`,
		// duplicate id
		`{"name":"x","root":{"kind":"seq","nodes":[{"kind":"step","id":"a","action":"x"},{"kind":"step","id":"a","action":"x"}]}}`,
	}
	for i, js := range bad {
		d, err := ParseDefinition([]byte(js))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Compile(d, reg); err == nil {
			t.Errorf("case %d compiled but should not", i)
		}
	}
}
