package core

import (
	"testing"
)

// kairo.template renders values as graphon's segments do (ADR 0028).
func TestTemplate(t *testing.T) {
	for _, tc := range []struct{ in, tpl, mode, want string }{
		{`{"s":"hi"}`, "prefix{{#in.s#}}suffix", "", `{"answer":"prefixhisuffix","files":[]}`},
		{`{"n":1,"f":1.0,"b":true}`, "{{#in.n#}} {{#in.f#}} {{#in.b#}}", "", `{"answer":"1 1.0 True","files":[]}`},
		{`{"l":["a","b"]}`, "{{#in.l#}}", "", `{"answer":"- a\n- b","files":[]}`},
		{`{"l":["a","b"]}`, "{{#in.l#}}", "text", `{"answer":"['a', 'b']","files":[]}`},
		{`{"o":{"k":"<v>","n":[1,2]}}`, "{{#in.o#}}", "text", `{"answer":"{\"k\": \"<v>\", \"n\": [1, 2]}","files":[]}`},
		{`{"o":{"k":1}}`, "{{#in.o#}}", "", `{"answer":"{\n  \"k\": 1\n}","files":[]}`},
		{`{"z":null}`, "[{{#in.z#}}]", "", `{"answer":"[]","files":[]}`},
		// Unknown selectors stay as their text, without the braces;
		// what is not a selector stays as it is.
		{`{}`, "{{#node.gone#}} {{#nodot#}}", "", `{"answer":"node.gone {{#nodot#}}","files":[]}`},
	} {
		p := compile(t, `{"name":"t","root":{"kind":"step","id":"a","action":"kairo.template",
		  "input":{"in.s":"$input.s","in.n":"$input.n","in.f":"$input.f","in.b":"$input.b","in.l":"$input.l","in.o":"$input.o","in.z":"$input.z"},
		  "params":{"template":`+jsonStr(tc.tpl)+`,"key":"answer","mode":"`+tc.mode+`","extra":{"files":[]}}}}`)
		x := newSim(t, p)
		x.run(tc.in)
		if x.s.Status != StatusCompleted || string(x.s.Output) != tc.want {
			t.Errorf("%s with %s: %v %s %s, want %s", tc.tpl, tc.in, x.s.Status, x.s.Error, x.s.Output, tc.want)
		}
	}
}

// kairo.coalesce takes the first input that exists, even if null, per
// group; with none, the key is left out.
func TestCoalesce(t *testing.T) {
	p := compileGraph(t, `{"name":"c","root":{"kind":"graph","nodes":[
	  {"kind":"step","id":"cls","action":"classify"},
	  {"kind":"step","id":"a","action":"kairo.pass","params":{"v":"A"}},
	  {"kind":"step","id":"b","action":"kairo.pass","params":{"v":null}},
	  {"kind":"step","id":"agg","action":"kairo.coalesce","input":{"a.v":"a.v","b.v":"b.v"},
	   "params":{"groups":[{"name":"G1","inputs":["a.v","b.v"]},{"name":"G2","inputs":["a.v"]}]}}],
	  "edges":[{"from":"cls","handle":"refund","to":"a"},{"from":"cls","handle":"other","to":"b"},
	           {"from":"a","to":"agg"},{"from":"b","to":"agg"}]}}`)
	for label, want := range map[string]string{
		"refund": `{"agg":{"G1":{"output":"A"},"G2":{"output":"A"}}}`,
		"other":  `{"agg":{"G1":{"output":null}}}`,
	} {
		x := newSim(t, p)
		x.handler = labelHandler(label)
		x.run(`{}`)
		if x.s.Status != StatusCompleted || string(x.s.Output) != want {
			t.Errorf("%s: %v %s %s, want %s", label, x.s.Status, x.s.Error, x.s.Output, want)
		}
		x.checkReplay()
	}
}

func jsonStr(s string) string {
	b := []byte{'"'}
	for _, c := range s {
		switch c {
		case '"', '\\':
			b = append(b, '\\', byte(c))
		case '\n':
			b = append(b, '\\', 'n')
		default:
			b = append(b, string(c)...)
		}
	}
	return string(append(b, '"'))
}

var labelHandler = label
