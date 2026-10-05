package core

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"kairo/ir"
)

// Built-in protected actions for the pure nodes of Dify workflows (ADR
// 0028): templates (answer) and coalescing (variable-aggregator). They
// follow graphon 0.7.0: values are rendered as its segments render them.

// evalTemplate renders params.template, replacing {{#name#}} with the
// input named name (graphon's convert_template: a name that resolves to
// nothing stays as its text, without the braces). The output is
// {params.key: text} merged over params.extra.
func (m *machine) evalTemplate(n *ir.Node, scope uint32) (json.RawMessage, error) {
	var p struct {
		Template string                     `json:"template"`
		Key      string                     `json:"key"`
		Mode     string                     `json:"mode"` // markdown (default) | text
		Extra    map[string]json.RawMessage `json:"extra"`
	}
	if err := json.Unmarshal(n.Params, &p); err != nil {
		return nil, valueErr("kairo.template params: " + err.Error())
	}
	if p.Key == "" {
		p.Key = "output"
	}
	inputs := map[string]ir.Ref{}
	for _, in := range n.Inputs {
		inputs[in.Name] = in.Ref
	}
	var b strings.Builder
	s := p.Template
	for {
		i := strings.Index(s, "{{#")
		if i < 0 {
			break
		}
		j := strings.Index(s[i+3:], "#}}")
		if j < 0 {
			break
		}
		name := s[i+3 : i+3+j]
		b.WriteString(s[:i])
		raw, found := json.RawMessage(nil), false
		if r, ok := inputs[name]; ok && validSelector(name) {
			raw, found = m.resolveFound(r, scope)
		}
		switch {
		case !found:
			if validSelector(name) {
				b.WriteString(name) // graphon keeps the selector text
			} else {
				b.WriteString(s[i : i+3+j+3])
			}
		default:
			v, err := pyDecode(raw)
			if err != nil {
				return nil, err
			}
			if p.Mode == "text" {
				b.WriteString(segText(v))
			} else {
				b.WriteString(segMarkdown(v))
			}
		}
		s = s[i+3+j+3:]
	}
	b.WriteString(s)
	out := map[string]json.RawMessage{}
	for k, v := range p.Extra {
		out[k] = v
	}
	out[p.Key] = jsonString(b.String())
	return jsonNoEscape(out), nil
}

// jsonNoEscape encodes v without Go's HTML escaping.
func jsonNoEscape(v any) json.RawMessage {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
	return bytes.TrimRight(b.Bytes(), "\n")
}

// jsonString encodes s without Go's HTML escaping, as Python writes it.
func jsonString(s string) json.RawMessage {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.Encode(s)
	return bytes.TrimRight(b.Bytes(), "\n")
}

// validSelector is graphon's VARIABLE_PATTERN: node.name[.name...].
func validSelector(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) < 2 || len(parts) > 11 || len(parts[0]) == 0 || len(parts[0]) > 50 {
		return false
	}
	for i, p := range parts {
		if p == "" || i > 0 && len(p) > 30 {
			return false
		}
		for k, c := range p {
			alpha := c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
			digit := c >= '0' && c <= '9'
			if !alpha && !digit || i > 0 && k == 0 && digit {
				return false
			}
		}
	}
	return true
}

// segText is a value as graphon's Segment.text renders it.
func segText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case map[string]any:
		return pyJSON(x, false)
	case []any:
		if len(x) == 0 {
			return ""
		}
		return pyRepr0(x)
	}
	return pyScalar(v)
}

// segMarkdown is a value as graphon's Segment.markdown renders it.
func segMarkdown(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case map[string]any:
		return pyJSON(x, true)
	case []any:
		lines := make([]string, len(x))
		for i, e := range x {
			lines[i] = "- " + pyStrValue(e)
		}
		return strings.Join(lines, "\n")
	}
	return pyScalar(v)
}

// pyScalar is str() of a scalar.
func pyScalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return pyFloatRepr(x)
	}
	return ""
}

// pyStrValue is str() of any value (containers as Python reprs them).
func pyStrValue(v any) string {
	switch v.(type) {
	case []any, map[string]any:
		return pyRepr0(v)
	case nil:
		return "None"
	}
	return pyScalar(v)
}

// pyRepr0 is repr() of a value.
func pyRepr0(v any) string {
	switch x := v.(type) {
	case string:
		return pyRepr(x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = pyRepr0(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := sortedKeys(x)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = pyRepr(k) + ": " + pyRepr0(x[k])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case nil:
		return "None"
	}
	return pyScalar(v)
}

// pyJSON is json.dumps(v, ensure_ascii=False) (indent=2 if indent).
// Keys are sorted: the original order is not kept by the JSON decoding.
func pyJSON(v any, indent bool) string {
	var b bytes.Buffer
	pyJSONTo(&b, v, indent, 0)
	return b.String()
}

func pyJSONTo(b *bytes.Buffer, v any, indent bool, depth int) {
	nl := func(d int) {
		if indent {
			b.WriteByte('\n')
			b.WriteString(strings.Repeat("  ", d))
		}
	}
	sep := ", "
	if indent {
		sep = ","
	}
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			b.WriteString("{}")
			return
		}
		b.WriteByte('{')
		for i, k := range sortedKeys(x) {
			if i > 0 {
				b.WriteString(sep)
			}
			nl(depth + 1)
			b.Write(jsonString(k))
			b.WriteString(": ")
			pyJSONTo(b, x[k], indent, depth+1)
		}
		nl(depth)
		b.WriteByte('}')
	case []any:
		if len(x) == 0 {
			b.WriteString("[]")
			return
		}
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteString(sep)
			}
			nl(depth + 1)
			pyJSONTo(b, e, indent, depth+1)
		}
		nl(depth)
		b.WriteByte(']')
	case string:
		b.Write(jsonString(x))
	default:
		b.Write(pyEncode(nil, x))
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// evalCoalesce is Dify's variable-aggregator: the first of the inputs that
// exists (even if null), per group. params {"inputs":[names]} outputs
// {"output": v}; {"groups":[{"name":..,"inputs":[..]}]} outputs
// {name: {"output": v}}. Nothing found leaves the key out.
func (m *machine) evalCoalesce(n *ir.Node, scope uint32) (json.RawMessage, error) {
	var p struct {
		Inputs []string `json:"inputs"`
		Groups []struct {
			Name   string   `json:"name"`
			Inputs []string `json:"inputs"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(n.Params, &p); err != nil {
		return nil, valueErr("kairo.coalesce params: " + err.Error())
	}
	refs := map[string]ir.Ref{}
	for _, in := range n.Inputs {
		refs[in.Name] = in.Ref
	}
	first := func(names []string) (json.RawMessage, bool) {
		for _, name := range names {
			if r, ok := refs[name]; ok {
				if v, found := m.resolveFound(r, scope); found {
					return v, true
				}
			}
		}
		return nil, false
	}
	out := map[string]json.RawMessage{}
	if len(p.Groups) == 0 {
		if v, ok := first(p.Inputs); ok {
			out["output"] = v
		}
		return json.Marshal(out)
	}
	for _, g := range p.Groups {
		if v, ok := first(g.Inputs); ok {
			b, _ := json.Marshal(map[string]json.RawMessage{"output": v})
			out[g.Name] = b
		}
	}
	return json.Marshal(out)
}
