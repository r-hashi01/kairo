package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"kairo/ir"
)

// Large payloads are kept out of the state as content-addressed blobs. In
// the state they appear as an envelope carrying the blob reference and the
// node's typed fields, so conditions never need the payload itself:
//
//	{"$blob":"sha256:…","size":123456,"fields":{"label":"refund"}}
//
// When a reference reaches into a blob beyond its typed fields, the core
// cannot read it (no I/O); it emits a deferred reference instead, which the
// dispatcher resolves off the transition path:
//
//	{"$blob":"sha256:…","$path":"a.b"}
var blobPrefix = []byte(`{"$blob":`)

// IsBlobRef reports whether v is a blob envelope or deferred blob reference.
func IsBlobRef(v json.RawMessage) bool {
	return bytes.HasPrefix(bytes.TrimLeft(v, " \t\r\n"), blobPrefix)
}

type blobEnvelope struct {
	Blob   string                     `json:"$blob"`
	Size   int64                      `json:"size,omitempty"`
	Fields map[string]json.RawMessage `json:"fields,omitempty"`
	Path   string                     `json:"$path,omitempty"`
}

// lookup finds the value of node in the scope chain starting at scope.
func (m *machine) lookup(node int32, scope uint32) json.RawMessage {
	want := m.p.Nodes[node].MapScope
	for {
		sc := m.s.Scopes[scope]
		if sc == nil {
			return nil
		}
		if sc.Map == want {
			return sc.Vals[node]
		}
		if scope == 0 {
			return nil
		}
		scope = sc.Parent
	}
}

func (m *machine) innermost(scope uint32) *Scope {
	return m.s.Scopes[scope]
}

func (m *machine) resolve(r ir.Ref, scope uint32) json.RawMessage {
	var v json.RawMessage
	switch r.Kind {
	case ir.RefInput:
		v = m.s.Input
	case ir.RefItem:
		if sc := m.innermost(scope); sc != nil {
			v = sc.Item
		}
	case ir.RefIndex:
		if sc := m.innermost(scope); sc != nil {
			return strconv.AppendInt(nil, int64(sc.Index), 10)
		}
	case ir.RefNode:
		v = m.lookup(r.Node, scope)
	case ir.RefVar:
		v = m.runVarsScope(scope).Vals[runVarsNode]
	}
	if len(v) == 0 {
		return null
	}
	return extract(v, r.Path)
}

// extract follows path into v. Missing values are null.
func extract(v json.RawMessage, path []string) json.RawMessage {
	for i, seg := range path {
		if IsBlobRef(v) {
			var env blobEnvelope
			if json.Unmarshal(v, &env) != nil {
				return null
			}
			if f, ok := env.Fields[seg]; ok {
				v = f
				continue
			}
			rest := strings.Join(path[i:], ".")
			if env.Path != "" {
				rest = env.Path + "." + rest
			}
			b, _ := json.Marshal(blobEnvelope{Blob: env.Blob, Path: rest})
			return b
		}
		t := bytes.TrimLeft(v, " \t\r\n")
		if len(t) == 0 {
			return null
		}
		switch t[0] {
		case '{':
			var obj map[string]json.RawMessage
			if json.Unmarshal(t, &obj) != nil {
				return null
			}
			f, ok := obj[seg]
			if !ok {
				return null
			}
			v = f
		case '[':
			idx, err := strconv.Atoi(seg)
			if err != nil {
				return null
			}
			var arr []json.RawMessage
			if json.Unmarshal(t, &arr) != nil || idx < 0 || idx >= len(arr) {
				return null
			}
			v = arr[idx]
		default:
			return null
		}
	}
	return v
}

// buildInput assembles the step's input object from its references.
func (m *machine) buildInput(n *ir.Node, scope uint32) json.RawMessage {
	return m.buildObject(n.Inputs, scope)
}

// buildObject assembles an object from named references.
func (m *machine) buildObject(ins []ir.Input, scope uint32) json.RawMessage {
	b := make([]byte, 0, 64)
	b = append(b, '{')
	for i, in := range ins {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendQuote(b, in.Name)
		b = append(b, ':')
		b = append(b, m.resolve(in.Ref, scope)...)
	}
	b = append(b, '}')
	return b
}

// evalPred evaluates a condition on a typed field. A missing field or a
// value of the wrong type makes the condition false.
func (m *machine) evalPred(p *ir.Pred, scope uint32) bool {
	v := bytes.TrimSpace(m.resolve(p.Ref, scope))
	switch p.Type {
	case ir.FieldBool:
		var b bool
		if json.Unmarshal(v, &b) != nil || bytes.Equal(v, null) {
			return false
		}
		if p.Op == ir.OpEq {
			return b == p.Bool
		}
		return b != p.Bool
	case ir.FieldInt, ir.FieldNumber:
		var f float64
		if json.Unmarshal(v, &f) != nil || bytes.Equal(v, null) {
			return false
		}
		switch p.Op {
		case ir.OpEq:
			return f == p.Num
		case ir.OpNe:
			return f != p.Num
		case ir.OpLt:
			return f < p.Num
		case ir.OpLe:
			return f <= p.Num
		case ir.OpGt:
			return f > p.Num
		case ir.OpGe:
			return f >= p.Num
		}
	case ir.FieldEnum:
		var s string
		if json.Unmarshal(v, &s) != nil {
			return false
		}
		switch p.Op {
		case ir.OpEq:
			return s == p.Str
		case ir.OpNe:
			return s != p.Str
		case ir.OpIn:
			for _, x := range p.Set {
				if x == s {
					return true
				}
			}
			return false
		}
	}
	return false
}

// protected evaluates a protected step.
func (m *machine) protected(n *ir.Node, scope uint32) (json.RawMessage, error) {
	if n.Switch != nil {
		return m.evalSwitch(n, scope)
	}
	switch n.Spec.Action {
	case ir.ActionAssign:
		return m.evalAssign(n, scope)
	case ir.ActionTemplate:
		return m.evalTemplate(n, scope)
	case ir.ActionCoalesce:
		return m.evalCoalesce(n, scope)
	case ir.ActionList:
		return m.evalList(n, scope)
	}
	if n.Spec.Action == ir.ActionAppend {
		b := []byte{'['}
		first := true
		for _, in := range n.Inputs {
			v := bytes.TrimSpace(m.resolve(in.Ref, scope))
			if len(v) == 0 || bytes.Equal(v, null) {
				continue
			}
			var parts []json.RawMessage
			if v[0] == '[' {
				if err := json.Unmarshal(v, &parts); err != nil {
					return nil, err
				}
			} else {
				parts = []json.RawMessage{v}
			}
			for _, p := range parts {
				if !first {
					b = append(b, ',')
				}
				first = false
				b = append(b, p...)
			}
		}
		return append(b, ']'), nil
	}
	in := m.buildInput(n, scope)
	if len(bytes.TrimSpace(n.Params)) == 0 {
		return in, nil
	}
	var base map[string]json.RawMessage
	if err := json.Unmarshal(n.Params, &base); err != nil {
		return nil, errors.New("params of a protected step must be an object")
	}
	var over map[string]json.RawMessage
	json.Unmarshal(in, &over)
	for k, v := range over {
		base[k] = v
	}
	return json.Marshal(base) // map keys are sorted: deterministic
}
