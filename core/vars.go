package core

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/r-hashi01/kairo/ir"
)

// Variables (ADR 0033), Dify-style loops and map element errors (ADR 0032).
//
// Run variables live in the root scope under the reserved node index -1
// as one JSON object; a loop's variables are the loop node's own value
// (an object) in the loop's scope, so <loop>.<name> reads them like any
// node output.

const runVarsNode int32 = -1

// RunVars returns the run's variables (an object), or nil if it has none.
func RunVars(s *State) json.RawMessage {
	if sc := s.Scopes[0]; sc != nil {
		return sc.Vals[runVarsNode]
	}
	return nil
}

// runVarsScope is the scope holding the run variables as seen from scope:
// a map element that wrote them has its own copy (graphon copies the
// variable pool for each iteration element, so such writes are not seen
// outside it; ADR 0033).
func (m *machine) runVarsScope(scope uint32) *Scope {
	for {
		sc := m.s.Scopes[scope]
		if sc == nil {
			return m.s.Scopes[0]
		}
		if _, ok := sc.Vals[runVarsNode]; ok || scope == 0 {
			return sc
		}
		scope = sc.Parent
	}
}

// localRunVars returns the scope to write run variables to from scope:
// the innermost map element's, copying them there on first write.
func (m *machine) localRunVars(scope uint32) *Scope {
	for x := scope; x != 0; {
		sc := m.s.Scopes[x]
		if sc == nil {
			break
		}
		if sc.Map >= 0 {
			if _, ok := sc.Vals[runVarsNode]; !ok {
				setVal(sc, runVarsNode, m.runVarsScope(sc.Parent).Vals[runVarsNode])
			}
			return sc
		}
		x = sc.Parent
	}
	return m.s.Scopes[0]
}

// zeroValue is the value of a variable of type t that has none
// (SegmentType.get_zero_value).
func zeroValue(t string) json.RawMessage {
	switch {
	case t == "string" || t == "secret":
		return json.RawMessage(`""`)
	case t == "number" || t == "integer":
		return json.RawMessage(`0`)
	case t == "float":
		return json.RawMessage(`0.0`)
	case t == "boolean":
		return json.RawMessage(`false`)
	case t == "object":
		return json.RawMessage(`{}`)
	case strings.HasPrefix(t, "array["):
		return json.RawMessage(`[]`)
	}
	return null
}

// initRunVars sets the run variables from the start event's values, the
// declared values, or the types' zero values.
func (m *machine) initRunVars(given json.RawMessage) {
	if len(m.p.Vars) == 0 {
		return
	}
	var in map[string]json.RawMessage
	json.Unmarshal(given, &in)
	obj := map[string]json.RawMessage{}
	for _, v := range m.p.Vars {
		switch {
		case in[v.Name] != nil:
			obj[v.Name] = in[v.Name]
		case len(v.Init) > 0:
			obj[v.Name] = v.Init
		default:
			obj[v.Name] = zeroValue(v.Type)
		}
	}
	b, _ := json.Marshal(obj) // sorted keys
	sc := m.s.Scopes[0]
	if sc.Vals == nil {
		sc.Vals = map[int32]json.RawMessage{}
	}
	sc.Vals[runVarsNode] = b
}

// scopeOf finds the scope holding node's value, starting at scope.
func (m *machine) scopeOf(node int32, scope uint32) *Scope {
	want := int32(-1)
	if node >= 0 {
		want = m.p.Nodes[node].MapScope
	}
	for {
		sc := m.s.Scopes[scope]
		if sc == nil {
			return nil
		}
		if sc.Map == want {
			return sc
		}
		if scope == 0 {
			return nil
		}
		scope = sc.Parent
	}
}

func setVal(sc *Scope, node int32, v json.RawMessage) {
	if sc.Vals == nil {
		sc.Vals = map[int32]json.RawMessage{}
	}
	sc.Vals[node] = v
}

// --- loops ------------------------------------------------------------------

// startLoop begins loop activation id: its variables, then the first round
// unless a "before" check already ends it.
func (m *machine) startLoop(id uint32, a *Act, n *ir.Node, node int32) {
	// break_on values from an earlier activation of this loop mean nothing.
	if sc := m.s.Scopes[a.Scope]; sc != nil {
		for _, b := range n.BreakOn {
			delete(sc.Vals, b)
		}
	}
	if len(n.Vars) > 0 {
		obj := map[string]json.RawMessage{}
		for _, v := range n.Vars {
			switch {
			case v.Ref != nil:
				obj[v.Name] = m.resolve(*v.Ref, a.Scope)
			case len(v.Init) > 0:
				obj[v.Name] = v.Init
			default:
				obj[v.Name] = zeroValue(v.Type)
			}
		}
		b, _ := json.Marshal(obj)
		setVal(m.s.Scopes[a.Scope], node, b)
		m.traceStart(id, a, n, b)
	} else {
		m.traceStart(id, a, n, nil)
	}
	if n.Before {
		stop := n.Pred != nil && !m.evalPred(n.Pred, a.Scope)
		if !stop && n.Break != nil {
			// As graphon: before the first round, a ValueError means "do
			// not break".
			brk, err := m.evalCase(*n.Break, breakInputs(n), a.Scope)
			var ce *condError
			if err != nil && (!asCondError(err, &ce) || ce.typ != "ValueError") {
				m.failAt(id, "loop "+n.ID+": "+err.Error())
				return
			}
			stop = err == nil && brk
		}
		if stop {
			out := null
			if n.VarsOut {
				out = m.loopVars(a, node, false)
			}
			m.complete(id, a, out)
			return
		}
	}
	m.trace(Trace{Kind: TrRoundStart, Act: id, Node: a.Node, StepID: m.stepID(id, a), Index: 0})
	m.start(n.Children[0], id, a.Scope, 0)
}

// roundDone is called when a round of loop pid finished with out.
func (m *machine) roundDone(pid uint32, pa *Act, pn *ir.Node, out json.RawMessage) {
	m.trace(Trace{Kind: TrRoundEnd, Act: pid, Node: pa.Node, StepID: m.stepID(pid, pa), Index: pa.Pos, Output: out})
	pa.Pos++
	node := pa.Node
	sc := m.s.Scopes[pa.Scope]
	stop := false
	for _, b := range pn.BreakOn {
		if sc != nil && sc.Vals[b] != nil {
			stop = true // a loop-end node ran in this round
		}
	}
	if !stop && pn.Break != nil {
		brk, err := m.evalCase(*pn.Break, breakInputs(pn), pa.Scope)
		if err != nil {
			m.failAt(pid, "loop "+pn.ID+": "+err.Error())
			return
		}
		stop = brk
	}
	if !stop && int(pa.Pos) < int(pn.MaxIter) && (pn.Pred == nil || m.evalPred(pn.Pred, pa.Scope)) {
		for _, b := range pn.BreakOn {
			delete(sc.Vals, b)
		}
		if pn.VarsOut {
			m.loopVars(pa, node, true) // loop_round so far, visible to the next round
		}
		m.trace(Trace{Kind: TrRoundStart, Act: pid, Node: pa.Node, StepID: m.stepID(pid, pa), Index: pa.Pos})
		m.start(pn.Children[0], pid, pa.Scope, 0)
		return
	}
	if pn.VarsOut {
		out = m.loopVars(pa, node, true)
	}
	m.complete(pid, pa, out)
}

// loopVars returns (and stores) the loop's variables, with loop_round if
// any round ran.
func (m *machine) loopVars(a *Act, node int32, rounds bool) json.RawMessage {
	sc := m.s.Scopes[a.Scope]
	obj := map[string]json.RawMessage{}
	json.Unmarshal(sc.Vals[node], &obj)
	if obj == nil {
		obj = map[string]json.RawMessage{}
	}
	if rounds {
		obj["loop_round"] = strconv.AppendInt(nil, int64(a.Pos), 10)
	}
	b, _ := json.Marshal(obj)
	setVal(sc, node, b)
	return b
}

func breakInputs(n *ir.Node) map[string]ir.Ref {
	in := make(map[string]ir.Ref, len(n.BreakIn))
	for _, x := range n.BreakIn {
		in[x.Name] = x.Ref
	}
	return in
}

// --- map elements -----------------------------------------------------------

// elementDone records element idx of map pid (out, or nothing if omitted)
// and moves the map on.
func (m *machine) elementDone(pid uint32, pa *Act, pn *ir.Node, idx int32, out json.RawMessage, omit bool) {
	m.trace(Trace{Kind: TrRoundEnd, Act: pid, Node: pa.Node, StepID: m.stepID(pid, pa), Index: idx, Output: out})
	if !omit {
		pa.Results[idx] = out
	}
	pa.Pending--
	if int(pa.Pos) < len(pa.Items) {
		m.launchElement(pid, pa, pn)
		return
	}
	if pa.Pending == 0 {
		res := mapResult(pn, pa.Results)
		pa.Results, pa.Items = nil, nil
		m.complete(pid, pa, res)
	}
}

// mapResult joins the element results in index order, leaving out omitted
// ones (nil); with flatten, results that are all lists are concatenated.
func mapResult(pn *ir.Node, results []json.RawMessage) json.RawMessage {
	parts := make([]json.RawMessage, 0, len(results))
	for _, r := range results {
		if r != nil {
			parts = append(parts, r)
		}
	}
	if pn.Flatten && len(parts) > 0 {
		var flat []json.RawMessage
		all := true
		for _, p := range parts {
			var l []json.RawMessage
			if t := bytes.TrimSpace(p); len(t) == 0 || t[0] != '[' || json.Unmarshal(t, &l) != nil {
				all = false
				break
			}
			flat = append(flat, l...)
		}
		if all {
			return joinArray(flat)
		}
	}
	return joinArray(parts)
}

// failAt fails the run because of activation id, unless the failure is
// inside an element of a map that contains element failures (ADR 0032):
// then only that element ends, with a null result or none.
func (m *machine) failAt(id uint32, msg string) {
	child := id
	for a := m.s.Acts[id]; a != nil && a.Parent != 0; a = m.s.Acts[a.Parent] {
		p := a.Parent
		pa := m.s.Acts[p]
		if pa == nil {
			break
		}
		if pn := &m.p.Nodes[pa.Node]; pn.Kind == ir.KMap && pn.ElemErr != ir.ElemFail {
			m.abortElement(p, pa, pn, m.s.Acts[child].Idx)
			return
		}
		child = p
	}
	m.fail(StatusFailed, msg)
}

// abortElement removes everything of element idx of map mid, aborting its
// running steps and timers, and records the element as failed.
func (m *machine) abortElement(mid uint32, ma *Act, mn *ir.Node, idx int32) {
	s := m.s
	// Collect the element's activations before removing any: finding an
	// activation's element walks its parents.
	var scope uint32
	var members []uint32
	for _, id := range sortedActs(s) {
		if root, ok := m.elementRoot(id, mid); ok && s.Acts[root].Idx == idx {
			members = append(members, id)
			if id == root {
				scope = s.Acts[id].Scope
			}
		}
	}
	for _, id := range members {
		if s.Acts[id].Flags&fReview != 0 {
			// A real command with an unknown outcome is never contained
			// (invariant 5): the run stops as before ADR 0032.
			m.fail(StatusFailed, "map "+mn.ID+": an element failed while a step of it needed review")
			return
		}
	}
	for _, id := range members {
		a := s.Acts[id]
		if a.Flags&fDispatched != 0 {
			m.out = append(m.out, Command{Kind: CmdAbort, Act: id, Node: a.Node, Attempt: a.Attempt})
			s.Inflight--
		}
		if a.Timer != 0 {
			m.out = append(m.out, Command{Kind: CmdCancelTimer, Node: a.Node, Timer: a.Timer})
		}
		delete(s.Acts, id)
	}
	// The element's scope and the scopes of maps nested in it.
	ids := make([]uint32, 0, len(s.Scopes))
	for id := range s.Scopes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		for x := id; x != 0; {
			if x == scope {
				delete(s.Scopes, id)
				break
			}
			sc := s.Scopes[x]
			if sc == nil {
				break
			}
			x = sc.Parent
		}
	}
	m.refreshBlocked()
	m.elementDone(mid, ma, mn, idx, null, mn.ElemErr == ir.ElemOmit)
}

// elementRoot returns the child of map mid that activation id descends from.
func (m *machine) elementRoot(id, mid uint32) (uint32, bool) {
	for a := m.s.Acts[id]; a != nil; a = m.s.Acts[a.Parent] {
		if a.Parent == mid {
			return id, true
		}
		if a.Parent == 0 {
			return 0, false
		}
		id = a.Parent
	}
	return 0, false
}

// --- kairo.assign -----------------------------------------------------------

// evalAssign applies a kairo.assign step's items in order on a working
// copy and, if all succeed, writes the variables (Dify's Variable Assigner
// v2: read-after-write inside the node, nothing written on failure).
func (m *machine) evalAssign(n *ir.Node, scope uint32) (json.RawMessage, error) {
	inputs := map[string]ir.Ref{}
	for _, in := range n.Inputs {
		inputs[in.Name] = in.Ref
	}
	type target struct {
		sc   *Scope
		node int32
	}
	objs := map[target]map[string]json.RawMessage{}
	var order []target
	var updated []ir.AssignItem
	get := func(it ir.AssignItem) (target, map[string]json.RawMessage) {
		t := target{node: runVarsNode, sc: m.localRunVars(scope)}
		if it.Loop >= 0 {
			t = target{node: it.Loop, sc: m.scopeOf(it.Loop, scope)}
		}
		if o, ok := objs[t]; ok {
			return t, o
		}
		o := map[string]json.RawMessage{}
		if t.sc != nil {
			json.Unmarshal(t.sc.Vals[t.node], &o)
		}
		objs[t] = o
		order = append(order, t)
		return t, o
	}
	for _, it := range n.Assign {
		_, obj := get(it)
		cur, ok := obj[it.Name]
		if !ok {
			return nil, &condError{"VariableNotFoundError", "variable " + it.Name + " not found"}
		}
		value, raw, skip, err := m.assignInput(it, inputs, scope)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		curV, err := pyDecode(cur)
		if err != nil {
			return nil, err
		}
		nv, err := applyAssign(it, curV, value)
		if err != nil {
			return nil, err
		}
		if f, ok := nv.(float64); ok && (f != f || f > 1.7976931348623157e308 || f < -1.7976931348623157e308) {
			// inf and nan have no JSON form.
			return nil, &condError{"InvalidInputValueError", "result of " + it.Op + " on " + it.Name + " is not a finite number"}
		}
		if b, ok := spliceAssign(it.Op, cur, raw, nv); ok {
			obj[it.Name] = b
		} else {
			obj[it.Name] = pyEncode(nil, nv)
		}
		updated = append(updated, it)
	}
	for _, t := range order {
		if t.sc == nil {
			continue
		}
		b, err := json.Marshal(objs[t]) // sorted keys
		if err != nil {
			return nil, &condError{"InvalidInputValueError", err.Error()}
		}
		setVal(t.sc, t.node, b)
	}
	if m.tracing {
		// Each variable once, in the order first written (as graphon).
		seen := map[[2]any]bool{}
		for _, it := range updated {
			k := [2]any{it.Loop, it.Name}
			if seen[k] {
				continue
			}
			seen[k] = true
			t := target{node: runVarsNode, sc: m.localRunVars(scope)}
			if it.Loop >= 0 {
				t = target{node: it.Loop, sc: m.scopeOf(it.Loop, scope)}
			}
			m.trace(Trace{Kind: TrVarUpdate, Var: it.Name, Loop: it.Loop, Output: objs[t][it.Name]})
		}
	}
	return json.RawMessage(`{}`), nil
}

var noValueOps = []string{"clear", "remove-first", "remove-last"}

// spliceAssign builds the result of an operation that only moves values
// (over-write, append, extend, remove-first, remove-last) from the bytes
// of the variable and of the input, so that objects inside keep their
// keys in order (as Python's dicts and JavaScript's objects do; decoding
// to a Go map would sort them). The result is the same value as nv, which
// was checked; ok is false when it cannot be spliced (nv is not a list or
// an object, or the bytes are not what was decoded).
func spliceAssign(op string, cur, in json.RawMessage, nv any) (json.RawMessage, bool) {
	switch nv.(type) {
	case []any, map[string]any:
	default:
		return nil, false
	}
	elems := func(b json.RawMessage) ([]json.RawMessage, bool) {
		var l []json.RawMessage
		if json.Unmarshal(b, &l) != nil {
			return nil, false
		}
		return l, true
	}
	var out []json.RawMessage
	switch op {
	case "over-write":
		if in == nil {
			return nil, false
		}
		return pyReencode(in)
	case "append":
		l, ok := elems(cur)
		if !ok || in == nil {
			return nil, false
		}
		out = append(l, in)
	case "extend":
		l, ok1 := elems(cur)
		add, ok2 := elems(in)
		if !ok1 || !ok2 {
			return nil, false
		}
		out = append(l, add...)
	case "remove-first", "remove-last":
		l, ok := elems(cur)
		if !ok {
			return nil, false
		}
		if len(l) > 0 && op == "remove-first" {
			l = l[1:]
		} else if len(l) > 0 {
			l = l[:len(l)-1]
		}
		out = l
	default:
		return nil, false
	}
	if l, ok := nv.([]any); !ok || len(l) != len(out) {
		return nil, false
	}
	b := []byte{'['}
	for i, e := range out {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, e...)
	}
	return pyReencode(append(b, ']'))
}

// pyReencode writes a JSON value as pyEncode would (numbers in Python's
// form, strings as json.Marshal writes them, no spaces) but keeps object
// keys in the order they come. ok is false for invalid JSON or a number
// with no finite value.
func pyReencode(raw []byte) (json.RawMessage, bool) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var b []byte
	// Per open container: whether it is an object, and how many tokens
	// (keys and values) it has had.
	type level struct {
		obj bool
		n   int
	}
	var stack []level
	for {
		tok, err := d.Token()
		if err != nil {
			return nil, false
		}
		if top := len(stack) - 1; top >= 0 {
			if _, closing := tok.(json.Delim); !closing || (tok != json.Delim('}') && tok != json.Delim(']')) {
				l := &stack[top]
				switch {
				case l.obj && l.n%2 == 1:
					b = append(b, ':')
				case l.n > 0:
					b = append(b, ',')
				}
				l.n++
			}
		}
		switch x := tok.(type) {
		case json.Delim:
			b = append(b, byte(x))
			switch x {
			case '{', '[':
				stack = append(stack, level{obj: x == '{'})
			default:
				stack = stack[:len(stack)-1]
			}
		case json.Number:
			v := pyNums(x)
			if f, ok := v.(float64); ok && (f != f || f > 1.7976931348623157e308 || f < -1.7976931348623157e308) {
				return nil, false
			}
			b = pyEncode(b, v)
		default:
			b = pyEncode(b, x)
		}
		if len(stack) == 0 {
			break
		}
	}
	if len(bytes.TrimSpace(raw[d.InputOffset():])) > 0 {
		return nil, false
	}
	return b, true
}

// assignInput checks that the item's operation and input kind fit the
// variable's type and returns its input value and the bytes it was
// decoded from (skip: a null variable input, which graphon ignores).
func (m *machine) assignInput(it ir.AssignItem, inputs map[string]ir.Ref, scope uint32) (any, json.RawMessage, bool, error) {
	if !opSupported(it.Type, it.Op) {
		return nil, nil, false, &condError{"OperationNotSupportedError", "operation " + it.Op + " is not supported for type " + it.Type}
	}
	noValue := slices.Contains(noValueOps, it.Op)
	if it.Input != "" || noValue {
		if slices.Contains([]string{"set", "+=", "-=", "*=", "/="}, it.Op) {
			return nil, nil, false, &condError{"InputTypeNotSupportedError", "input type variable is not supported for " + it.Op}
		}
	} else if !constantSupported(it.Type, it.Op) {
		return nil, nil, false, &condError{"InputTypeNotSupportedError", "input type constant is not supported for " + it.Op}
	}
	if noValue {
		return nil, nil, false, nil
	}
	var value any
	var raw json.RawMessage
	if it.Input != "" {
		var found bool
		raw, found = m.resolveFound(inputs[it.Input], scope)
		if !found {
			return nil, nil, false, &condError{"VariableNotFoundError", "variable " + it.Input + " not found"}
		}
		v, err := pyDecode(raw)
		if err != nil {
			return nil, nil, false, err
		}
		if v == nil {
			return nil, nil, true, nil
		}
		value = v
	} else if len(bytes.TrimSpace(it.Value)) > 0 {
		v, err := pyDecode(it.Value)
		if err != nil {
			return nil, nil, false, err
		}
		value, raw = v, it.Value
	}
	if s, ok := value.(string); ok && it.Op == "set" && it.Type == "object" {
		v, err := pyDecode(json.RawMessage(s))
		if err != nil {
			return nil, nil, false, &condError{"InvalidInputValueError", "invalid input value " + s}
		}
		value = v
	}
	if !inputValid(it.Type, it.Op, value) {
		return nil, nil, false, &condError{"InvalidInputValueError", "invalid input value for " + it.Op + " on " + it.Type}
	}
	return value, raw, false, nil
}

func isNumericType(t string) bool { return t == "number" || t == "integer" || t == "float" }

func opSupported(t, op string) bool {
	switch op {
	case "over-write", "clear":
		return true
	case "set":
		return t == "object" || t == "string" || t == "boolean" || isNumericType(t)
	case "+=", "-=", "*=", "/=":
		return isNumericType(t)
	}
	return strings.HasPrefix(t, "array[") // append, extend, remove-first, remove-last
}

func constantSupported(t, op string) bool {
	switch {
	case t == "string" || t == "object" || t == "boolean":
		return op == "over-write" || op == "set"
	case isNumericType(t):
		return op == "over-write" || op == "set" || op == "+=" || op == "-=" || op == "*=" || op == "/="
	}
	return false
}

// itemTypeOK is isinstance for the element types of array variables.
func itemTypeOK(t string, v any) bool {
	switch t {
	case "array[any]":
		switch v.(type) {
		case string, float64, int64, bool, map[string]any:
			return true
		}
	case "array[string]":
		_, ok := v.(string)
		return ok
	case "array[number]":
		switch v.(type) {
		case float64, int64, bool:
			return true
		}
	case "array[object]":
		_, ok := v.(map[string]any)
		return ok
	case "array[boolean]":
		_, ok := v.(bool)
		return ok
	}
	return false
}

func inputValid(t, op string, v any) bool {
	if slices.Contains(noValueOps, op) {
		return true
	}
	if isNumericType(t) {
		n, ok := pyNumber(v)
		return ok && !(op == "/=" && n == 0)
	}
	switch t {
	case "string":
		if _, ok := v.(string); ok {
			return true
		}
	case "boolean":
		if _, ok := v.(bool); ok {
			return true
		}
	case "object":
		if _, ok := v.(map[string]any); ok {
			return true
		}
	}
	switch op {
	case "append":
		return itemTypeOK(t, v)
	case "extend", "over-write":
		l, ok := v.([]any)
		if !ok {
			return false
		}
		for _, x := range l {
			if !itemTypeOK(t, x) {
				return false
			}
		}
		return true
	}
	return false
}

// applyAssign computes the variable's new value.
func applyAssign(it ir.AssignItem, cur, v any) (any, error) {
	switch it.Op {
	case "over-write", "set":
		return v, nil
	case "clear":
		z, _ := pyDecode(zeroValue(it.Type))
		return z, nil
	case "append":
		l, _ := cur.([]any)
		return append(slices.Clone(l), v), nil
	case "extend":
		l, _ := cur.([]any)
		add, _ := v.([]any)
		return append(slices.Clone(l), add...), nil
	case "remove-first", "remove-last":
		l, _ := cur.([]any)
		if len(l) == 0 {
			return l, nil
		}
		if it.Op == "remove-first" {
			return slices.Clone(l[1:]), nil
		}
		return slices.Clone(l[:len(l)-1]), nil
	}
	return arith(it.Op, cur, v)
}

// arith is Python arithmetic on ints (bools count as ints) and floats.
func arith(op string, a, b any) (any, error) {
	ai, aInt := pyIntOf(a)
	bi, bInt := pyIntOf(b)
	if op != "/=" && aInt && bInt {
		switch op {
		case "+=":
			return ai + bi, nil
		case "-=":
			return ai - bi, nil
		}
		return ai * bi, nil
	}
	x, ok1 := pyNumber(a)
	y, ok2 := pyNumber(b)
	if !ok1 || !ok2 {
		return nil, &condError{"TypeError", "unsupported operand types for " + op}
	}
	switch op {
	case "+=":
		return x + y, nil
	case "-=":
		return x - y, nil
	case "*=":
		return x * y, nil
	}
	return x / y, nil
}

func pyIntOf(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// pyEncode appends v as JSON, writing floats as Python does (2.0, 1e+16)
// and object keys sorted.
func pyEncode(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		return strconv.AppendBool(b, x)
	case int64:
		return strconv.AppendInt(b, x, 10)
	case float64:
		return append(b, pyFloatRepr(x)...)
	case string:
		s, _ := json.Marshal(x)
		return append(b, s...)
	case []any:
		b = append(b, '[')
		for i, e := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = pyEncode(b, e)
		}
		return append(b, ']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b = append(b, '{')
		for i, k := range keys {
			if i > 0 {
				b = append(b, ',')
			}
			ks, _ := json.Marshal(k)
			b = append(b, ks...)
			b = append(b, ':')
			b = pyEncode(b, x[k])
		}
		return append(b, '}')
	}
	return append(b, "null"...)
}

// pyFloatRepr formats f as Python's repr: the shortest digits, in fixed
// notation for exponents from -4 to 15 (with ".0" if integral), otherwise
// scientific with a two-digit exponent.
func pyFloatRepr(f float64) string {
	e := strconv.FormatFloat(f, 'e', -1, 64) // d.ddde±XX
	mant, exps, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(exps)
	if exp < -4 || exp >= 16 {
		sign := "+"
		if exp < 0 {
			sign, exp = "-", -exp
		}
		es := strconv.Itoa(exp)
		if len(es) < 2 {
			es = "0" + es
		}
		return mant + "e" + sign + es
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".") {
		s += ".0"
	}
	return s
}

func asCondError(err error, ce **condError) bool {
	c, ok := err.(*condError)
	if ok {
		*ce = c
	}
	return ok
}
