package ir

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Variables (ADR 0033) and the loop extras of ADR 0032.

// Dify variable types.
var varTypes = []string{"string", "number", "integer", "float", "boolean", "object", "secret", "file",
	"array[string]", "array[number]", "array[object]", "array[boolean]", "array[any]", "array[file]"}

// Operations of kairo.assign (Dify's Variable Assigner v2).
var assignOps = []string{"over-write", "clear", "append", "extend", "set", "+=", "-=", "*=", "/=", "remove-first", "remove-last"}

// ActionAssign writes run and loop variables.
const ActionAssign = "kairo.assign"

func compileVars(defs map[string]VarDef, what string, allowRef bool) ([]Var, error) {
	names := make([]string, 0, len(defs))
	for k := range defs {
		names = append(names, k)
	}
	slices.Sort(names)
	var out []Var
	for _, name := range names {
		d := defs[name]
		if name == "" || strings.ContainsAny(name, ".$[]") {
			return nil, fmt.Errorf("ir: %s: invalid variable name %q", what, name)
		}
		if !slices.Contains(varTypes, d.Type) {
			return nil, fmt.Errorf("ir: %s variable %q: unknown type %q", what, name, d.Type)
		}
		if d.Ref != "" && (!allowRef || len(d.Value) > 0) {
			return nil, fmt.Errorf("ir: %s variable %q: give a value, or (loop variables only) a ref", what, name)
		}
		v := Var{Name: name, Type: d.Type, Init: d.Value}
		if len(v.Init) > 0 && !json.Valid(v.Init) {
			return nil, fmt.Errorf("ir: %s variable %q: value is not JSON", what, name)
		}
		out = append(out, v)
	}
	return out, nil
}

// loopVarRef reports whether a reference to node t with path reads a
// variable of loop t: allowed from inside the loop.
func (c *compiler) loopVarRef(t int32, path []string) bool {
	n := &c.plan.Nodes[t]
	return n.Kind == KLoop && len(path) > 0 && slices.ContainsFunc(n.Vars, func(v Var) bool { return v.Name == path[0] })
}

// loopExtras resolves a loop's break condition, break_on nodes and the
// references of its variables' initial values.
func (c *compiler) loopExtras(i int32, d *Def) error {
	n := &c.plan.Nodes[i]
	body := n.Children[0]
	for k := range n.Vars {
		vd := d.Vars[n.Vars[k].Name]
		if vd.Ref == "" {
			continue
		}
		// Initial values are read when the loop starts, outside the body.
		r, err := c.parseRef(i, vd.Ref)
		if err != nil {
			return fmt.Errorf("ir: loop %q variable %q: %w", n.ID, n.Vars[k].Name, err)
		}
		n.Vars[k].Ref = &r
	}
	if b := d.Break; b != nil {
		if b.Logic != "and" && b.Logic != "or" {
			return fmt.Errorf("ir: loop %q: break logical_operator must be and or or", n.ID)
		}
		names := make([]string, 0, len(d.BreakInput))
		for k := range d.BreakInput {
			names = append(names, k)
		}
		slices.Sort(names)
		for _, name := range names {
			r, err := c.parseRef(body, d.BreakInput[name])
			if err != nil {
				return fmt.Errorf("ir: loop %q break input %q: %w", n.ID, name, err)
			}
			n.BreakIn = append(n.BreakIn, Input{Name: name, Ref: r})
		}
		for _, cd := range b.Conds {
			if _, ok := d.BreakInput[cd.Var]; !ok {
				return fmt.Errorf("ir: loop %q: break condition on %q, which is not a break input", n.ID, cd.Var)
			}
			if !slices.Contains(switchOps, cd.Op) {
				return fmt.Errorf("ir: loop %q: unknown operator %q", n.ID, cd.Op)
			}
		}
		n.Break = b
	}
	// Only members of a hand-written body graph: their values are cleared
	// when they are skipped, so a value means "ran in this round".
	bn := &c.plan.Nodes[body]
	for _, id := range d.BreakOn {
		t, ok := c.plan.ByID[id]
		if !ok || bn.Kind != KGraph || bn.Sugar != SugarGraph || c.plan.Nodes[t].Parent != body {
			return fmt.Errorf("ir: loop %q: break_on %q must be a node of the loop's body graph", n.ID, id)
		}
		n.BreakOn = append(n.BreakOn, t)
	}
	return nil
}

// compileAssign compiles the items of kairo.assign step i.
func (c *compiler) compileAssign(i int32, d *Def) error {
	n := &c.plan.Nodes[i]
	var p struct {
		Items []struct {
			Var   string          `json:"var"`
			Op    string          `json:"op"`
			Input string          `json:"input"`
			Value json.RawMessage `json:"value"`
		} `json:"items"`
	}
	if err := json.Unmarshal(d.Params, &p); err != nil || len(p.Items) == 0 {
		return fmt.Errorf("ir: assign %q: params must hold items (%v)", n.ID, err)
	}
	for _, it := range p.Items {
		if !slices.Contains(assignOps, it.Op) {
			return fmt.Errorf("ir: assign %q: unknown operation %q", n.ID, it.Op)
		}
		if it.Input != "" {
			if _, ok := d.Input[it.Input]; !ok {
				return fmt.Errorf("ir: assign %q: %q is not an input of the step", n.ID, it.Input)
			}
		}
		item := AssignItem{Loop: -1, Op: it.Op, Input: it.Input, Value: it.Value}
		head, name, _ := strings.Cut(it.Var, ".")
		if head == "$var" {
			k := slices.IndexFunc(c.plan.Vars, func(v Var) bool { return v.Name == name })
			if k < 0 {
				return fmt.Errorf("ir: assign %q: undeclared run variable %q", n.ID, it.Var)
			}
			item.Name, item.Type = name, c.plan.Vars[k].Type
		} else {
			l, ok := c.plan.ByID[head]
			if !ok || c.plan.Nodes[l].Kind != KLoop || !isAncestor(c.plan, l, i) {
				return fmt.Errorf("ir: assign %q: %q is neither $var.<name> nor a variable of an enclosing loop", n.ID, it.Var)
			}
			ln := &c.plan.Nodes[l]
			k := slices.IndexFunc(ln.Vars, func(v Var) bool { return v.Name == name })
			if k < 0 {
				return fmt.Errorf("ir: assign %q: loop %q has no variable %q", n.ID, head, name)
			}
			if ln.MapScope != n.MapScope {
				return fmt.Errorf("ir: assign %q: loop variables cannot be written inside a map", n.ID)
			}
			item.Loop, item.Name, item.Type = l, name, ln.Vars[k].Type
		}
		n.Assign = append(n.Assign, item)
	}
	return nil
}
