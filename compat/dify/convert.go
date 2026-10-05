// Package dify converts Dify workflows (the "workflow" part of Dify's DSL,
// as JSON) into kairo definitions (ADR 0028).
//
// The graph is kept as a graph (ADR 0029). Pure nodes become kairo's
// protected steps: start and end kairo.pass, answer kairo.template,
// if-else kairo.switch, variable-aggregator kairo.coalesce, assigner
// kairo.assign. Nodes that reach the outside become steps of the action
// "dify.<type>", run by a worker that has Dify's node implementations:
// their params are the node's data, their inputs every variable the data
// refers to, named by selector ("node.var").
//
// The run's input is {<start variable>: value, ..., "sys": {"query": ...}};
// conversation variables are the run's variables (ADR 0033), given and
// returned by the host.
package dify

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"kairo/ir"
)

// Workflow is the "workflow" object of a Dify DSL document.
type Workflow struct {
	Graph struct {
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	} `json:"graph"`
	ConversationVariables []Variable `json:"conversation_variables"`
	EnvironmentVariables  []Variable `json:"environment_variables"`
}

type Node struct {
	ID       string          `json:"id"`
	ParentID string          `json:"parentId,omitempty"`
	Type     string          `json:"type,omitempty"`
	Data     json.RawMessage `json:"data"`
}

type Edge struct {
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceHandle string `json:"sourceHandle,omitempty"`
}

type Variable struct {
	Name      string          `json:"name"`
	ValueType string          `json:"value_type"`
	Value     json.RawMessage `json:"value"`
}

// Converted is a kairo definition and the specs of the dify.* actions it
// uses (register them before compiling).
type Converted struct {
	Definition *ir.Definition
	Specs      []ir.NodeSpec
	// Responses are the end and answer nodes, whose outputs make the run's
	// outputs (merged as graphon's merge_response_outputs does).
	Responses []string
}

// envNode holds the environment variables as a protected step.
const envNode = "__env"

type common struct {
	Type          string `json:"type"`
	ErrorStrategy string `json:"error_strategy"`
	DefaultValue  []struct {
		Key   string          `json:"key"`
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	} `json:"default_value"`
	RetryConfig *struct {
		RetryEnabled  bool `json:"retry_enabled"`
		MaxRetries    int  `json:"max_retries"`
		RetryInterval int  `json:"retry_interval"` // ms
	} `json:"retry_config"`
}

type converter struct {
	w        *Workflow
	byID     map[string]*Node
	data     map[string]common
	children map[string][]*Node
	specs    map[string]ir.NodeSpec
	resp     []string
	env      bool
}

// Convert converts a workflow into a definition named name.
func Convert(name string, w *Workflow) (*Converted, error) {
	c := &converter{w: w, byID: map[string]*Node{}, data: map[string]common{}, children: map[string][]*Node{}, specs: map[string]ir.NodeSpec{}}
	for i := range w.Graph.Nodes {
		n := &w.Graph.Nodes[i]
		var cm common
		if err := json.Unmarshal(n.Data, &cm); err != nil {
			return nil, fmt.Errorf("dify: node %s: %w", n.ID, err)
		}
		if cm.Type == "" || n.Type == "custom-note" {
			continue // notes
		}
		c.byID[n.ID] = n
		c.data[n.ID] = cm
		c.children[n.ParentID] = append(c.children[n.ParentID], n)
	}
	def := &ir.Definition{Name: name, Vars: map[string]ir.VarDef{}}
	for _, v := range w.ConversationVariables {
		def.Vars[v.Name] = ir.VarDef{Type: varType(v.ValueType), Value: constValue(v.ValueType, v.Value)}
	}
	if len(def.Vars) == 0 {
		def.Vars = nil
	}
	var root *Node
	for _, n := range c.children[""] {
		if t := c.data[n.ID].Type; t == "start" || strings.HasPrefix(t, "trigger-") {
			if root != nil {
				return nil, fmt.Errorf("dify: more than one start node (%s, %s)", root.ID, n.ID)
			}
			root = n
		}
	}
	if root == nil {
		return nil, fmt.Errorf("dify: no start node")
	}
	g, err := c.graph("", root.ID, nil)
	if err != nil {
		return nil, err
	}
	if c.env {
		g = c.withEnv(g)
	}
	def.Root = g
	out := &Converted{Definition: def, Responses: c.resp}
	for _, s := range c.specs {
		out.Specs = append(out.Specs, s)
	}
	slices.SortFunc(out.Specs, func(a, b ir.NodeSpec) int { return strings.Compare(a.Action, b.Action) })
	return out, nil
}

// withEnv runs the environment step before the graph.
func (c *converter) withEnv(g *ir.Def) *ir.Def {
	vals := map[string]json.RawMessage{}
	for _, v := range c.w.EnvironmentVariables {
		vals[v.Name] = constValue(v.ValueType, v.Value)
	}
	params, _ := json.Marshal(vals)
	return &ir.Def{Kind: "seq", Nodes: []*ir.Def{{Kind: "step", ID: envNode, Action: ir.ActionPass, Params: params}, g}}
}

// scope is where a node is: the iterations and loops around it, innermost
// last.
type scope []*Node

// graph converts the nodes whose parent is parent into a graph entered at
// entry. Nodes not reachable from entry are left out, as graphon never runs
// them.
func (c *converter) graph(parent, entry string, sc scope) (*ir.Def, error) {
	members := map[string]bool{}
	for _, n := range c.children[parent] {
		members[n.ID] = true
	}
	var edges []Edge
	out := map[string][]Edge{}
	for _, e := range c.w.Graph.Edges {
		if members[e.Source] && members[e.Target] {
			edges = append(edges, e)
			out[e.Source] = append(out[e.Source], e)
		}
	}
	reach := map[string]bool{entry: true}
	queue := []string{entry}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, e := range out[id] {
			if !reach[e.Target] {
				reach[e.Target] = true
				queue = append(queue, e.Target)
			}
		}
	}
	g := &ir.Def{Kind: "graph", ID: graphID(parent), Entry: []string{entry}}
	for _, n := range c.children[parent] {
		if !reach[n.ID] {
			continue
		}
		if c.data[n.ID].Type == "human-input" {
			wait, route, err := c.humanInput(n, sc)
			if err != nil {
				return nil, fmt.Errorf("dify: node %s (human-input): %w", n.ID, err)
			}
			g.Nodes = append(g.Nodes, wait, route)
			g.Edges = append(g.Edges, ir.EdgeDef{From: wait.ID, To: route.ID, Handle: ir.HandleSource})
			continue
		}
		d, err := c.node(n, sc)
		if err != nil {
			return nil, fmt.Errorf("dify: node %s (%s): %w", n.ID, c.data[n.ID].Type, err)
		}
		g.Nodes = append(g.Nodes, d)
	}
	seen := map[ir.EdgeDef]bool{}
	for _, e := range edges {
		if !reach[e.Source] {
			continue
		}
		h := e.SourceHandle
		if h == "" {
			h = ir.HandleSource
		}
		from := e.Source
		if c.data[from].Type == "human-input" {
			from = routeID(from) // its branches leave from the route
		}
		ed := ir.EdgeDef{From: from, To: e.Target, Handle: h}
		if !seen[ed] { // Dify may store an edge twice
			seen[ed] = true
			g.Edges = append(g.Edges, ed)
		}
	}
	return g, nil
}

func graphID(parent string) string {
	if parent == "" {
		return "__graph"
	}
	return parent + "__body"
}

// node converts one node.
func (c *converter) node(n *Node, sc scope) (*ir.Def, error) {
	cm := c.data[n.ID]
	d := &ir.Def{Kind: "step", ID: n.ID}
	var err error
	switch cm.Type {
	case "start", "trigger-webhook", "trigger-schedule", "trigger-plugin":
		err = c.start(d, n)
	case "end":
		err = c.end(d, n, sc)
	case "answer":
		err = c.answer(d, n, sc)
	case "if-else":
		err = c.ifElse(d, n, sc)
	case "variable-aggregator", "variable-assigner":
		if cm.Type == "variable-assigner" && !isAggregator(n.Data) {
			err = c.assigner(d, n, sc)
		} else {
			err = c.aggregator(d, n, sc)
		}
	case "assigner":
		err = c.assigner(d, n, sc)
	case "list-operator":
		err = c.listOperator(d, n, sc)
	case "iteration-start", "loop-start":
		d.Action = ir.ActionPass
	case "loop-end":
		d.Action = ir.ActionPass
	case "iteration":
		return c.iteration(n, sc)
	case "loop":
		return c.loop(n, sc)
	default:
		err = c.worker(d, n, sc)
	}
	if err != nil {
		return nil, err
	}
	return d, c.errorHandling(d, cm)
}

func isAggregator(data json.RawMessage) bool {
	var v struct {
		Variables json.RawMessage `json:"variables"`
	}
	json.Unmarshal(data, &v)
	return len(v.Variables) > 0
}

func (c *converter) start(d *ir.Def, n *Node) error {
	var data struct {
		Variables []struct {
			Variable string `json:"variable"`
		} `json:"variables"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	d.Action = ir.ActionPass
	d.Input = map[string]string{}
	for _, v := range data.Variables {
		d.Input[v.Variable] = "$input." + v.Variable
	}
	return nil
}

func (c *converter) end(d *ir.Def, n *Node, sc scope) error {
	var data struct {
		Outputs []struct {
			Variable      string   `json:"variable"`
			ValueSelector []string `json:"value_selector"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	d.Action = ir.ActionPass
	d.Input = map[string]string{}
	var nulls []string
	for _, o := range data.Outputs {
		if len(o.ValueSelector) == 0 {
			nulls = append(nulls, o.Variable)
			continue
		}
		r, err := c.ref(o.ValueSelector, sc)
		if err != nil {
			return err
		}
		d.Input[o.Variable] = r
	}
	if len(nulls) > 0 {
		p := map[string]any{}
		for _, k := range nulls {
			p[k] = nil
		}
		d.Params, _ = json.Marshal(p)
	}
	c.resp = append(c.resp, n.ID)
	return nil
}

var templateRef = regexp.MustCompile(`\{\{#([a-zA-Z0-9_]{1,50}(?:\.[a-zA-Z_][a-zA-Z0-9_]{0,29}){1,10})#\}\}`)

// templateInputs adds an input for every {{#selector#}} of s.
func (c *converter) templateInputs(d *ir.Def, s string, sc scope) error {
	for _, m := range templateRef.FindAllStringSubmatch(s, -1) {
		sel := strings.Split(m[1], ".")
		r, err := c.ref(sel, sc)
		if err != nil {
			continue // left as text, as graphon does when it finds nothing
		}
		if d.Input == nil {
			d.Input = map[string]string{}
		}
		d.Input[m[1]] = r
	}
	return nil
}

func (c *converter) answer(d *ir.Def, n *Node, sc scope) error {
	var data struct {
		Answer string `json:"answer"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	d.Action = ir.ActionTemplate
	if err := c.templateInputs(d, data.Answer, sc); err != nil {
		return err
	}
	d.Params, _ = json.Marshal(map[string]any{"template": data.Answer, "key": "answer", "extra": map[string]any{"files": []any{}}})
	c.resp = append(c.resp, n.ID)
	return nil
}

type difyCondition struct {
	VariableSelector []string        `json:"variable_selector"`
	Operator         string          `json:"comparison_operator"`
	Value            json.RawMessage `json:"value"`
	Sub              *struct {
		LogicalOperator string `json:"logical_operator"`
		Conditions      []struct {
			Key      string          `json:"key"`
			Operator string          `json:"comparison_operator"`
			Value    json.RawMessage `json:"value"`
		} `json:"conditions"`
	} `json:"sub_variable_condition"`
}

// conditions converts Dify conditions, adding their inputs to d.
func (c *converter) conditions(d *ir.Def, in map[string]string, conds []difyCondition, sc scope) ([]ir.Condition, error) {
	var out []ir.Condition
	for _, cd := range conds {
		r, err := c.ref(cd.VariableSelector, sc)
		if err != nil {
			return nil, err
		}
		name := strings.Join(cd.VariableSelector, ".")
		in[name] = r
		x := ir.Condition{Var: name, Op: cd.Operator, Value: cd.Value}
		if v := strings.TrimSpace(string(cd.Value)); v != "" && v[0] == '"' {
			var s string
			json.Unmarshal(cd.Value, &s)
			tmp := &ir.Def{}
			c.templateInputs(tmp, s, sc)
			for k, v := range tmp.Input {
				in[k] = v
			}
		}
		if cd.Sub != nil {
			x.Sub = &ir.SubConditions{Logic: cd.Sub.LogicalOperator}
			for _, s := range cd.Sub.Conditions {
				x.Sub.Conds = append(x.Sub.Conds, ir.SubCondition{Key: s.Key, Op: s.Operator, Value: s.Value})
			}
		}
		out = append(out, x)
	}
	return out, nil
}

func (c *converter) ifElse(d *ir.Def, n *Node, sc scope) error {
	var data struct {
		Cases []struct {
			CaseID          string          `json:"case_id"`
			LogicalOperator string          `json:"logical_operator"`
			Conditions      []difyCondition `json:"conditions"`
		} `json:"cases"`
		// Legacy shape: one case, handle "true".
		LogicalOperator string          `json:"logical_operator"`
		Conditions      []difyCondition `json:"conditions"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	if data.Cases == nil {
		data.Cases = append(data.Cases, struct {
			CaseID          string          `json:"case_id"`
			LogicalOperator string          `json:"logical_operator"`
			Conditions      []difyCondition `json:"conditions"`
		}{"true", data.LogicalOperator, data.Conditions})
	}
	d.Action = ir.ActionSwitch
	d.Input = map[string]string{}
	var sw ir.Switch
	for _, cs := range data.Cases {
		conds, err := c.conditions(d, d.Input, cs.Conditions, sc)
		if err != nil {
			return err
		}
		logic := cs.LogicalOperator
		if logic == "" {
			logic = "and"
		}
		sw.Cases = append(sw.Cases, ir.SwitchCase{ID: cs.CaseID, Logic: logic, Conds: conds})
	}
	d.Params, _ = json.Marshal(sw)
	return nil
}

func (c *converter) aggregator(d *ir.Def, n *Node, sc scope) error {
	var data struct {
		Variables        [][]string `json:"variables"`
		AdvancedSettings *struct {
			GroupEnabled bool `json:"group_enabled"`
			Groups       []struct {
				GroupName string     `json:"group_name"`
				Variables [][]string `json:"variables"`
			} `json:"groups"`
		} `json:"advanced_settings"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	d.Action = ir.ActionCoalesce
	d.Input = map[string]string{}
	names := func(sels [][]string) ([]string, error) {
		var out []string
		for _, s := range sels {
			r, err := c.ref(s, sc)
			if err != nil {
				return nil, err
			}
			name := strings.Join(s, ".")
			d.Input[name] = r
			out = append(out, name)
		}
		return out, nil
	}
	type group struct {
		Name   string   `json:"name"`
		Inputs []string `json:"inputs"`
	}
	if a := data.AdvancedSettings; a != nil && a.GroupEnabled {
		var gs []group
		for _, g := range a.Groups {
			ins, err := names(g.Variables)
			if err != nil {
				return err
			}
			gs = append(gs, group{g.GroupName, ins})
		}
		d.Params, _ = json.Marshal(map[string]any{"groups": gs})
		return nil
	}
	ins, err := names(data.Variables)
	if err != nil {
		return err
	}
	d.Params, _ = json.Marshal(map[string]any{"inputs": ins})
	return nil
}

func (c *converter) assigner(d *ir.Def, n *Node, sc scope) error {
	var data struct {
		Version string `json:"version"`
		Items   []struct {
			VariableSelector []string        `json:"variable_selector"`
			InputType        string          `json:"input_type"`
			Operation        string          `json:"operation"`
			Value            json.RawMessage `json:"value"`
		} `json:"items"`
		// Version 1.
		AssignedVariableSelector []string `json:"assigned_variable_selector"`
		InputVariableSelector    []string `json:"input_variable_selector"`
		WriteMode                string   `json:"write_mode"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	type item struct {
		Var   string          `json:"var"`
		Op    string          `json:"op"`
		Input string          `json:"input,omitempty"`
		Value json.RawMessage `json:"value,omitempty"`
	}
	d.Action = ir.ActionAssign
	d.Input = map[string]string{}
	target := func(sel []string) (string, error) {
		if len(sel) == 2 && sel[0] == "conversation" {
			return "$var." + sel[1], nil
		}
		if len(sel) == 2 && c.isLoop(sel[0]) {
			return sel[0] + "." + sel[1], nil
		}
		return "", fmt.Errorf("cannot assign to %v", sel)
	}
	var items []item
	if data.Items == nil && data.AssignedVariableSelector != nil {
		t, err := target(data.AssignedVariableSelector)
		if err != nil {
			return err
		}
		it := item{Var: t, Op: data.WriteMode}
		if data.WriteMode != "clear" && len(data.InputVariableSelector) > 0 {
			r, err := c.ref(data.InputVariableSelector, sc)
			if err != nil {
				return err
			}
			it.Input = strings.Join(data.InputVariableSelector, ".")
			d.Input[it.Input] = r
		}
		items = append(items, it)
	}
	for _, x := range data.Items {
		t, err := target(x.VariableSelector)
		if err != nil {
			return err
		}
		it := item{Var: t, Op: x.Operation}
		var sel []string
		if x.InputType == "variable" && json.Unmarshal(x.Value, &sel) == nil && len(sel) > 0 {
			r, err := c.ref(sel, sc)
			if err != nil {
				return err
			}
			it.Input = strings.Join(sel, ".")
			d.Input[it.Input] = r
		} else if x.InputType != "variable" {
			it.Value = x.Value
		}
		items = append(items, it)
	}
	d.Params, _ = json.Marshal(map[string]any{"items": items})
	return nil
}

func (c *converter) listOperator(d *ir.Def, n *Node, sc scope) error {
	var data struct {
		Variable []string `json:"variable"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return err
	}
	d.Action = ir.ActionList
	d.Input = map[string]string{}
	name := strings.Join(data.Variable, ".")
	if r, err := c.ref(data.Variable, sc); err == nil {
		d.Input[name] = r
	}
	// Templates in condition values and the extract serial.
	for _, sel := range selectors(n.Data) {
		if r, err := c.ref(sel, sc); err == nil {
			d.Input[strings.Join(sel, ".")] = r
		}
	}
	var params map[string]json.RawMessage
	json.Unmarshal(n.Data, &params)
	params["variable_input"], _ = json.Marshal(name)
	d.Params, _ = json.Marshal(params)
	return nil
}

// HumanTimeout is the handle a human input takes when its form times out
// (Dify's TIMEOUT_HANDLE).
const HumanTimeout = "__timeout"

// HumanSignal is the signal that answers human input node id. Its payload
// is {"handle": <user action id>, "outputs": {<field>: value, ...}}, sent by
// the host when the form is submitted (ADR 0036).
func HumanSignal(id string) string { return "human-input:" + id }

func routeID(id string) string { return id + "__route" }

// humanInput converts a human input node into a wait for its signal (with
// the node's timeout) and a route that branches on the chosen action, or
// on the timeout. References to the node read the submitted outputs.
func (c *converter) humanInput(n *Node, sc scope) (*ir.Def, *ir.Def, error) {
	var data struct {
		UserActions []struct {
			ID string `json:"id"`
		} `json:"user_actions"`
		Timeout     *int   `json:"timeout"`
		TimeoutUnit string `json:"timeout_unit"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return nil, nil, err
	}
	timeout := 36 * time.Hour
	if data.Timeout != nil {
		unit := time.Hour
		if data.TimeoutUnit == "day" {
			unit = 24 * time.Hour
		}
		timeout = time.Duration(*data.Timeout) * unit
	}
	wait := &ir.Def{Kind: "wait", ID: n.ID, Signal: HumanSignal(n.ID), Timeout: ir.Duration(timeout)}
	sw := ir.Switch{Cases: []ir.SwitchCase{{ID: HumanTimeout, Logic: "and",
		Conds: []ir.Condition{{Var: "w.timed_out", Op: "is", Value: json.RawMessage("true")}}}}}
	for _, a := range data.UserActions {
		v, _ := json.Marshal(a.ID)
		sw.Cases = append(sw.Cases, ir.SwitchCase{ID: a.ID, Logic: "and",
			Conds: []ir.Condition{{Var: "w.handle", Op: "is", Value: v}}})
	}
	params, _ := json.Marshal(sw)
	route := &ir.Def{Kind: "step", ID: routeID(n.ID), Action: ir.ActionSwitch, Params: params,
		Input: map[string]string{"w.timed_out": n.ID + ".timed_out", "w.handle": n.ID + ".payload.handle"}}
	return wait, route, nil
}

func (c *converter) isLoop(id string) bool {
	return c.data[id].Type == "loop"
}

func (c *converter) iteration(n *Node, sc scope) (*ir.Def, error) {
	var data struct {
		IteratorSelector []string `json:"iterator_selector"`
		OutputSelector   []string `json:"output_selector"`
		IsParallel       bool     `json:"is_parallel"`
		ParallelNums     int      `json:"parallel_nums"`
		ErrorHandleMode  string   `json:"error_handle_mode"`
		FlattenOutput    *bool    `json:"flatten_output"`
		StartNodeID      string   `json:"start_node_id"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return nil, err
	}
	over, err := c.ref(data.IteratorSelector, sc)
	if err != nil {
		return nil, err
	}
	inner := append(slices.Clone(sc), n)
	body, err := c.graph(n.ID, data.StartNodeID, inner)
	if err != nil {
		return nil, err
	}
	d := &ir.Def{Kind: "map", ID: n.ID, Over: over, Body: body, Flatten: data.FlattenOutput == nil || *data.FlattenOutput}
	d.MaxConcurrency = 1
	if data.IsParallel {
		d.MaxConcurrency = max(data.ParallelNums, 1)
	}
	switch data.ErrorHandleMode {
	case "continue-on-error":
		d.OnElementError = "null"
	case "remove-abnormal-output":
		d.OnElementError = "omit"
	default:
		d.OnElementError = "fail"
	}
	if len(data.OutputSelector) > 0 {
		r, err := c.ref(data.OutputSelector, inner)
		if err != nil {
			return nil, err
		}
		d.ElementOutput = r
	}
	return d, nil
}

func (c *converter) loop(n *Node, sc scope) (*ir.Def, error) {
	var data struct {
		LoopCount       int             `json:"loop_count"`
		BreakConditions []difyCondition `json:"break_conditions"`
		LogicalOperator string          `json:"logical_operator"`
		LoopVariables   []struct {
			Label     string          `json:"label"`
			VarType   string          `json:"var_type"`
			ValueType string          `json:"value_type"`
			Value     json.RawMessage `json:"value"`
		} `json:"loop_variables"`
		StartNodeID string `json:"start_node_id"`
	}
	if err := json.Unmarshal(n.Data, &data); err != nil {
		return nil, err
	}
	inner := append(slices.Clone(sc), n)
	body, err := c.graph(n.ID, data.StartNodeID, inner)
	if err != nil {
		return nil, err
	}
	d := &ir.Def{Kind: "loop", ID: n.ID, Body: body, MaxIter: max(data.LoopCount, 1), Check: "before", LoopOutput: "vars"}
	if len(data.LoopVariables) > 0 {
		d.Vars = map[string]ir.VarDef{}
	}
	for _, v := range data.LoopVariables {
		vd := ir.VarDef{Type: varType(v.VarType)}
		if v.ValueType == "variable" {
			var sel []string
			if err := json.Unmarshal(v.Value, &sel); err != nil {
				return nil, fmt.Errorf("loop variable %s: %w", v.Label, err)
			}
			if vd.Ref, err = c.ref(sel, sc); err != nil {
				return nil, err
			}
		} else {
			vd.Value = constValue(v.VarType, v.Value)
		}
		d.Vars[v.Label] = vd
	}
	if len(data.BreakConditions) > 0 {
		in := map[string]string{}
		conds, err := c.conditions(d, in, data.BreakConditions, inner)
		if err != nil {
			return nil, err
		}
		logic := data.LogicalOperator
		if logic == "" {
			logic = "and"
		}
		d.Break = &ir.SwitchCase{ID: "break", Logic: logic, Conds: conds}
		d.BreakInput = in
	}
	for _, ch := range c.children[n.ID] {
		if c.data[ch.ID].Type == "loop-end" {
			d.BreakOn = append(d.BreakOn, ch.ID)
		}
	}
	return d, nil
}

// retries are the node types graphon retries.
var retries = map[string]bool{"llm": true, "code": true, "http-request": true, "tool": true}

// effects of the node types run by workers (ADR 0035).
var real = map[string]bool{"http-request": true, "tool": true, "agent": true, "knowledge-index": true}

func (c *converter) worker(d *ir.Def, n *Node, sc scope) error {
	t := c.data[n.ID].Type
	d.Action = "dify." + t
	d.Params = n.Data
	sels := selectors(n.Data)
	if len(sels) > 0 {
		d.Input = map[string]string{}
	}
	for _, sel := range sels {
		r, err := c.ref(sel, sc)
		if err != nil {
			continue // not resolvable here: the worker sees it missing, as graphon would
		}
		d.Input[strings.Join(sel, ".")] = r
	}
	spec := ir.NodeSpec{Action: d.Action, Effect: ir.EffectUnprotected}
	if real[t] {
		spec.Effect = ir.EffectReal
		d.OnUnknown = "fail" // Dify does not stop for review (ADR 0035)
	}
	if t == "question-classifier" {
		var data struct {
			Classes []struct {
				ID string `json:"id"`
			} `json:"classes"`
		}
		json.Unmarshal(n.Data, &data)
		for _, cl := range data.Classes {
			d.Handles = append(d.Handles, cl.ID)
		}
		spec.Branch = "class_id"
		spec.Outputs = map[string]ir.FieldType{"class_id": {Type: ir.FieldEnum}}
	}
	c.specs[d.Action] = spec
	return nil
}

// selectors finds the variables a node's data refers to: arrays of strings
// under keys ending in "selector", and {{#node.var#}} in strings.
func selectors(data json.RawMessage) [][]string {
	var v any
	json.Unmarshal(data, &v)
	var out [][]string
	seen := map[string]bool{}
	add := func(sel []string) {
		if len(sel) >= 2 && !seen[strings.Join(sel, ".")] {
			seen[strings.Join(sel, ".")] = true
			out = append(out, sel)
		}
	}
	var walk func(key string, v any)
	walk = func(key string, v any) {
		switch x := v.(type) {
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				walk(k, x[k])
			}
		case []any:
			if strings.HasSuffix(key, "selector") {
				var sel []string
				for _, e := range x {
					s, ok := e.(string)
					if !ok {
						sel = nil
						break
					}
					sel = append(sel, s)
				}
				if sel != nil {
					add(sel)
					return
				}
			}
			for _, e := range x {
				walk(key, e)
			}
		case string:
			for _, m := range templateRef.FindAllStringSubmatch(x, -1) {
				add(strings.Split(m[1], "."))
			}
		}
	}
	walk("", v)
	return out
}

// ref converts a Dify selector seen from scope sc into a kairo reference.
func (c *converter) ref(sel []string, sc scope) (string, error) {
	if len(sel) < 2 {
		return "", fmt.Errorf("selector %v is too short", sel)
	}
	head, rest := sel[0], sel[1:]
	path := func(p []string) string {
		if len(p) == 0 {
			return ""
		}
		return "." + strings.Join(p, ".")
	}
	switch head {
	case "sys":
		return "$input.sys" + path(rest), nil
	case "env":
		c.env = true
		return envNode + path(rest), nil
	case "conversation":
		return "$var" + path(rest), nil
	}
	n := c.byID[head]
	if n == nil {
		return "", fmt.Errorf("selector %v: no node %s", sel, head)
	}
	switch c.data[head].Type {
	case "human-input":
		// The submitted form: the signal's outputs.
		return head + ".payload.outputs" + path(rest), nil
	case "iteration":
		inner := len(sc) > 0 && sc[len(sc)-1].ID == head
		switch rest[0] {
		case "item":
			if !inner {
				return "", fmt.Errorf("selector %v: an outer iteration's item is not supported", sel)
			}
			return "$item" + path(rest[1:]), nil
		case "index":
			if !inner {
				return "", fmt.Errorf("selector %v: an outer iteration's index is not supported", sel)
			}
			return "$index", nil
		case "output":
			// The map's value is the output list itself.
			return head + path(rest[1:]), nil
		}
	}
	return head + path(rest), nil
}

// errorHandling converts a node's retry and error strategy (ADR 0030).
func (c *converter) errorHandling(d *ir.Def, cm common) error {
	if d.Kind != "step" {
		return nil
	}
	// graphon retries only these node types (their Node.retry).
	if r := cm.RetryConfig; r != nil && r.RetryEnabled && retries[cm.Type] {
		d.Retry = &ir.RetryDef{MaxAttempts: r.MaxRetries + 1, Interval: ir.Duration(time.Duration(r.RetryInterval) * time.Millisecond)}
	}
	switch cm.ErrorStrategy {
	case "fail-branch":
		d.OnError = &ir.OnErrorDef{Strategy: "fail-branch"}
	case "default-value":
		v := map[string]json.RawMessage{}
		for _, dv := range cm.DefaultValue {
			v[dv.Key] = constValue(dv.Type, dv.Value)
		}
		b, _ := json.Marshal(v)
		d.OnError = &ir.OnErrorDef{Strategy: "default-value", Value: b}
	}
	return nil
}

// varType maps a Dify value type to a kairo variable type.
func varType(t string) string {
	switch t {
	case "", "text", "paragraph", "select", "text-input":
		return "string"
	}
	return t
}

// constValue is a constant of Dify type t: numbers given as strings are
// numbers, as Dify converts them.
func constValue(t string, v json.RawMessage) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	var s string
	if (t == "number" || t == "integer" || t == "float") && json.Unmarshal(v, &s) == nil {
		s = strings.TrimSpace(s)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil && t != "float" {
			return json.RawMessage(strconv.FormatInt(i, 10))
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			b, _ := json.Marshal(f)
			return b
		}
	}
	if t == "boolean" && json.Unmarshal(v, &s) == nil {
		if b, err := strconv.ParseBool(s); err == nil {
			return json.RawMessage(strconv.FormatBool(b))
		}
	}
	return v
}
