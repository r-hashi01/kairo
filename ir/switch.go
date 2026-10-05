package ir

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Switch is the compiled condition of a kairo.switch step (ADR 0031): Dify
// if-else cases, evaluated in order; the first that holds chooses its
// handle, "false" if none does. Conditions name the step's inputs.
//
//	{"cases":[{"id":"c1","logical_operator":"and","conditions":[
//	  {"var":"answer","operator":"contains","value":"yes"},
//	  {"var":"files","operator":"contains","sub":{"logical_operator":"or",
//	    "conditions":[{"key":"extension","operator":"is","value":".pdf"}]}}]}]}
type Switch struct {
	Cases []SwitchCase `json:"cases"`
}

type SwitchCase struct {
	ID    string      `json:"id"`
	Logic string      `json:"logical_operator"` // and | or
	Conds []Condition `json:"conditions"`
}

// Condition compares the input named Var. Value is a JSON string (which
// may embed {{#input#}} references), an array of strings or booleans, a
// boolean, or absent.
type Condition struct {
	Var   string          `json:"var"`
	Op    string          `json:"operator"`
	Value json.RawMessage `json:"value,omitempty"`
	Sub   *SubConditions  `json:"sub,omitempty"` // file attributes of a file array
}

type SubConditions struct {
	Logic string         `json:"logical_operator"`
	Conds []SubCondition `json:"conditions"`
}

type SubCondition struct {
	Key   string          `json:"key"`
	Op    string          `json:"operator"`
	Value json.RawMessage `json:"value,omitempty"`
}

// Condition operators of graphon 0.7.0 (utils/condition).
var switchOps = []string{"contains", "not contains", "start with", "end with", "is", "is not", "empty", "not empty",
	"in", "not in", "all of", "=", "≠", ">", "<", "≥", "≤", "null", "not null", "exists", "not exists"}

// File attributes a sub-condition may test.
var fileAttrs = []string{"type", "size", "name", "mime_type", "transfer_method", "url", "extension", "related_id"}

// branchHandles resolves the handles of step i (ADR 0029, 0031).
func (c *compiler) branchHandles(i int32, d *Def) error {
	n := &c.plan.Nodes[i]
	spec := n.Spec
	if spec.Action == ActionSwitch {
		var sw Switch
		if err := json.Unmarshal(d.Params, &sw); err != nil || len(sw.Cases) == 0 {
			return fmt.Errorf("ir: switch %q: params must hold cases (%v)", n.ID, err)
		}
		hs := []string{}
		for _, cs := range sw.Cases {
			if cs.ID == "" || slices.Contains(hs, cs.ID) || cs.ID == "false" || cs.ID == HandleFailBranch {
				return fmt.Errorf("ir: switch %q: case ids must be unique, non-empty and not \"false\" or %q", n.ID, HandleFailBranch)
			}
			if cs.Logic != "and" && cs.Logic != "or" {
				return fmt.Errorf("ir: switch %q case %q: logical_operator must be and or or", n.ID, cs.ID)
			}
			for _, cd := range cs.Conds {
				if _, ok := d.Input[cd.Var]; !ok {
					return fmt.Errorf("ir: switch %q case %q: %q is not an input of the step", n.ID, cs.ID, cd.Var)
				}
				if !slices.Contains(switchOps, cd.Op) {
					return fmt.Errorf("ir: switch %q: unknown operator %q", n.ID, cd.Op)
				}
				if s := cd.Sub; s != nil {
					if s.Logic != "and" && s.Logic != "or" {
						return fmt.Errorf("ir: switch %q: sub logical_operator must be and or or", n.ID)
					}
					for _, sc := range s.Conds {
						if !slices.Contains(fileAttrs, sc.Key) || !slices.Contains(switchOps, sc.Op) {
							return fmt.Errorf("ir: switch %q: bad file condition %q %q", n.ID, sc.Key, sc.Op)
						}
					}
				}
			}
			hs = append(hs, cs.ID)
		}
		n.Switch = &sw
		hs = append(hs, "false")
		if len(d.Handles) > 0 && !sameSet(d.Handles, hs) {
			return fmt.Errorf("ir: switch %q: handles %v do not match its cases %v", n.ID, d.Handles, hs)
		}
		n.Handles = hs
		return nil
	}
	if spec.Branch == "" {
		if len(d.Handles) > 0 {
			return fmt.Errorf("ir: step %q: handles given, but action %q does not branch", n.ID, d.Action)
		}
		return nil
	}
	ft, ok := spec.Outputs[spec.Branch]
	if !ok || ft.Type != FieldEnum {
		return fmt.Errorf("ir: step %q: action %q branches on %q, which is not a declared enum output", n.ID, d.Action, spec.Branch)
	}
	hs := ft.Values
	if len(d.Handles) > 0 {
		hs = d.Handles
	}
	if len(hs) == 0 {
		return fmt.Errorf("ir: step %q: action %q branches but no handles are declared", n.ID, d.Action)
	}
	for k, h := range hs {
		if h == "" || slices.Contains(hs[:k], h) || h == HandleFailBranch {
			return fmt.Errorf("ir: step %q: bad or duplicate handle %q", n.ID, h)
		}
	}
	n.Handles = slices.Clone(hs)
	return nil
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}
