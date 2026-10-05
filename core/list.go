package core

import (
	"encoding/json"
	"slices"
	"strings"

	"kairo/ir"
)

// kairo.list is Dify's list-operator (ADR 0028), ported from graphon 0.7.0
// (nodes/list_operator): filter, extract one element, order, limit, in
// that order, on a list of strings, numbers, booleans or files. The kind
// of list decides the filters, as graphon's segment type does. Params are
// the node's data plus "variable_input", the name of the input holding the
// list; condition values and the extract serial may contain {{#...#}}.
// Output: {"result", "first_record", "last_record"}.
func (m *machine) evalList(n *ir.Node, scope uint32) (json.RawMessage, error) {
	var p struct {
		VariableInput string `json:"variable_input"`
		FilterBy      struct {
			Enabled    bool `json:"enabled"`
			Conditions []struct {
				Key      string          `json:"key"`
				Operator string          `json:"comparison_operator"`
				Value    json.RawMessage `json:"value"`
			} `json:"conditions"`
		} `json:"filter_by"`
		OrderBy struct {
			Enabled bool   `json:"enabled"`
			Key     string `json:"key"`
			Value   string `json:"value"`
		} `json:"order_by"`
		Limit struct {
			Enabled bool `json:"enabled"`
			Size    *int `json:"size"`
		} `json:"limit"`
		ExtractBy struct {
			Enabled bool   `json:"enabled"`
			Serial  string `json:"serial"`
		} `json:"extract_by"`
	}
	if err := json.Unmarshal(n.Params, &p); err != nil {
		return nil, valueErr("kairo.list params: " + err.Error())
	}
	inputs := map[string]ir.Ref{}
	for _, in := range n.Inputs {
		inputs[in.Name] = in.Ref
	}
	raw, found := m.resolveFound(inputs[p.VariableInput], scope)
	if !found {
		return nil, &condError{"ListOperatorError", "Variable not found for selector: " + p.VariableInput}
	}
	v, err := pyDecode(raw)
	if err != nil {
		return nil, err
	}
	if !truthy(v) {
		return listOutput(nil), nil
	}
	list, ok := v.([]any)
	kind := listKind(list)
	if !ok || kind == "" {
		return nil, &condError{"ListOperatorError", "Variable " + p.VariableInput + " is not an array type"}
	}
	if p.FilterBy.Enabled {
		for _, c := range p.FilterBy.Conditions {
			cv, err := pyDecode(c.Value)
			if err != nil {
				cv = ""
			}
			if s, ok := cv.(string); ok {
				cv = m.template(s, inputs, scope)
			}
			keep, err := listFilter(kind, c.Key, c.Operator, cv)
			if err != nil {
				return nil, err
			}
			list = slices.DeleteFunc(slices.Clone(list), func(x any) bool { return !keep(x) })
		}
	}
	if p.ExtractBy.Enabled {
		serial := p.ExtractBy.Serial
		if serial == "" {
			serial = "1"
		}
		i, err := pyInt(m.template(serial, inputs, scope))
		if err != nil {
			return nil, err
		}
		if i < 1 {
			return nil, valueErr("Invalid serial index: must be >= 1")
		}
		if int(i) > len(list) {
			return nil, &condError{"InvalidKeyError", "Invalid serial index: out of range"}
		}
		list = []any{list[i-1]}
	}
	if p.OrderBy.Enabled {
		desc := p.OrderBy.Value == "desc"
		var key func(any) any
		if kind == "file" {
			if !slices.Contains([]string{"name", "type", "extension", "mime_type", "transfer_method", "url", "related_id", "size"}, p.OrderBy.Key) {
				return nil, &condError{"InvalidKeyError", "Invalid order key: " + p.OrderBy.Key}
			}
			key = func(x any) any { return fileAttr(x.(map[string]any), p.OrderBy.Key) }
		} else {
			key = func(x any) any { return x }
		}
		list = slices.Clone(list)
		slices.SortStableFunc(list, func(a, b any) int {
			c := pyCompare(key(a), key(b))
			if desc {
				return -c
			}
			return c
		})
	}
	if p.Limit.Enabled {
		size := -1
		if p.Limit.Size != nil {
			size = *p.Limit.Size
		}
		// Python's value[:size]: a negative size counts from the end.
		end := size
		if end < 0 {
			end = max(len(list)+size, 0)
		}
		list = list[:min(end, len(list))]
	}
	return listOutput(list), nil
}

func listOutput(list []any) json.RawMessage {
	out := map[string]any{"result": list, "first_record": nil, "last_record": nil}
	if list == nil {
		out["result"] = []any{}
	}
	if len(list) > 0 {
		out["first_record"], out["last_record"] = list[0], list[len(list)-1]
	}
	return pyEncode(nil, out)
}

// listKind is the segment type graphon infers for a list: string, number,
// boolean or file ("" for anything else).
func listKind(l []any) string {
	kind := ""
	for _, x := range l {
		k := ""
		switch y := x.(type) {
		case string:
			k = "string"
		case int64, float64:
			k = "number"
		case bool:
			k = "boolean"
		case map[string]any:
			if y["dify_model_identity"] == "__dify__file__" {
				k = "file"
			}
		}
		if k == "" || kind != "" && kind != k {
			return ""
		}
		kind = k
	}
	return kind
}

// listFilter returns the filter of one condition for a list of kind.
func listFilter(kind, key, op string, v any) (func(any) bool, error) {
	switch kind {
	case "string":
		s, ok := v.(string)
		if !ok {
			return nil, &condError{"InvalidFilterValueError", "Invalid filter value"}
		}
		f, err := stringFilter(op, s)
		if err != nil {
			return nil, err
		}
		return func(x any) bool { return f(x.(string)) }, nil
	case "number":
		s, ok := v.(string)
		if !ok {
			return nil, &condError{"InvalidFilterValueError", "Invalid filter value"}
		}
		want, err := pyFloat(s)
		if err != nil {
			return nil, err
		}
		cmp := map[string]func(float64) bool{
			"=": func(x float64) bool { return x == want }, "≠": func(x float64) bool { return x != want },
			"<": func(x float64) bool { return x < want }, "≤": func(x float64) bool { return x <= want },
			">": func(x float64) bool { return x > want }, "≥": func(x float64) bool { return x >= want },
		}[op]
		if cmp == nil {
			return nil, &condError{"InvalidConditionError", "Invalid condition: " + op}
		}
		return func(x any) bool { f, _ := pyNumber(x); return cmp(f) }, nil
	case "boolean":
		b, ok := v.(bool)
		if !ok {
			return nil, typeErr("Boolean filter expects a boolean value")
		}
		switch op {
		case "is":
			return func(x any) bool { return x == b }, nil
		case "is not":
			return func(x any) bool { return x != b }, nil
		}
		return nil, &condError{"InvalidConditionError", "Invalid condition: " + op}
	}
	// Files.
	if _, ok := v.(bool); ok {
		return nil, typeErr("File filter expects a string value")
	}
	attr := func(x any) any { return fileAttr(x.(map[string]any), key) }
	switch {
	case slices.Contains([]string{"name", "extension", "mime_type", "url", "related_id"}, key):
		s, ok := v.(string)
		if !ok {
			return nil, &condError{"InvalidKeyError", "Invalid key: " + key}
		}
		f, err := stringFilter(op, s)
		if err != nil {
			return nil, err
		}
		return func(x any) bool { a, _ := attr(x).(string); return f(a) }, nil
	case key == "type" || key == "transfer_method":
		var set []string
		switch y := v.(type) {
		case string:
			return listFilter("string", "", op, y) // in / not in on a string: substring
		case []any:
			for _, e := range y {
				if s, ok := e.(string); ok {
					set = append(set, s)
				}
			}
		}
		switch op {
		case "in":
			return func(x any) bool { a, _ := attr(x).(string); return slices.Contains(set, a) }, nil
		case "not in":
			return func(x any) bool { a, _ := attr(x).(string); return !slices.Contains(set, a) }, nil
		}
		return nil, &condError{"InvalidConditionError", "Invalid condition: " + op}
	case key == "size":
		s, ok := v.(string)
		if !ok {
			return nil, &condError{"InvalidKeyError", "Invalid key: " + key}
		}
		f, err := listFilter("number", "", op, s)
		if err != nil {
			return nil, err
		}
		return func(x any) bool { return f(attr(x)) }, nil
	}
	return nil, &condError{"InvalidKeyError", "Invalid key: " + key}
}

func stringFilter(op, v string) (func(string) bool, error) {
	switch op {
	case "empty":
		return func(x string) bool { return x == "" }, nil
	case "not empty":
		return func(x string) bool { return x != "" }, nil
	case "contains":
		return func(x string) bool { return strings.Contains(x, v) }, nil
	case "not contains":
		return func(x string) bool { return !strings.Contains(x, v) }, nil
	case "start with":
		return func(x string) bool { return strings.HasPrefix(x, v) }, nil
	case "end with":
		return func(x string) bool { return strings.HasSuffix(x, v) }, nil
	case "is":
		return func(x string) bool { return x == v }, nil
	case "is not":
		return func(x string) bool { return x != v }, nil
	case "in": // x in v, v a string: a substring of it
		return func(x string) bool { return strings.Contains(v, x) }, nil
	case "not in":
		return func(x string) bool { return !strings.Contains(v, x) }, nil
	}
	return nil, &condError{"InvalidConditionError", "Invalid condition: " + op}
}

// pyCompare orders values as Python's sorted does for one kind of list:
// numbers (and bools) by value, strings by code point.
func pyCompare(a, b any) int {
	if x, ok := pyNumber(a); ok {
		if y, ok := pyNumber(b); ok {
			switch {
			case x < y:
				return -1
			case x > y:
				return 1
			}
			return 0
		}
	}
	sa, _ := a.(string)
	sb, _ := b.(string)
	return strings.Compare(sa, sb)
}
