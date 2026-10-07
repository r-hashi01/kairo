package core

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"kairo/ir"
)

// kairo.switch (ADR 0031): Dify's if-else conditions, ported from graphon
// 0.7.0 (utils/condition/processor.py) with Python's semantics: truthiness
// ("not value"), int/float/bool coercions, equality where True == 1, and
// short-circuit evaluation (a later condition's errors are not raised).
// Values are decoded from JSON into nil, bool, int64, float64, string,
// []any and map[string]any; a JSON number is an int if it has no fraction
// or exponent, as Python's json module decides.

// condError is a failed evaluation; typ is the Python exception class
// graphon raises, reported as the step's error_type.
type condError struct{ typ, msg string }

func (e *condError) Error() string { return e.msg }

func valueErr(msg string) error { return &condError{"ValueError", msg} }
func typeErr(msg string) error  { return &condError{"TypeError", msg} }

// evalSwitch evaluates step n's cases and returns its output.
func (m *machine) evalSwitch(n *ir.Node, scope uint32) (json.RawMessage, error) {
	inputs := map[string]ir.Ref{}
	for _, in := range n.Inputs {
		inputs[in.Name] = in.Ref
	}
	handle, result := "false", false
	for _, cs := range n.Switch.Cases {
		ok, err := m.evalCase(cs, inputs, scope)
		if err != nil {
			return nil, err
		}
		if ok {
			handle, result = cs.ID, true
			break
		}
	}
	h, _ := json.Marshal(handle)
	b := []byte(`{"result":`)
	b = strconv.AppendBool(b, result)
	b = append(b, `,"selected_case_id":`...)
	b = append(b, h...)
	return append(b, '}'), nil
}

func (m *machine) evalCase(cs ir.SwitchCase, inputs map[string]ir.Ref, scope uint32) (bool, error) {
	and := cs.Logic == "and"
	var results []bool
	for _, cd := range cs.Conds {
		raw, found := m.resolveFound(inputs[cd.Var], scope)
		if !found {
			return false, valueErr("Variable " + cd.Var + " not found")
		}
		if IsBlobRef(raw) {
			// Its content is in the blob store, out of the core's reach.
			return false, valueErr("Variable " + cd.Var + " is too large to compare (stored as a blob)")
		}
		actual, err := pyDecode(raw)
		if err != nil {
			return false, err
		}
		var r bool
		switch {
		case cd.Sub != nil && isFileArray(actual) && (cd.Op == "contains" || cd.Op == "not contains" || cd.Op == "all of"):
			r, err = subConditions(actual.([]any), cd.Sub)
		case cd.Op == "exists" || cd.Op == "not exists":
			r, err = evalOp(cd.Op, actual, nil)
		default:
			var exp any
			exp, err = m.expected(cd.Value, actual, inputs, scope)
			if err == nil {
				r, err = evalOp(cd.Op, actual, exp)
			}
		}
		if err != nil {
			return false, err
		}
		results = append(results, r)
		if and && !r || !and && r {
			return r, nil
		}
	}
	if and {
		for _, r := range results {
			if !r {
				return false, nil
			}
		}
		return true, nil
	}
	for _, r := range results {
		if r {
			return true, nil
		}
	}
	return false, nil
}

// resolveFound resolves r, reporting whether the value exists at all (a
// skipped node, a missing field): graphon fails the condition then, while
// a present null is a value.
func (m *machine) resolveFound(r ir.Ref, scope uint32) (json.RawMessage, bool) {
	var v json.RawMessage
	switch r.Kind {
	case ir.RefNode:
		v = m.lookup(r.Node, scope)
	case ir.RefInput:
		v = m.s.Input
	case ir.RefItem:
		if sc := m.innermost(scope); sc != nil {
			v = sc.Item
		}
	case ir.RefIndex:
		return m.resolve(r, scope), true
	case ir.RefVar:
		v = m.runVarsScope(scope).Vals[runVarsNode]
	}
	if len(v) == 0 {
		return nil, false
	}
	// A node with ports is read by port (ADR 0043): its first segment may
	// index its list of ports, also when the engine kept it as a blob.
	// Other references walk objects only, as graphon's variable pool.
	ported := r.Kind == ir.RefNode && m.p.Nodes[r.Node].Ports
	for i, seg := range r.Path {
		t := bytes.TrimSpace(v)
		if ported && i == 0 && IsBlobRef(t) {
			x := extract(t, r.Path)
			return x, !bytes.Equal(bytes.TrimSpace(x), null)
		}
		if ported && i == 0 && len(t) > 0 && t[0] == '[' {
			var arr []json.RawMessage
			i, err := strconv.Atoi(seg)
			if err != nil || json.Unmarshal(t, &arr) != nil || i < 0 || i >= len(arr) {
				return nil, false
			}
			v = arr[i]
			continue
		}
		var obj map[string]json.RawMessage
		if len(t) == 0 || t[0] != '{' || json.Unmarshal(t, &obj) != nil {
			return nil, false
		}
		f, ok := obj[seg]
		if !ok {
			return nil, false
		}
		v = f
	}
	return v, true
}

// expected prepares a condition's expected value (_prepare_expected_value).
func (m *machine) expected(raw json.RawMessage, actual any, inputs map[string]ir.Ref, scope uint32) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	exp, err := pyDecode(raw)
	if err != nil {
		return nil, err
	}
	if s, ok := exp.(string); ok {
		exp = m.template(s, inputs, scope)
	}
	if exp == nil {
		return nil, nil
	}
	if isBoolish(actual) {
		if l, ok := exp.([]any); ok {
			out := make([]any, len(l))
			for i, x := range l {
				b, err := convertToBool(x)
				if err != nil {
					return nil, err
				}
				out[i] = b
			}
			return out, nil
		}
		return convertToBool(exp)
	}
	switch e := exp.(type) {
	case string, bool:
		return e, nil
	case []any:
		allStr, allBool := true, true
		for _, x := range e {
			_, s := x.(string)
			_, b := x.(bool)
			allStr, allBool = allStr && s, allBool && b
		}
		if allStr || allBool {
			return e, nil
		}
	}
	return nil, typeErr("unexpected expected value")
}

// template replaces {{#name#}} with the text of input name, as graphon's
// convert_template does (a name that resolves to nothing stays as its
// text, without the braces).
func (m *machine) template(s string, inputs map[string]ir.Ref, scope uint32) string {
	if !strings.Contains(s, "{{#") {
		return s
	}
	var b strings.Builder
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
		switch r, ok := inputs[name]; {
		case !validSelector(name):
			b.WriteString(s[i : i+3+j+3])
		case !ok:
			b.WriteString(name)
		default:
			raw, found := m.resolveFound(r, scope)
			v, err := pyDecode(raw)
			if !found || err != nil {
				b.WriteString(name)
			} else {
				b.WriteString(segText(v))
			}
		}
		s = s[i+3+j+3:]
	}
	b.WriteString(s)
	return b.String()
}

func isBoolish(v any) bool {
	switch x := v.(type) {
	case bool:
		return true
	case []any:
		if len(x) == 0 {
			return false
		}
		for _, e := range x {
			if _, ok := e.(bool); !ok {
				return false
			}
		}
		return true
	}
	return false
}

// convertToBool is _convert_to_bool: ints (and bools) by truth, strings by
// parsing them as JSON ints or bools.
func convertToBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case string:
		l, err := pyDecode(json.RawMessage(x))
		if err != nil {
			return nil, valueErr("invalid JSON: " + x)
		}
		switch y := l.(type) {
		case bool:
			return y, nil
		case int64:
			return y != 0, nil
		}
	}
	return nil, typeErr("unexpected value for a boolean condition")
}

func evalOp(op string, value, exp any) (bool, error) {
	switch op {
	case "contains":
		if !truthy(value) {
			return false, nil
		}
		switch v := value.(type) {
		case string:
			return strings.Contains(v, pyStrOf(exp)), nil
		case []any:
			return pyIn(exp, v), nil
		}
		return false, valueErr("Invalid actual value type: string or array")
	case "not contains":
		if !truthy(value) {
			return true, nil
		}
		switch v := value.(type) {
		case string:
			return !strings.Contains(v, pyStrOf(exp)), nil
		case []any:
			return !pyIn(exp, v), nil
		}
		return false, valueErr("Invalid actual value type: string or array")
	case "start with", "end with":
		if !truthy(value) {
			return false, nil
		}
		v, ok := value.(string)
		if !ok {
			return false, typeErr("Invalid actual value type: string")
		}
		e, ok := exp.(string)
		if !ok {
			return false, typeErr("Expected value must be a string")
		}
		if op == "start with" {
			return strings.HasPrefix(v, e), nil
		}
		return strings.HasSuffix(v, e), nil
	case "is", "is not":
		if value == nil {
			return false, nil
		}
		switch value.(type) {
		case string, bool:
			eq := pyEqual(value, exp)
			return eq == (op == "is"), nil
		}
		return false, valueErr("Invalid actual value type: string or boolean")
	case "empty":
		return !truthy(value), nil
	case "not empty":
		return truthy(value), nil
	case "=", "≠":
		if value == nil {
			return false, nil
		}
		e, err := numericEqualityExpected(value, exp)
		if err != nil {
			return false, err
		}
		return pyEqual(value, e) == (op == "="), nil
	case ">", "<", "≥", "≤":
		if value == nil {
			return false, nil
		}
		return compareNumbers(op, value, exp)
	case "null", "not exists":
		return value == nil, nil
	case "not null", "exists":
		return value != nil, nil
	case "in", "not in":
		if !truthy(value) {
			return op == "not in", nil
		}
		l, ok := exp.([]any)
		if !ok {
			return false, valueErr("Invalid expected value type: array")
		}
		return pyIn(value, l) == (op == "in"), nil
	case "all of":
		l, ok := exp.([]any)
		if !ok {
			return false, valueErr("all of operator expects homogeneous list of strings or booleans")
		}
		allStr, allBool := true, true
		for _, x := range l {
			_, s := x.(string)
			_, b := x.(bool)
			allStr, allBool = allStr && s, allBool && b
		}
		if !allStr && !allBool {
			return false, valueErr("all of operator expects homogeneous list of strings or booleans")
		}
		if !truthy(value) {
			return false, nil
		}
		switch v := value.(type) {
		case []any:
			for _, x := range l {
				if !pyIn(x, v) {
					return false, nil
				}
			}
			return true, nil
		case string:
			if !allStr {
				return false, nil
			}
			for _, x := range l {
				if !strings.Contains(v, x.(string)) {
					return false, nil
				}
			}
			return true, nil
		}
		return false, nil
	}
	return false, valueErr("Unsupported operator: " + op)
}

// numericEqualityExpected is _normalize_numeric_equality_expected.
func numericEqualityExpected(value, exp any) (any, error) {
	switch value.(type) {
	case bool:
		switch e := exp.(type) {
		case bool:
			return e, nil
		case int64:
			return e != 0, nil
		case string:
			return e != "", nil
		}
		return nil, valueErr("Cannot convert to bool")
	case int64:
		switch e := exp.(type) {
		case bool:
			if e {
				return int64(1), nil
			}
			return int64(0), nil
		case int64:
			return e, nil
		case float64:
			return int64(e), nil
		case string:
			i, err := pyInt(e)
			if err != nil {
				return nil, err
			}
			return i, nil
		}
		return nil, valueErr("Cannot convert to int")
	case float64:
		switch e := exp.(type) {
		case bool:
			if e {
				return 1.0, nil
			}
			return 0.0, nil
		case int64:
			return float64(e), nil
		case float64:
			return e, nil
		case string:
			f, err := pyFloat(e)
			if err != nil {
				return nil, err
			}
			return f, nil
		}
		return nil, valueErr("Cannot convert to float")
	}
	return nil, valueErr("Invalid actual value type: number or boolean")
}

// compareNumbers is the ordering operators with _normalize_numeric_values.
func compareNumbers(op string, value, exp any) (bool, error) {
	v, ok := pyNumber(value)
	if !ok {
		return false, valueErr("Invalid actual value type: number")
	}
	var e float64
	switch x := exp.(type) {
	case string:
		f, err := pyFloat(x)
		if err != nil {
			return false, valueErr("Cannot convert '" + x + "' to number")
		}
		e = f
	default:
		f, ok := pyNumber(x)
		if !ok {
			return false, valueErr("Cannot convert to number")
		}
		e = f
	}
	switch op {
	case ">":
		return v > e, nil
	case "<":
		return v < e, nil
	case "≥":
		return v >= e, nil
	}
	return v <= e, nil
}

// subConditions tests file attributes over a file array
// (_process_sub_conditions).
func subConditions(files []any, sub *ir.SubConditions) (bool, error) {
	var results []bool
	for _, sc := range sub.Conds {
		var exp any
		if len(bytes.TrimSpace(sc.Value)) > 0 {
			var err error
			if exp, err = pyDecode(sc.Value); err != nil {
				return false, err
			}
		}
		values := make([]any, len(files))
		for i, f := range files {
			values[i] = fileAttr(f.(map[string]any), sc.Key)
		}
		if sc.Key == "extension" {
			s, ok := exp.(string)
			if !ok {
				return false, typeErr("Expected value must be a string when key is FileAttribute.EXTENSION")
			}
			if s != "" && !strings.HasPrefix(s, ".") {
				exp = "." + s
			}
			for i, v := range values {
				if s, ok := v.(string); ok && s != "" && !strings.HasPrefix(s, ".") {
					values[i] = "." + s
				}
			}
		}
		not := strings.Contains(sc.Op, "not")
		r := not
		for _, v := range values {
			ok, err := evalOp(sc.Op, v, exp)
			if err != nil {
				return false, err
			}
			if not && !ok {
				r = false
				break
			}
			if !not && ok {
				r = true
				break
			}
		}
		results = append(results, r)
	}
	all := sub.Logic == "and"
	for _, r := range results {
		if all && !r {
			return false, nil
		}
		if !all && r {
			return true, nil
		}
	}
	return all, nil
}

// fileAttr reads an attribute of a serialized Dify file.
func fileAttr(f map[string]any, key string) any {
	switch key {
	case "name":
		return f["filename"]
	case "url":
		if u, ok := f["remote_url"]; ok && u != nil {
			return u
		}
		return f["url"]
	}
	return f[key]
}

func isFileArray(v any) bool {
	l, ok := v.([]any)
	if !ok || len(l) == 0 {
		return false
	}
	for _, x := range l {
		f, ok := x.(map[string]any)
		if !ok || f["dify_model_identity"] != "__dify__file__" {
			return false
		}
	}
	return true
}

// --- Python values ---------------------------------------------------------

func pyDecode(raw json.RawMessage) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, valueErr("invalid value: " + err.Error())
	}
	return pyNums(v), nil
}

func pyNums(v any) any {
	switch x := v.(type) {
	case json.Number:
		s := string(x)
		if !strings.ContainsAny(s, ".eE") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
		}
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case []any:
		for i := range x {
			x[i] = pyNums(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = pyNums(x[k])
		}
	}
	return v
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int64:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return true
}

// pyNumber is a number for ordering: bools are ints in Python.
func pyNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// pyEqual is Python's ==.
func pyEqual(a, b any) bool {
	if an, ok := pyNumber(a); ok {
		if bn, ok := pyNumber(b); ok {
			ai, aInt := a.(int64)
			bi, bInt := b.(int64)
			if aInt && bInt {
				return ai == bi
			}
			return an == bn
		}
		return false
	}
	switch x := a.(type) {
	case nil:
		return b == nil
	case string:
		y, ok := b.(string)
		return ok && x == y
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !pyEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !pyEqual(v, w) {
				return false
			}
		}
		return true
	}
	return false
}

func pyIn(x any, l []any) bool {
	for _, e := range l {
		if pyEqual(x, e) {
			return true
		}
	}
	return false
}

// pyStrOf is str(expected) for "contains" on a string.
func pyStrOf(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case []any:
		var b strings.Builder
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteString(", ")
			}
			if s, ok := e.(string); ok {
				b.WriteString(pyRepr(s))
			} else {
				b.WriteString(pyStrOf(e))
			}
		}
		b.WriteByte(']')
		return b.String()
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	}
	return ""
}

func pyRepr(s string) string {
	q := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		q = '"'
	}
	var b strings.Builder
	b.WriteByte(q)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\':
			b.WriteString(`\\`)
		case c == q:
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\t':
			b.WriteString(`\t`)
		case c == '\r':
			b.WriteString(`\r`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte(q)
	return b.String()
}

// pyInt is int(str).
func pyInt(s string) (int64, error) {
	t := strings.TrimSpace(s)
	t = underscores(t)
	i, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, valueErr("invalid literal for int() with base 10: " + pyRepr(s))
	}
	return i, nil
}

// pyFloat is float(str).
func pyFloat(s string) (float64, error) {
	t := underscores(strings.TrimSpace(s))
	f, err := strconv.ParseFloat(t, 64)
	if err != nil {
		return 0, valueErr("could not convert string to float: " + pyRepr(s))
	}
	return f, nil
}

// underscores drops single underscores between digits, as Python's int()
// and float() accept them; anything else is left to fail parsing.
func underscores(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '_' && (i == 0 || i == len(s)-1 || !isDigit(s[i-1]) || !isDigit(s[i+1])) {
			return s
		}
	}
	return strings.ReplaceAll(s, "_", "")
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
