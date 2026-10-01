package sqlstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// Every SQL statement this package executes must be a field of the
// queries struct built by buildQueries (from the dialect and the validated
// prefix only), never a string assembled at the call site from values.
// That is what keeps SQL injection out (ADR 0020).
func TestOnlyPreparedQueriesAreExecuted(t *testing.T) {
	execs := map[string]bool{"ExecContext": true, "QueryContext": true, "QueryRowContext": true, "PrepareContext": true,
		"Exec": true, "Query": true, "QueryRow": true, "Prepare": true}
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !execs[sel.Sel.Name] {
				return true
			}
			idx := 0
			if strings.HasSuffix(sel.Sel.Name, "Context") {
				idx = 1
			}
			if len(call.Args) <= idx {
				return true
			}
			if !isQueryField(call.Args[idx]) {
				t.Errorf("%s: %s with a statement that is not a field of queries", fset.Position(call.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
}

// isQueryField accepts s.q.X, q.X, s.q.insert[n] and range variables over
// q.create (stmt).
func isQueryField(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.IndexExpr:
		return isQueryField(x.X)
	case *ast.SelectorExpr:
		switch y := x.X.(type) {
		case *ast.Ident:
			return y.Name == "q"
		case *ast.SelectorExpr:
			return y.Sel.Name == "q"
		}
	case *ast.Ident:
		return x.Name == "stmt" // for _, stmt := range q.create
	}
	return false
}
