package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Invariant: the transition path performs no I/O, reads no clock, starts no
// goroutines and takes no locks. Checked statically on the package source.
func TestCoreIsPure(t *testing.T) {
	allowed := map[string]bool{
		"bytes": true, "encoding/json": true, "encoding/binary": true, "errors": true,
		"slices": true, "strconv": true, "strings": true, "time": true, "kairo/ir": true,
	}
	forbiddenCalls := map[string]bool{
		"time.Now": true, "time.Since": true, "time.Until": true, "time.Sleep": true,
		"time.After": true, "time.AfterFunc": true, "time.NewTimer": true, "time.NewTicker": true, "time.Tick": true,
	}
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
		for _, imp := range file.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if !allowed[p] {
				t.Errorf("%s imports %q: not allowed in the pure core", f, p)
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.GoStmt:
				t.Errorf("%s: go statement in the core", fset.Position(x.Pos()))
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && forbiddenCalls[id.Name+"."+x.Sel.Name] {
					t.Errorf("%s: %s.%s in the core", fset.Position(x.Pos()), id.Name, x.Sel.Name)
				}
			}
			return true
		})
	}
}
