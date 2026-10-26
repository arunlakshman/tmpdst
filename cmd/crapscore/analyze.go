package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Func is one analysed function or method.
type Func struct {
	Key        string  `json:"key"` // stable identity: dir.Recv.Name (no line numbers)
	Name       string  `json:"name"`
	File       string  `json:"file"` // slash path relative to the root
	Line       int     `json:"line"`
	EndLine    int     `json:"end_line"`
	Complexity int     `json:"complexity"`
	Coverage   float64 `json:"coverage"` // 0..1
	CRAP       float64 `json:"crap"`
}

// CRAP is the Savoia/Evans formula: comp^2 * (1-cov)^3 + comp.
func CRAP(comp int, cov float64) float64 {
	c := float64(comp)
	return c*c*(1-cov)*(1-cov)*(1-cov) + c
}

// Complexity is cyclomatic complexity: 1 + every if, for, range, non-default
// case clause, non-default select comm clause, && and ||. Function literals
// nested in the body are counted into the enclosing function. This is the
// gocyclo rule except that default clauses are not counted.
func Complexity(fn ast.Node) int {
	n := 1
	ast.Inspect(fn, func(x ast.Node) bool {
		switch v := x.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt:
			n++
		case *ast.CaseClause:
			if v.List != nil {
				n++
			}
		case *ast.CommClause:
			if v.Comm != nil {
				n++
			}
		case *ast.BinaryExpr:
			if v.Op == token.LAND || v.Op == token.LOR {
				n++
			}
		}
		return true
	})
	return n
}

func isGenerated(src []byte) bool {
	for line := range strings.SplitSeq(string(src), "\n") {
		if strings.HasPrefix(line, "package ") {
			return false
		}
		if strings.HasPrefix(line, "// Code generated") && strings.HasSuffix(line, "DO NOT EDIT.") {
			return true
		}
	}
	return false
}

// recvName renders the receiver type without pointer or type parameters.
func recvName(d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return ""
	}
	return typeName(d.Recv.List[0].Type)
}

func typeName(t ast.Expr) string {
	switch v := t.(type) {
	case *ast.StarExpr:
		return typeName(v.X)
	case *ast.IndexExpr:
		return typeName(v.X)
	case *ast.IndexListExpr:
		return typeName(v.X)
	case *ast.Ident:
		return v.Name
	}
	return "?"
}

// ParseTree parses every non-test, non-generated .go file under root. It skips
// hidden, underscore, vendor and testdata directories and nested modules.
func ParseTree(root string) ([]*Func, error) {
	var out []*Func
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipTreeDir(root, p, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		return collectGo(fset, root, p, d.Name(), &out)
	})
	if err != nil {
		return nil, err
	}
	sortFuncs(out)
	disambiguateKeys(out)
	return out, nil
}

func skipTreeDir(root, path, name string) bool {
	if path == root {
		return false
	}
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
		return true
	}
	if name == "vendor" || name == "testdata" {
		return true
	}
	_, err := os.Stat(filepath.Join(path, "go.mod"))
	return err == nil
}

func collectGo(fset *token.FileSet, root, path, name string, out *[]*Func) error {
	if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
		return nil
	}
	src, err := os.ReadFile(path) //nolint:gosec // G304: path is a file under the explicit CLI root
	if err != nil {
		return err
	}
	if isGenerated(src) {
		return nil
	}
	rel, _ := filepath.Rel(root, path)
	rel = filepath.ToSlash(rel)
	f, err := parser.ParseFile(fset, rel, src, 0)
	if err != nil {
		return fmt.Errorf("parse %s: %w", rel, err)
	}
	dir := filepath.ToSlash(filepath.Dir(rel))
	for _, decl := range f.Decls {
		d, ok := decl.(*ast.FuncDecl)
		if !ok || d.Body == nil {
			continue
		}
		nm := d.Name.Name
		if r := recvName(d); r != "" {
			nm = r + "." + nm
		}
		*out = append(*out, &Func{
			Key:        dir + "." + nm,
			Name:       nm,
			File:       rel,
			Line:       fset.Position(d.Pos()).Line,
			EndLine:    fset.Position(d.End()).Line,
			Complexity: Complexity(d),
		})
	}
	return nil
}

func sortFuncs(out []*Func) {
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
}

// disambiguateKeys numbers repeated keys (several init functions, "_" methods).
func disambiguateKeys(out []*Func) {
	seen := map[string]int{}
	for _, f := range out {
		seen[f.Key]++
		if c := seen[f.Key]; c > 1 {
			f.Key = fmt.Sprintf("%s#%d", f.Key, c)
		}
	}
}
