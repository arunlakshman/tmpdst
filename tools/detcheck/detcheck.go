// Command detcheck enforces the determinism rules of docs/lessons.md (lesson 12)
// on the packages that run inside the simulator (core/, sim/), and is reused by
// scripts/audit-dep.sh to audit third-party packages before they are allowed in.
//
// It reports:
//   - imports not on the exact-path allow-list (allow.txt, embedded);
//   - go statements, channel types, sends, receives and select statements;
//   - range over a map (Go randomizes map iteration order);
//   - reflect map iteration (Value.MapKeys / Value.MapRange);
//   - package-level variables (mutable global state leaks between sim runs),
//     except error sentinels and blank (_) assertions.
//
// A finding is suppressed by a comment on the same or the previous line:
//
//	//detcheck:ok <reason>
//
// The reason is mandatory. _test.go files are not checked.
package main

import (
	"bufio"
	_ "embed"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/singlechecker"
)

//go:embed allow.txt
var embeddedAllow string

var allowFile string

// Analyzer is the detcheck analysis pass.
var Analyzer = &analysis.Analyzer{
	Name: "detcheck",
	Doc:  "reports constructs that break deterministic simulation (see docs/lessons.md, lesson 12)",
	Run:  run,
}

func init() {
	Analyzer.Flags.StringVar(&allowFile, "allow", "", "import allow-list file (default: the embedded allow.txt)")
}

func main() { singlechecker.Main(Analyzer) }

type allowList struct {
	exact  map[string]bool
	prefix []string
}

func (a allowList) allows(path string) bool {
	if a.exact[path] {
		return true
	}
	for _, p := range a.prefix {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

func loadAllow() (allowList, error) {
	text := embeddedAllow
	if allowFile != "" {
		b, err := os.ReadFile(allowFile) //nolint:gosec // G304: path is an explicit CLI argument
		if err != nil {
			return allowList{}, err
		}
		text = string(b)
	}
	al := allowList{exact: map[string]bool{}}
	sc := bufio.NewScanner(strings.NewReader(text))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if p, ok := strings.CutSuffix(line, "/..."); ok {
			al.prefix = append(al.prefix, p)
		} else {
			al.exact[line] = true
		}
	}
	return al, sc.Err()
}

func run(pass *analysis.Pass) (any, error) {
	// The go-tool-generated test main (package path "x.test") imports os and
	// testing by construction; it is not our code.
	if strings.HasSuffix(pass.Pkg.Path(), ".test") {
		return nil, nil
	}
	al, err := loadAllow()
	if err != nil {
		return nil, err
	}
	for _, f := range pass.Files {
		if strings.HasSuffix(pass.Fset.File(f.Pos()).Name(), "_test.go") {
			continue
		}
		checkFile(pass, f, al, []fileCheck{checkImports, checkGlobals, checkAST})
	}
	return nil, nil
}

// reporter returns a report func that honours //detcheck:ok on the same or
// the previous line.
func reporter(pass *analysis.Pass, sup map[int]string) func(token.Pos, string, ...any) {
	return func(pos token.Pos, format string, args ...any) {
		line := pass.Fset.Position(pos).Line
		if reason, ok := sup[line]; ok && reason != "" {
			return
		}
		if reason, ok := sup[line-1]; ok && reason != "" {
			return
		}
		pass.Reportf(pos, format, args...)
	}
}

type reportFunc = func(token.Pos, string, ...any)

type fileCheck func(pass *analysis.Pass, f *ast.File, al allowList, report reportFunc)

func checkFile(pass *analysis.Pass, f *ast.File, al allowList, checks []fileCheck) {
	sup := suppressions(pass, f)
	report := reporter(pass, sup)
	for line, reason := range sup { // order irrelevant: the driver sorts diagnostics
		if reason == "" {
			pass.Reportf(pass.Fset.File(f.Pos()).LineStart(line), "detcheck:ok needs a reason")
		}
	}
	for _, check := range checks {
		check(pass, f, al, report)
	}
}

func checkImports(_ *analysis.Pass, f *ast.File, al allowList, report reportFunc) {
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if !al.allows(path) {
			report(imp.Pos(), "import %q is not on the deterministic allow-list", path)
		}
	}
}

func checkGlobals(pass *analysis.Pass, f *ast.File, _ allowList, report reportFunc) {
	errorIface := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			for _, id := range spec.(*ast.ValueSpec).Names {
				if id.Name == "_" {
					continue
				}
				obj := pass.TypesInfo.Defs[id]
				if obj != nil && types.Implements(obj.Type(), errorIface) {
					continue
				}
				report(id.Pos(), "package-level variable %s: mutable global state leaks between simulation runs", id.Name)
			}
		}
	}
}

func checkAST(pass *analysis.Pass, f *ast.File, _ allowList, report reportFunc) {
	ast.Inspect(f, func(n ast.Node) bool {
		checkNode(pass, n, report)
		return true
	})
}

func checkNode(pass *analysis.Pass, n ast.Node, report reportFunc) {
	switch n := n.(type) {
	case *ast.GoStmt:
		report(n.Pos(), "go statement: goroutine scheduling is nondeterministic")
	case *ast.SelectStmt:
		report(n.Pos(), "select statement: ready cases are chosen at random")
	case *ast.SendStmt:
		report(n.Pos(), "channel send")
	case *ast.UnaryExpr:
		if n.Op == token.ARROW {
			report(n.Pos(), "channel receive")
		}
	case *ast.ChanType:
		report(n.Pos(), "channel type")
	case *ast.RangeStmt:
		checkRange(pass, n, report)
	case *ast.CallExpr:
		checkReflectMap(pass, n, report)
	}
}

func checkRange(pass *analysis.Pass, n *ast.RangeStmt, report reportFunc) {
	switch pass.TypesInfo.TypeOf(n.X).Underlying().(type) {
	case *types.Map:
		report(n.Pos(), "range over map: iteration order is randomized; iterate sorted keys")
	case *types.Chan:
		report(n.Pos(), "range over channel")
	}
}

func checkReflectMap(pass *analysis.Pass, n *ast.CallExpr, report reportFunc) {
	sel, ok := n.Fun.(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "MapKeys" && sel.Sel.Name != "MapRange") {
		return
	}
	if t := pass.TypesInfo.TypeOf(sel.X); t != nil && t.String() == "reflect.Value" {
		report(n.Pos(), "reflect map iteration: order is randomized")
	}
}

// suppressions maps line -> reason for every //detcheck:ok comment in f.
func suppressions(pass *analysis.Pass, f *ast.File) map[int]string {
	out := map[int]string{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			text, ok := strings.CutPrefix(c.Text, "//detcheck:ok")
			if !ok {
				continue
			}
			out[pass.Fset.Position(c.Pos()).Line] = strings.TrimSpace(text)
		}
	}
	return out
}
