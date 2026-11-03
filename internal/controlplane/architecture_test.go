package controlplane

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Import rules for the whole repo. The tests read only import lists, with
// go/parser and ImportsOnly. They do not load or build packages.

// skipDirs are directory names that the walk never enters.
var skipDirs = map[string]bool{
	".git":      true,
	"worktrees": true,
	".claude":   true,
	"vendor":    true,
	"testdata":  true,
}

// goFile is one .go file of the repo and the imports it lists.
type goFile struct {
	rel     string   // slash path from the module root, for example internal/core/doc.go
	dir     string   // slash dir from the module root, "." at the root
	isTest  bool     // true for *_test.go files
	imports []string // import paths as written in the file
}

// repo is the module root, its module path and every .go file in it.
type repo struct {
	root   string
	module string
	files  []goFile
}

// loadRepo walks up to go.mod, then parses the imports of every .go file.
func loadRepo(t *testing.T) repo {
	t.Helper()
	root := moduleRoot(t)
	r := repo{root: root, module: readModulePath(t, root)}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		f := goFile{
			rel:    filepath.ToSlash(rel),
			dir:    filepath.ToSlash(filepath.Dir(rel)),
			isTest: strings.HasSuffix(path, "_test.go"),
		}
		for _, spec := range parsed.Imports {
			p, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return fmt.Errorf("%s: bad import %s: %w", f.rel, spec.Path.Value, err)
			}
			f.imports = append(f.imports, p)
		}
		r.files = append(r.files, f)
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	return r
}

// moduleRoot returns the first directory above the test's directory that has go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// readModulePath returns the path on the module line of go.mod.
func readModulePath(t *testing.T, root string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "go.mod")) //nolint:gosec // G304: path is the module root from the test's own search
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatal("go.mod has no module line")
	return ""
}

// inModule reports whether imp is a package of this module. If it is, it
// returns the import path relative to the module root.
func inModule(imp, module string) (string, bool) {
	if imp == module {
		return ".", true
	}
	return strings.CutPrefix(imp, module+"/")
}

// underDir reports whether dir is prefix or a directory below it.
func underDir(dir, prefix string) bool {
	return dir == prefix || strings.HasPrefix(dir, prefix+"/")
}

// underAny reports whether dir is under one of the prefixes.
func underAny(dir string, prefixes []string) bool {
	for _, p := range prefixes {
		if underDir(dir, p) {
			return true
		}
	}
	return false
}

// TestControlPlaneTestPackageUsedOnlyByTests: the test-support package may be
// imported by test files, and by files in its own folder. Nothing else.
func TestControlPlaneTestPackageUsedOnlyByTests(t *testing.T) {
	r := loadRepo(t)
	const support = "internal/controlplane/controlplanetest"
	for _, f := range r.files {
		if f.isTest || underDir(f.dir, support) {
			continue
		}
		for _, imp := range f.imports {
			if strings.Contains(imp, "/controlplane/controlplanetest") {
				t.Errorf("%s imports %q: only test files, or files in %s, may import the test-support package", f.rel, imp, support)
			}
		}
	}
}

// TestWireBoundaryPackagesDoNotImportWire: core, broker and the simulator
// (and their sub-packages) must not import the wire packages. Test files count too.
func TestWireBoundaryPackagesDoNotImportWire(t *testing.T) {
	r := loadRepo(t)
	guarded := []string{"internal/core", "internal/broker", "internal/verify/simulator"}
	wire := []string{"internal/controlplane/api/v1", "internal/controlplane/wireboundary"}
	for _, f := range r.files {
		if !underAny(f.dir, guarded) {
			continue
		}
		for _, imp := range f.imports {
			rel, ok := inModule(imp, r.module)
			if ok && underAny(rel, wire) {
				t.Errorf("%s imports %q: packages under %v must not import the wire packages %v", f.rel, imp, guarded, wire)
			}
		}
	}
}

// TestRunLoopImportsOnlyCore: the run loop doc says it imports only core and
// the standard library. This test checks that rule for non-test files.
func TestRunLoopImportsOnlyCore(t *testing.T) {
	r := loadRepo(t)
	const dir = "internal/controlplane/runloop"
	const core = "internal/core"
	claim := "imports only " + r.module + "/" + core + " and stdlib"
	if doc := packageComment(t, r, dir); !strings.Contains(doc, claim) {
		t.Fatalf("the package comment of %s no longer says %q; update this rule to match", dir, claim)
	}
	for _, f := range r.files {
		if f.dir != dir || f.isTest {
			continue
		}
		for _, imp := range f.imports {
			rel, ok := inModule(imp, r.module)
			if ok {
				if !underDir(rel, core) {
					t.Errorf("%s imports %q: the run loop may import only %s and the standard library", f.rel, imp, core)
				}
				continue
			}
			// A standard library path has no dot in its first element.
			if strings.Contains(strings.Split(imp, "/")[0], ".") {
				t.Errorf("%s imports %q: the run loop may import only %s and the standard library", f.rel, imp, core)
			}
		}
	}
}

// packageComment returns the package doc comments of the non-test files in
// dir, joined into one string with single spaces.
func packageComment(t *testing.T, r repo, dir string) string {
	t.Helper()
	var parts []string
	for _, f := range r.files {
		if f.dir != dir || f.isTest {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(r.root, f.rel), nil, parser.ImportsOnly|parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", f.rel, err)
		}
		if parsed.Doc != nil {
			parts = append(parts, parsed.Doc.Text())
		}
	}
	return strings.Join(strings.Fields(strings.Join(parts, " ")), " ")
}

// TestReportControlPlaneImporters only logs. It never fails. It lists the
// packages under internal/controlplane that no other package imports, and
// the packages that only test files import.
func TestReportControlPlaneImporters(t *testing.T) {
	r := loadRepo(t)
	const prefix = "internal/controlplane"
	known := map[string]bool{}
	for _, f := range r.files {
		if underDir(f.dir, prefix) && !f.isTest {
			known[f.dir] = true
		}
	}
	// importers[target] holds the dirs that import it, from non-test files
	// (prod) and from test files (test).
	prod := map[string]map[string]bool{}
	test := map[string]map[string]bool{}
	for _, f := range r.files {
		for _, imp := range f.imports {
			rel, ok := inModule(imp, r.module)
			if !ok || rel == f.dir || !known[rel] {
				continue
			}
			set := prod
			if f.isTest {
				set = test
			}
			if set[rel] == nil {
				set[rel] = map[string]bool{}
			}
			set[rel][f.dir] = true
		}
	}
	dirs := make([]string, 0, len(known))
	for d := range known {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	for _, d := range dirs {
		switch {
		case len(prod[d]) == 0 && len(test[d]) == 0:
			t.Logf("report: %s is imported by no other package", d)
		case len(prod[d]) == 0:
			t.Logf("report: %s is imported only by test files", d)
		}
	}
}
