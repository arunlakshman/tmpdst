package controlplane

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// Import rules for the control plane. One `go list` call gives every package and
// the packages it imports directly; check reads that list and returns one message
// for every broken rule. TestImportRulesCanFail feeds check a graph with one bad
// import and expects the message, so each rule is shown to be able to fail.

const (
	module = "github.com/manifesterio/mds/controlplane/"
	dst    = "github.com/manifesterio/mds/dst"
	cp     = "internal/controlplane/"
	rt     = "internal/runtime"
	spec   = "internal/spec"
	infra  = "internal/testinfra"
)

type pkg struct {
	dir     string   // module-relative, for example internal/controlplane/metadb/engine
	files   []string // non-test .go files
	imports []string // direct imports of the non-test files
	tests   []string // direct imports of the test files
	deps    []string // every package the non-test files need, directly or not
}

// load lists every package of the module with go list. It uses the import path, not ./...,
// because the test runs in a sub-folder and ./... would miss the rest of the module.
// The integration tag is on so that files behind it are seen as well.
func load(t *testing.T) map[string]pkg {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "list", "-tags", "integration", "-f",
		`{{.ImportPath}}|{{.Dir}}|{{join .GoFiles ","}}|{{join .Imports ","}}|{{join .TestImports ","}} {{join .XTestImports ","}}|{{join .Deps ","}}`, module+"...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	pkgs := map[string]pkg{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "|")
		p := pkg{dir: strings.TrimPrefix(f[0], module)}
		for name := range strings.SplitSeq(f[2], ",") {
			if name != "" {
				p.files = append(p.files, filepath.Join(f[1], name))
			}
		}
		p.imports = strings.Split(f[3], ",")
		p.tests = strings.Fields(strings.ReplaceAll(f[4], ",", " "))
		p.deps = strings.Split(f[5], ",")
		pkgs[p.dir] = p
	}
	return pkgs
}

// local turns a package path into a module-relative one ("" if it is not in the module).
func local(path string) string {
	if rel, ok := strings.CutPrefix(path, module); ok {
		return rel
	}
	return ""
}

// thirdParty tells whether a path is a package of another module (not std, not dst).
func thirdParty(path string) bool {
	return local(path) == "" && strings.Contains(strings.Split(path, "/")[0], ".") && !strings.HasPrefix(path, dst)
}

// The domain packages. Each is one bounded piece of the control plane, and the
// test fails if the folder holds one that is not listed here.
var domains = []string{cp + "domain/broker", cp + "domain/group", cp + "domain/rebalance", cp + "domain/topic"}

// domainEdges are the imports between domain packages. There are none: group needs the
// topic only through a function that metadb gives it when it wires the two together.
var domainEdges = map[string][]string{}

// The verification side is test infrastructure, so it may import the reference model
// (internal/spec). "Outside tests" in the rule means: no non-test file of any other
// package, production or not, imports it. Test files may import both packages.
// internal/testinfra is for test files and for the integration tests only.
var (
	specImporters  = []string{"internal/verify/simulator", "cmd/cpsim", "test/conformance"}
	infraImporters = []string{"test/integration"}
)

// problems collects the messages of broken rules.
type problems []string

func (p *problems) add(format string, args ...any) { *p = append(*p, fmt.Sprintf(format, args...)) }

// onlyImports reports every import of dir that is outside allowed. Third-party packages
// are refused too when strict is set.
func (p *problems) onlyImports(pkgs map[string]pkg, dir string, allowed []string, strict bool) {
	for _, imp := range pkgs[dir].imports {
		rel := local(imp)
		switch {
		case rel != "" && !slices.Contains(allowed, rel):
			p.add("%s imports %s: it may import only %v", dir, rel, allowed)
		case strict && thirdParty(imp):
			p.add("%s imports %s: a third-party package; put that code in an adapter", dir, imp)
		}
	}
}

// check returns one message for every rule that the package graph breaks.
func check(pkgs map[string]pkg) problems {
	var p problems

	// (a) The domain packages depend inward only: contract, runtime and the engine
	// interface, and no third-party package.
	for dir := range pkgs {
		if strings.HasPrefix(dir, cp+"domain/") && !slices.Contains(domains, dir) {
			p.add("%s is a domain package that the architecture test does not list", dir)
		}
	}
	for _, dir := range domains {
		allowed := append([]string{cp + "contract", cp + "metadb/engine", rt}, domainEdges[dir]...)
		p.onlyImports(pkgs, dir, allowed, true)
	}
	// The other inner packages.
	layers := map[string][]string{
		cp + "contract":      {rt},
		cp + "metadb/engine": {cp + "contract", rt},
		cp + "replica":       {cp + "contract", cp + "metadb/engine", cp + "metadb", rt},
		cp + "executor":      {rt},
	}
	for dir, allowed := range layers {
		p.onlyImports(pkgs, dir, allowed, true)
	}

	// (b) metadb wires the topic and group contexts together and owns the log entry format,
	// which is protobuf (the log and shared messages). Its only third-party packages are two of
	// the protobuf runtime, audited in docs/dep-audits/google.golang.org/protobuf@<version>.md.
	// It does not know the group protocol machine (rebalance), nor the broker.
	metadb := cp + "metadb"
	protobuf := []string{"google.golang.org/protobuf/proto", "google.golang.org/protobuf/reflect/protoreflect"}
	p.onlyImports(pkgs, metadb, []string{"api/controlplane/log/v1", "api/controlplane/shared/v1", cp + "contract",
		cp + "domain/group", cp + "domain/topic", cp + "metadb/engine", cp + "metadb/engine/memory", rt}, false)
	for _, imp := range pkgs[metadb].imports {
		if thirdParty(imp) && !slices.Contains(protobuf, imp) {
			p.add("%s imports %s: the only third-party packages it may use are %v", metadb, imp, protobuf)
		}
		if rel := local(imp); rel == cp+"domain/rebalance" || rel == cp+"domain/broker" {
			p.add("%s imports %s: not allowed", metadb, rel)
		}
	}
	for _, want := range []string{cp + "domain/topic", cp + "domain/group"} {
		if !slices.Contains(pkgs[metadb].imports, module+want) {
			p.add("%s does not import %s: it must, to wire the two together", metadb, want)
		}
	}

	// (c) The reference model stands alone: it takes no domain package, no metadb and no engine,
	// directly or through another package. Contract and runtime are fine.
	for _, imp := range append(slices.Clone(pkgs[spec].deps), pkgs[spec].tests...) {
		if rel := local(imp); strings.HasPrefix(rel, cp+"domain/") || rel == metadb || strings.HasPrefix(rel, metadb+"/") {
			p.add("%s imports %s: the reference model must not depend on it", spec, rel)
		}
	}

	// (d) Nothing outside tests imports internal/spec or internal/testinfra: only the verifiers
	// listed above do, in their non-test files.
	for dir, pk := range pkgs {
		for _, imp := range pk.imports {
			switch rel := local(imp); {
			case rel == spec && !slices.Contains(specImporters, dir):
				p.add("%s imports %s: only %v and test files may", dir, rel, specImporters)
			case rel == infra && !slices.Contains(infraImporters, dir):
				p.add("%s imports %s: only %v and test files may", dir, rel, infraImporters)
			}
		}
	}

	// live is the production runner: it knows no control-plane type.
	p.onlyImports(pkgs, "internal/system/live", nil, true)
	// The log adapters import the port and little else of the module.
	for _, a := range []string{"dynamolog", "sqllog"} {
		p.onlyImports(pkgs, cp+"executor/"+a, []string{cp + "executor", rt, "internal/system/realclock"}, false)
	}
	// The log entries and the shared messages do not pull in grpc: only the service package does.
	// Every api package imports the options of protoc-gen-mdswire, which import no package of the module.
	p.onlyImports(pkgs, "api/controlplane/log/v1", []string{"api/controlplane/shared/v1", "api/controlplane/options/v1"}, false)
	p.onlyImports(pkgs, "api/controlplane/options/v1", nil, false)
	for _, dir := range []string{"api/controlplane/log/v1", "api/controlplane/shared/v1"} {
		for _, imp := range pkgs[dir].imports {
			if strings.HasPrefix(imp, "google.golang.org/grpc") {
				p.add("%s imports %s: only the service package may use grpc", dir, imp)
			}
		}
	}
	// runtime never imports contract, and neither imports the wire packages (tests included).
	for _, dir := range []string{rt, cp + "contract"} {
		for _, imp := range append(slices.Clone(pkgs[dir].imports), pkgs[dir].tests...) {
			if rel := local(imp); strings.HasPrefix(rel, "api/") || rel == cp+"wire" || (dir == rt && rel == cp+"contract") {
				p.add("%s imports %s: not allowed", dir, rel)
			}
		}
	}
	return p
}

func TestImportRules(t *testing.T) {
	for _, msg := range check(load(t)) {
		t.Error(msg)
	}
}

// with returns a copy of the graph in which dir also imports imp (a path without a dot in
// its first element is taken as module-relative); without returns one in which it no longer does.
func with(pkgs map[string]pkg, dir, imp string) map[string]pkg {
	if !strings.Contains(imp, ".") {
		imp = module + imp
	}
	out := maps.Clone(pkgs)
	p := out[dir]
	p.imports = append(slices.Clone(p.imports), imp)
	p.deps = append(slices.Clone(p.deps), imp)
	out[dir] = p
	return out
}

func without(pkgs map[string]pkg, dir, imp string) map[string]pkg {
	out := maps.Clone(pkgs)
	p := out[dir]
	p.imports = slices.DeleteFunc(slices.Clone(p.imports), func(s string) bool { return s == module+imp })
	out[dir] = p
	return out
}

// TestImportRulesCanFail shows that every rule of check can fail: for each case it breaks the
// real graph in one place and expects a message that names the importing package and the import.
func TestImportRulesCanFail(t *testing.T) {
	real := load(t)
	if got := check(real); len(got) > 0 {
		t.Fatalf("the real graph breaks a rule: %v", got)
	}
	cases := []struct {
		name  string
		graph map[string]pkg
		dir   string // must be in the message
		imp   string // must be in the message
	}{
		{"(a) domain imports metadb", with(real, cp+"domain/topic", cp+"metadb"), cp + "domain/topic", cp + "metadb"},
		{"(a) group imports topic", with(real, cp+"domain/group", cp+"domain/topic"), cp + "domain/group", cp + "domain/topic"},
		{"(a) domain imports a sibling", with(real, cp+"domain/broker", cp+"domain/rebalance"), cp + "domain/broker", cp + "domain/rebalance"},
		{"(a) domain imports third-party", with(real, cp+"domain/broker", "example.com/lib"), cp + "domain/broker", "example.com/lib"},
		{"(a) domain imports spec", with(real, cp+"domain/rebalance", spec), cp + "domain/rebalance", spec},
		{"(a) unlisted domain package", extra(real, cp+"domain/extra"), cp + "domain/extra", ""},
		{"(b) metadb imports rebalance", with(real, cp+"metadb", cp+"domain/rebalance"), cp + "metadb", cp + "domain/rebalance"},
		{"(b) metadb imports broker", with(real, cp+"metadb", cp+"domain/broker"), cp + "metadb", cp + "domain/broker"},
		{"(b) metadb stops importing topic", without(real, cp+"metadb", cp+"domain/topic"), cp + "metadb", cp + "domain/topic"},
		{"(b) metadb stops importing group", without(real, cp+"metadb", cp+"domain/group"), cp + "metadb", cp + "domain/group"},
		{"(c) spec imports a domain package", with(real, spec, cp+"domain/topic"), spec, cp + "domain/topic"},
		{"(c) spec imports metadb", with(real, spec, cp+"metadb"), spec, cp + "metadb"},
		{"(c) spec imports the engine", with(real, spec, cp+"metadb/engine"), spec, cp + "metadb/engine"},
		{"(d) the server imports spec", with(real, cp+"server", spec), cp + "server", spec},
		{"(d) the binary imports testinfra", with(real, "cmd/mdscontrolplane", infra), "cmd/mdscontrolplane", infra},
		{"(d) a system package imports testinfra", with(real, "internal/system/s3store", infra), "internal/system/s3store", infra},
		{"(d) the simulator imports testinfra", with(real, "internal/verify/simulator", infra), "internal/verify/simulator", infra},
		{"(d) the conformance suite imports testinfra", with(real, "test/conformance", infra), "test/conformance", infra},
		{"(d) runtime imports spec", with(real, rt, spec), rt, spec},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !slices.ContainsFunc(check(c.graph), func(m string) bool {
				return strings.Contains(m, c.dir) && strings.Contains(m, c.imp)
			}) {
				t.Errorf("check did not report %s -> %s", c.dir, c.imp)
			}
		})
	}
	// The verifiers on the list may import the model: check must stay quiet for them.
	for _, dir := range specImporters {
		if got := check(with(real, dir, spec)); len(got) > 0 {
			t.Errorf("%s may import %s, but check says %v", dir, spec, got)
		}
	}
}

// extra returns a copy of the graph with one more, empty package.
func extra(pkgs map[string]pkg, dir string) map[string]pkg {
	out := maps.Clone(pkgs)
	out[dir] = pkg{dir: dir}
	return out
}

// All time comes from machine.Clock: these packages never read the wall clock.
var wallClock = regexp.MustCompile(`\btime\.(Now|Since|Until|AfterFunc|After|NewTimer|NewTicker|Tick|Sleep)\(`)

func TestNoWallClockInTheDomain(t *testing.T) {
	pkgs := load(t)
	dirs := append([]string{cp + "metadb/engine", cp + "metadb", cp + "replica", cp + "server"}, domains...)
	for _, dir := range dirs {
		if len(pkgs[dir].files) == 0 {
			t.Errorf("%s has no files: the list of packages in this test is out of date", dir)
		}
		for _, file := range pkgs[dir].files {
			src, err := os.ReadFile(file) //nolint:gosec // G304: the path comes from go list
			if err != nil {
				t.Fatal(err)
			}
			if m := wallClock.Find(src); m != nil {
				t.Errorf("%s calls %s: take a machine.Clock instead", file, m)
			}
		}
	}
}
