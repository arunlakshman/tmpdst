package sim

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"manifester/toys3/control"
)

// TestTelemetryHashKeyFrozen pins the hash encoding of telemetry. If this fails, a
// change altered every trace hash: see telemetryHashKey.
func TestTelemetryHashKeyFrozen(t *testing.T) {
	for _, c := range []struct {
		f    control.Telemetry
		want string
	}{
		{control.Committed{Client: 7, Key: "k1", Sum: 0xbeef}, `Committed{c7 "k1" crc=0000beef}`},
		{control.Committed{Client: 8, Key: "k2", Deleted: true}, `Committed{c8 delete "k2"}`},
		{control.Recovered{TempsRemoved: 3}, `Recovered{temps=3}`},
		{control.CorruptionDetected{Key: "k3"}, `CorruptObject{"k3"}`},
	} {
		if got := telemetryHashKey(c.f); got != c.want {
			t.Errorf("telemetryHashKey(%#v) = %s, want %s", c.f, got, c.want)
		}
	}
}

// TestTelemetryHashKeyCoversAllTypes checks that every Telemetry type in
// package control has an explicit case in telemetryHashKey, so the "?"
// fallback stays unreachable. The set of types is read from the source (every
// receiver of isTelemetry), so adding a telemetry type without a hash key
// fails here.
func TestTelemetryHashKeyCoversAllTypes(t *testing.T) {
	files, err := filepath.Glob("../control/*.go")
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "isTelemetry" {
				continue
			}
			if id, ok := fn.Recv.List[0].Type.(*ast.Ident); ok {
				declared[id.Name] = true
			}
		}
	}
	if len(declared) == 0 {
		t.Fatal("found no Telemetry types in ../control")
	}
	covered := map[string]bool{}
	for _, f := range []control.Telemetry{
		control.Committed{},
		control.Committed{Deleted: true},
		control.Recovered{},
		control.CorruptionDetected{},
	} {
		if got := telemetryHashKey(f); got == "?" {
			t.Errorf("telemetryHashKey(%#v) uses the fallback", f)
		}
		covered[strings.TrimPrefix(fmt.Sprintf("%T", f), "control.")] = true
	}
	for name := range declared {
		if !covered[name] {
			t.Errorf("Telemetry type control.%s has no explicit case in telemetryHashKey (add one and a sample here)", name)
		}
	}
}
