package sim

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"manifester/core"
)

// scanCore maps each marker method (isEvent, isAction, ...) to the core types that
// have it, so a new type without a trace-hash tag fails the test.
func scanCore(t *testing.T) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../core", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, p := range pkgs {
		for fname, f := range p.Files {
			if strings.HasSuffix(fname, "_test.go") {
				continue
			}
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Recv == nil || len(fd.Recv.List) != 1 || !strings.HasPrefix(fd.Name.Name, "is") {
					continue
				}
				if id, ok := fd.Recv.List[0].Type.(*ast.Ident); ok {
					out[fd.Name.Name] = append(out[fd.Name.Name], id.Name)
				}
			}
		}
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

func names[T any](m map[string]T) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func eq(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

func TestEveryCoreTypeHasATag(t *testing.T) {
	events := map[string]core.EventBody{
		"Started": core.Started{}, "Stop": core.Stop{}, "ConnOpened": core.ConnOpened{},
		"BytesReceived": core.BytesReceived{}, "ConnClosed": core.ConnClosed{}, "SendDone": core.SendDone{},
		"DialDone": core.DialDone{}, "TimerFired": core.TimerFired{}, "ObjPutDone": core.ObjPutDone{},
		"ObjGetDone": core.ObjGetDone{}, "ObjDeleteDone": core.ObjDeleteDone{}, "ObjListDone": core.ObjListDone{},
		"CPDone": core.CPDone{},
	}
	actions := map[string]core.Action{
		"SetTimer": core.SetTimer{}, "CancelTimer": core.CancelTimer{}, "Send": core.Send{},
		"CloseConn": core.CloseConn{}, "PauseReads": core.PauseReads{}, "ResumeReads": core.ResumeReads{},
		"Dial": core.Dial{}, "ObjPut": core.ObjPut{}, "ObjGet": core.ObjGet{}, "ObjDelete": core.ObjDelete{},
		"ObjList": core.ObjList{}, "CPCall": core.CPCall{}, "Emit": core.Emit{}, "Trip": core.Trip{}, "Exit": core.Exit{},
	}
	ops := map[string]core.CPOp{
		"Register": core.Register{}, "CommitSegment": core.CommitSegment{}, "Lookup": core.Lookup{},
		"DescribeTopics": core.DescribeTopics{},
	}
	resps := map[string]core.CPResp{
		"RegisterResp": core.RegisterResp{}, "CommitSegmentResp": core.CommitSegmentResp{},
		"LookupResp": core.LookupResp{}, "DescribeTopicsResp": core.DescribeTopicsResp{},
	}
	fields := map[string]core.FieldValue{
		"I64": core.I64(0), "U64": core.U64(0), "Str": core.Str(""), "Bool": core.Bool(false), "Bytes": core.Bytes(nil),
	}
	src := scanCore(t)
	for marker, have := range map[string][]string{
		"isEvent": names(events), "isAction": names(actions), "isCPOp": names(ops),
		"isCPResp": names(resps), "isFieldValue": names(fields),
	} {
		if !eq(src[marker], have) {
			t.Errorf("%s: core declares %v, tag test lists %v", marker, src[marker], have)
		}
	}

	seen := map[uint64]string{}
	check := func(name string, fn func(h *hasher)) {
		defer func() {
			if x := recover(); x != nil {
				t.Errorf("%s has no tag: %v", name, x)
			}
		}()
		h := newHasher()
		fn(&h)
		if prev, dup := seen[h.sum]; dup {
			t.Errorf("%s and %s hash alike", name, prev)
		}
		seen[h.sum] = name
	}
	for n, v := range events {
		check("event "+n, func(h *hasher) { h.event(v) })
	}
	for n, v := range actions {
		check("action "+n, func(h *hasher) { h.action(v) })
	}
	for n, v := range ops {
		check("cpop "+n, func(h *hasher) { h.cpOp(v) })
	}
	for n, v := range resps {
		check("cpresp "+n, func(h *hasher) { h.cpResp(v) })
	}
	for n, v := range fields {
		check("field "+n, func(h *hasher) { h.fieldValue(v) })
	}
}

// Every core.EventBody must also classify, or the inbox would panic mid-run.
func TestTaggedEventsClassify(t *testing.T) {
	for _, b := range []core.EventBody{core.Started{}, core.Stop{}, core.ConnOpened{}, core.DialDone{}, core.CPDone{}} {
		_ = core.ClassOf(b)
	}
}
