package sim

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"manifester/internal/core"
)

// scanCore maps each marker method (isEvent, isAction, ...) to the core types that
// have it, so a new type without a trace-hash tag fails the test.
func scanCore(t *testing.T) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	names, err := filepath.Glob("../../core/*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]string{}
	for _, fname := range names {
		if strings.HasSuffix(fname, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, fname, nil, 0)
		if err != nil {
			t.Fatal(err)
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
	events := map[string]core.EventData{
		"NodeStarted": core.NodeStarted{}, "StopRequest": core.StopRequest{}, "ConnOpened": core.ConnOpened{},
		"BytesReceived": core.BytesReceived{}, "ConnClosed": core.ConnClosed{}, "SendDone": core.SendDone{},
		"ConnectDone": core.ConnectDone{}, "TimerFired": core.TimerFired{}, "ObjectPutDone": core.ObjectPutDone{},
		"ObjectGetDone": core.ObjectGetDone{}, "ObjectDeleteDone": core.ObjectDeleteDone{}, "ObjectListDone": core.ObjectListDone{},
		"ControlPlaneDone": core.ControlPlaneDone{}, "ControlPlaneDeliver": core.ControlPlaneDeliver{},
		"LogAppendDone": core.LogAppendDone{}, "LogReadDone": core.LogReadDone{},
		"LogGetEndDone": core.LogGetEndDone{}, "LogDeleteOldDone": core.LogDeleteOldDone{},
	}
	actions := map[string]core.Action{
		"SetTimer": core.SetTimer{}, "CancelTimer": core.CancelTimer{}, "Send": core.Send{},
		"CloseConn": core.CloseConn{}, "PauseReads": core.PauseReads{}, "ResumeReads": core.ResumeReads{},
		"Connect": core.Connect{}, "ObjectPut": core.ObjectPut{}, "ObjectGet": core.ObjectGet{}, "ObjectDelete": core.ObjectDelete{},
		"ObjectList": core.ObjectList{}, "ControlPlaneCall": core.ControlPlaneCall{}, "ControlPlaneReply": core.ControlPlaneReply{},
		"LogAppend": core.LogAppend{}, "LogRead": core.LogRead{}, "LogGetEnd": core.LogGetEnd{},
		"LogDeleteOld": core.LogDeleteOld{},
		"RecordFact":   core.RecordFact{}, "StopForRestart": core.StopForRestart{}, "ShutdownDone": core.ShutdownDone{},
	}
	ops := map[string]core.ControlPlaneOperation{
		"CommitSegment": core.CommitSegment{}, "FindSegments": core.FindSegments{},
		"DescribeTopics": core.DescribeTopics{},
	}
	resps := map[string]core.ControlPlaneResponse{
		"CommitSegmentResp": core.CommitSegmentResp{},
		"FindSegmentsResp":  core.FindSegmentsResp{}, "DescribeTopicsResp": core.DescribeTopicsResp{},
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
		if prev, dup := seen[h.Sum()]; dup {
			t.Errorf("%s and %s hash alike", name, prev)
		}
		seen[h.Sum()] = name
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

// Every core.EventData must also classify, or the inbox would panic mid-run.
func TestTaggedEventsClassify(t *testing.T) {
	for _, b := range []core.EventData{core.NodeStarted{}, core.StopRequest{}, core.ConnOpened{}, core.ConnectDone{}, core.ControlPlaneDone{}} {
		_ = core.InboxKindOf(b)
	}
}
