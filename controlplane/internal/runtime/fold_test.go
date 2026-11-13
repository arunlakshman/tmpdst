package runtime

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// eventSamples has one value of every EventData case, with every field set.
func eventSamples() []EventData {
	self := machine.NodeInstance{Node: "cp-0", Inc: machine.BootNumber{1}}
	op := contract.ListGroups{}
	return []EventData{
		NodeStarted{Self: self},
		StopRequest{Deadline: 5},
		TimerFired{ID: 2, Gen: 3},
		ObjectPutDone{Req: 1, Key: "k", Err: ErrIO, Outcome: MaybeApplied},
		ObjectGetDone{Req: 1, Data: []byte("d"), Err: ErrNotFound},
		ObjectDeleteDone{Req: 1, Err: ErrIO, Outcome: Applied},
		ObjectListDone{Req: 1, Keys: []string{"a", "b"}, Err: OK},
		ControlPlaneDone{Req: 1, Resp: contract.ListGroupsResp{}, Err: OK, Outcome: Applied},
		ControlPlaneDeliver{From: self, Req: 2, Op: op},
		LocalRead{From: self, Req: 3, Op: op},
		LogAppendDone{Req: 1, Slot: 2, Taken: true, Existing: []byte("e"), Err: OK, Outcome: Applied},
		LogReadDone{Req: 1, Entries: []LogEntry{{Slot: 1, Data: []byte("x")}}, Err: OK, Outcome: Applied},
		LogGetEndDone{Req: 1, Tail: 9, Err: OK, Outcome: Applied},
		LogDeleteOldDone{Req: 1, Err: OK, Outcome: Applied},
		LogClaimEpochDone{Req: 1, Epoch: 4, Owner: "o", Won: true, Err: OK, Outcome: Applied},
	}
}

// actionSamples has one value of every Action case, with every field set.
func actionSamples() []Action {
	self := machine.NodeInstance{Node: "cp-0", Inc: machine.BootNumber{1}}
	return []Action{
		SetTimer{ID: 1, Gen: 2, After: 3},
		CancelTimer{ID: 1},
		StopForRestart{Cause: "c"},
		ShutdownDone{},
		ObjectPut{Req: 1, Key: "k", Data: []byte("d")},
		ObjectGet{Req: 1, Ref: SegmentRef{Key: "k", Off: 1, Len: 2}},
		ObjectDelete{Req: 1, Key: "k"},
		ObjectList{Req: 1, Prefix: "p"},
		ControlPlaneCall{Req: 1, Op: contract.ListGroups{}},
		ControlPlaneReply{To: self, Done: ControlPlaneDone{Req: 1, Err: OK}},
		LogAppend{Req: 1, Slot: 2, Data: []byte("d")},
		LogRead{Req: 1, From: 2, Limit: 3},
		LogGetEnd{Req: 1},
		LogDeleteOld{Req: 1, Through: 2},
		LogClaimEpoch{Req: 1, Expect: 2, Owner: "o"},
	}
}

// eventName and actionName have an exhaustive switch. The linter (gochecksumtype) reports
// a new case that is missing here, so the sample lists above can not fall behind.
func eventName(e EventData) string {
	switch e.(type) {
	case NodeStarted:
		return "NodeStarted"
	case StopRequest:
		return "StopRequest"
	case TimerFired:
		return "TimerFired"
	case ObjectPutDone:
		return "ObjectPutDone"
	case ObjectGetDone:
		return "ObjectGetDone"
	case ObjectDeleteDone:
		return "ObjectDeleteDone"
	case ObjectListDone:
		return "ObjectListDone"
	case ControlPlaneDone:
		return "ControlPlaneDone"
	case ControlPlaneDeliver:
		return "ControlPlaneDeliver"
	case LocalRead:
		return "LocalRead"
	case LogAppendDone:
		return "LogAppendDone"
	case LogReadDone:
		return "LogReadDone"
	case LogGetEndDone:
		return "LogGetEndDone"
	case LogDeleteOldDone:
		return "LogDeleteOldDone"
	case LogClaimEpochDone:
		return "LogClaimEpochDone"
	}
	return ""
}

func actionName(a Action) string {
	switch a.(type) {
	case SetTimer:
		return "SetTimer"
	case CancelTimer:
		return "CancelTimer"
	case StopForRestart:
		return "StopForRestart"
	case ShutdownDone:
		return "ShutdownDone"
	case ObjectPut:
		return "ObjectPut"
	case ObjectGet:
		return "ObjectGet"
	case ObjectDelete:
		return "ObjectDelete"
	case ObjectList:
		return "ObjectList"
	case ControlPlaneCall:
		return "ControlPlaneCall"
	case ControlPlaneReply:
		return "ControlPlaneReply"
	case LogAppend:
		return "LogAppend"
	case LogRead:
		return "LogRead"
	case LogGetEnd:
		return "LogGetEnd"
	case LogDeleteOld:
		return "LogDeleteOld"
	case LogClaimEpoch:
		return "LogClaimEpoch"
	}
	return ""
}

func foldOf(m machine.Message) uint64 {
	h := trace.New()
	m.Fold(&h)
	return h.Sum()
}

// TestEventTags checks that every event has its own tag in the event range, and that a
// value folds the same way twice and differently from every other case.
func TestEventTags(t *testing.T) {
	tags := map[uint64]string{}
	sums := map[uint64]string{}
	for _, e := range eventSamples() {
		name := eventName(e)
		if name == "" {
			t.Fatalf("%T has no name in eventName", e)
		}
		if tag := e.Tag(); tag < 1 || tag > 99 {
			t.Errorf("%s: tag %d is outside the event range 1 to 99", name, tag)
		}
		if other, ok := tags[e.Tag()]; ok {
			t.Errorf("%s and %s share tag %d", name, other, e.Tag())
		}
		tags[e.Tag()] = name
		sum := foldOf(e)
		if sum != foldOf(e) {
			t.Errorf("%s: Fold is not repeatable", name)
		}
		if other, ok := sums[sum]; ok {
			t.Errorf("%s and %s fold to the same hash", name, other)
		}
		sums[sum] = name
	}
	if len(tags) != 15 {
		t.Errorf("events: %d samples, want 15 (one per case)", len(tags))
	}
}

// TestActionTags is TestEventTags for the actions.
func TestActionTags(t *testing.T) {
	tags := map[uint64]string{}
	sums := map[uint64]string{}
	for _, a := range actionSamples() {
		name := actionName(a)
		if name == "" {
			t.Fatalf("%T has no name in actionName", a)
		}
		if tag := a.Tag(); tag < 101 || tag > 199 {
			t.Errorf("%s: tag %d is outside the action range 101 to 199", name, tag)
		}
		if other, ok := tags[a.Tag()]; ok {
			t.Errorf("%s and %s share tag %d", name, other, a.Tag())
		}
		tags[a.Tag()] = name
		sum := foldOf(a)
		if sum != foldOf(a) {
			t.Errorf("%s: Fold is not repeatable", name)
		}
		if other, ok := sums[sum]; ok {
			t.Errorf("%s and %s fold to the same hash", name, other)
		}
		sums[sum] = name
	}
	if len(tags) != 15 {
		t.Errorf("actions: %d samples, want 15 (one per case)", len(tags))
	}
}

// TestFoldSeesEveryField changes one field at a time and expects a new hash.
func TestFoldSeesEveryField(t *testing.T) {
	pairs := []struct {
		name string
		a, b machine.Message
	}{
		{"SetTimer.Gen", SetTimer{ID: 1, Gen: 2, After: 3}, SetTimer{ID: 1, Gen: 9, After: 3}},
		{"SetTimer.After", SetTimer{ID: 1, Gen: 2, After: 3}, SetTimer{ID: 1, Gen: 2, After: 9}},
		{"LogAppend.Data", LogAppend{Req: 1, Slot: 2, Data: []byte("a")}, LogAppend{Req: 1, Slot: 2, Data: []byte("b")}},
		{"LogClaimEpochDone.Won", LogClaimEpochDone{Epoch: 1, Won: true}, LogClaimEpochDone{Epoch: 1}},
		{"ControlPlaneDone.Resp", ControlPlaneDone{Req: 1}, ControlPlaneDone{Req: 1, Resp: contract.ListGroupsResp{}}},
		{"ControlPlaneReply.To", ControlPlaneReply{To: machine.NodeInstance{Node: "a"}}, ControlPlaneReply{To: machine.NodeInstance{Node: "b"}}},
		{"LocalRead.Op", LocalRead{Req: 1}, LocalRead{Req: 1, Op: contract.ListGroups{}}},
	}
	for _, p := range pairs {
		if foldOf(p.a) == foldOf(p.b) {
			t.Errorf("%s: the field is not folded", p.name)
		}
	}
}
