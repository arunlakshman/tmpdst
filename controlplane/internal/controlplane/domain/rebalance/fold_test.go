package rebalance

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// inputSamples has one value of every Input case, with every field set.
func inputSamples() []Input {
	return []Input{
		Join{Call: 1, Group: "g", MemberID: "m", GroupInstanceID: "i", ClientID: "c", ClientHost: "h",
			SessionTimeoutMs: 2, RebalanceTimeoutMs: 3, ProtocolType: "consumer", Protocols: []Protocol{{Name: "range", Metadata: []byte("x")}}},
		Sync{Call: 1, Group: "g", MemberID: "m", Generation: 2, Assignments: []Assignment{{Member: "m", Bytes: []byte("a")}}},
		Heartbeat{Call: 1, Group: "g", MemberID: "m", Generation: 2},
		Leave{Call: 1, Group: "g", Members: []string{"a", "b"}},
		OffsetCommit{Call: 1, Op: contract.OffsetCommit{Group: "g"}},
		Describe{Call: 1, Groups: []string{"g"}},
		List{Call: 1},
		CallAbandoned{Call: 1},
		RoleChanged{Active: true},
		ReplicaReply{Req: 1, Done: runtime.ControlPlaneDone{Req: 1, Err: runtime.ErrIO}},
		TimerFired{ID: 1, Gen: 2},
		drainLater{group: "g"},
	}
}

// outputSamples has one value of every Output case, with every field set.
func outputSamples() []Output {
	return []Output{
		SubmitCommand{Req: 1, Op: contract.GroupRemove{Group: "g"}},
		SubmitRead{Req: 1, Op: contract.ListGroups{}, Fresh: true},
		Reply{Call: 1, Err: runtime.ErrIO, Body: SyncAnswer{Assignment: []byte("a")}},
		SetTimer{ID: 1, Gen: 2, After: 3},
		CancelTimer{ID: 1},
		SendSelf{Msg: drainLater{group: "g"}},
	}
}

// inputName and outputName have an exhaustive switch. The linter (gochecksumtype) reports a
// new case that is missing here, so the sample lists above can not fall behind.
func inputName(in Input) string {
	switch in.(type) {
	case Join:
		return "Join"
	case Sync:
		return "Sync"
	case Heartbeat:
		return "Heartbeat"
	case Leave:
		return "Leave"
	case OffsetCommit:
		return "OffsetCommit"
	case Describe:
		return "Describe"
	case List:
		return "List"
	case CallAbandoned:
		return "CallAbandoned"
	case RoleChanged:
		return "RoleChanged"
	case ReplicaReply:
		return "ReplicaReply"
	case TimerFired:
		return "TimerFired"
	case drainLater:
		return "drainLater"
	}
	return ""
}

func outputName(out Output) string {
	switch out.(type) {
	case SubmitCommand:
		return "SubmitCommand"
	case SubmitRead:
		return "SubmitRead"
	case Reply:
		return "Reply"
	case SetTimer:
		return "SetTimer"
	case CancelTimer:
		return "CancelTimer"
	case SendSelf:
		return "SendSelf"
	}
	return ""
}

func foldOf(m machine.Message) uint64 {
	h := trace.New()
	m.Fold(&h)
	return h.Sum()
}

// checkSamples checks the tags and the folds of a list of messages.
func checkSamples(t *testing.T, kind string, lo, hi uint64, msgs []interface {
	machine.Message
	Tag() uint64
}, names []string) {
	t.Helper()
	tags := map[uint64]string{}
	sums := map[uint64]string{}
	for i, m := range msgs {
		name := names[i]
		if name == "" {
			t.Fatalf("%T has no name in the name switch", m)
		}
		if m.Tag() < lo || m.Tag() > hi {
			t.Errorf("%s: tag %d is outside the %s range %d to %d", name, m.Tag(), kind, lo, hi)
		}
		if other, ok := tags[m.Tag()]; ok {
			t.Errorf("%s and %s share tag %d", name, other, m.Tag())
		}
		tags[m.Tag()] = name
		sum := foldOf(m)
		if sum != foldOf(m) {
			t.Errorf("%s: Fold is not repeatable", name)
		}
		if other, ok := sums[sum]; ok {
			t.Errorf("%s and %s fold to the same hash", name, other)
		}
		sums[sum] = name
	}
}

func TestInputTags(t *testing.T) {
	var msgs []interface {
		machine.Message
		Tag() uint64
	}
	var names []string
	for _, in := range inputSamples() {
		msgs = append(msgs, in)
		names = append(names, inputName(in))
	}
	if len(msgs) != 12 {
		t.Fatalf("%d input samples, want 12 (one per case)", len(msgs))
	}
	checkSamples(t, "input", 1001, 1099, msgs, names)
}

func TestOutputTags(t *testing.T) {
	var msgs []interface {
		machine.Message
		Tag() uint64
	}
	var names []string
	for _, out := range outputSamples() {
		msgs = append(msgs, out)
		names = append(names, outputName(out))
	}
	if len(msgs) != 6 {
		t.Fatalf("%d output samples, want 6 (one per case)", len(msgs))
	}
	checkSamples(t, "output", 1101, 1199, msgs, names)
}

// TestFoldSeesEveryAnswer checks that each kind of Reply body changes the hash.
func TestFoldSeesEveryAnswer(t *testing.T) {
	bodies := []Answer{
		nil,
		JoinAnswer{MemberID: "m", Members: []Member{{ID: "m", Metadata: []byte("x")}}},
		SyncAnswer{Assignment: []byte("a")},
		LeaveAnswer{Errs: []contract.ErrCode{contract.ErrIO}},
		CommitAnswer{OffsetCommitResp: contract.OffsetCommitResp{Errs: []contract.ErrCode{contract.ErrIO}}},
		DescribeAnswer{Groups: []GroupDescription{{Name: "g"}}},
		ListAnswer{Groups: []contract.GroupSummary{{Name: "g"}}},
	}
	seen := map[uint64]int{}
	for i, b := range bodies {
		sum := foldOf(Reply{Call: 1, Body: b})
		if j, ok := seen[sum]; ok {
			t.Errorf("answers %d and %d fold to the same hash", j, i)
		}
		seen[sum] = i
	}
}
