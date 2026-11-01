package statemachine_test

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
	"mds/internal/verify/speccheck"
)

var lockstepCmds = []stateupdater.Entry{
	{ID: "0", Kind: stateupdater.KindCreateTopic, Topic: "orders", Parts: 2},
	{ID: "1", Kind: stateupdater.KindNoop},
	{ID: "2", Kind: stateupdater.KindCommit, Commit: core.CommitSegment{
		SegmentKey: "seg-a",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 3, Loc: core.SegmentRef{Key: "o1", Off: 0, Len: 30}},
			{TopicPartition: core.NewTopicPartition("orders", 1), Records: 2, Loc: core.SegmentRef{Key: "o1", Off: 30, Len: 20}},
		},
	}},
	{ID: "3", Kind: stateupdater.KindCommit, Commit: core.CommitSegment{
		SegmentKey: "seg-b",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 1, Loc: core.SegmentRef{Key: "o2", Off: 0, Len: 10}},
		},
	}},
	{ID: "4", Kind: stateupdater.KindCommit, Commit: core.CommitSegment{ // replay
		SegmentKey: "seg-a",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 3, Loc: core.SegmentRef{Key: "o1", Off: 0, Len: 30}},
			{TopicPartition: core.NewTopicPartition("orders", 1), Records: 2, Loc: core.SegmentRef{Key: "o1", Off: 30, Len: 20}},
		},
	}},
	{ID: "5", Kind: stateupdater.KindNoop},
	{ID: "6", Kind: stateupdater.KindCommit, Commit: core.CommitSegment{
		SegmentKey: "seg-c",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 1), Records: 4, Loc: core.SegmentRef{Key: "o3", Off: 0, Len: 40}},
		},
	}},
	// Exclusive creates go last, so the indexes above stay the same.
	{ID: "7", Kind: stateupdater.KindCreateTopicExclusive, Topic: "fresh", Parts: 3},  // new: OK
	{ID: "8", Kind: stateupdater.KindCreateTopicExclusive, Topic: "fresh", Parts: 1},  // exists: ErrTopicExists
	{ID: "9", Kind: stateupdater.KindCreateTopicExclusive, Topic: "orders", Parts: 5}, // exists: ErrTopicExists
	{ID: "10", Kind: stateupdater.KindCreateTopic, Topic: "fresh", Parts: 6},          // plain create: OK, grows it
	{ID: "11", Kind: stateupdater.KindCreateTopicExclusive, Topic: "", Parts: 1},      // invalid name: ErrInvalid
}

func queryDigest(exec func(core.ControlPlaneOperation) core.ControlPlaneDone) uint64 {
	h := fnv.New64a()
	d := exec(core.DescribeTopics{}).Resp.(core.DescribeTopicsResp)
	for _, t := range d.Topics {
		_, _ = fmt.Fprintf(h, "T%s/%d;", t.Name, t.Partitions)
		for p := int32(0); p < t.Partitions; p++ {
			l := exec(core.FindSegments{TopicPartition: core.NewTopicPartition(t.Name, p)}).Resp.(core.FindSegmentsResp)
			_, _ = fmt.Fprintf(h, "P%d/%d:%+v;", p, l.End, l.Ranges)
		}
	}
	return h.Sum64()
}

// feedContiguous drives catch-up so the machine applies entries in slot order
// through one ReadDone page (no gaps).
func feedContiguous(t *testing.T, m *statemachine.Machine, entries []stateupdater.Entry) []core.ControlPlaneDone {
	t.Helper()
	acts := step(m, core.NodeStarted{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	n := core.LogPosition(len(entries))
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: n, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	logs := make([]core.LogEntry, len(entries))
	for i, e := range entries {
		logs[i] = core.LogEntry{Slot: core.LogPosition(i), Data: stateupdater.Encode(e)}
	}
	step(m, core.LogReadDone{Req: read.Req, Err: core.OK, Outcome: core.Applied, Entries: logs})
	if m.Applied() != n {
		t.Fatalf("Applied = %d, want %d", m.Applied(), n)
	}
	out := make([]core.ControlPlaneDone, len(entries))
	twin := stateupdater.NewMem()
	t.Cleanup(func() { _ = twin.Close() })
	for i, e := range entries {
		out[i] = twin.Apply(int64(i), e)
	}
	if twin.StateHash() != m.Fold().StateHash() {
		t.Fatalf("twin fold hash %x != machine fold %x", twin.StateHash(), m.Fold().StateHash())
	}
	return out
}

func TestLockstepMachineVsSpecCheck(t *testing.T) {
	m := newMachine(t, statemachine.Config{CatchUpEvery: core.Millisecond, GapAfter: 0})
	orc := speccheck.NewControlPlane()
	machineAnswers := feedContiguous(t, m, lockstepCmds)

	var orcAnswers []core.ControlPlaneDone
	for i, e := range lockstepCmds {
		switch e.Kind {
		case stateupdater.KindNoop:
			orcAnswers = append(orcAnswers, core.ControlPlaneDone{Req: core.RequestID(i), Outcome: core.Applied})
		case stateupdater.KindCreateTopic, stateupdater.KindCreateTopicExclusive:
			exclusive := e.Kind == stateupdater.KindCreateTopicExclusive
			orcAnswers = append(orcAnswers, core.ControlPlaneDone{Req: core.RequestID(i), Err: orc.CreateTopicIf(e.Topic, e.Parts, exclusive)})
		case stateupdater.KindCommit:
			orcAnswers = append(orcAnswers, orc.Exec(core.ControlPlaneCall{Req: core.RequestID(i), Op: e.Commit}).(core.ControlPlaneDone))
		}
	}

	for i := range lockstepCmds {
		if lockstepCmds[i].Kind == stateupdater.KindNoop {
			continue
		}
		got, want := machineAnswers[i], orcAnswers[i]
		if got.Err != want.Err || !reflect.DeepEqual(got.Resp, want.Resp) {
			t.Fatalf("slot %d: machine %+v, speccheck %+v", i, got, want)
		}
	}

	appDig := queryDigest(func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		return m.Fold().Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
	})
	orcDig := queryDigest(func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		return orc.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
	})
	if appDig != orcDig {
		t.Fatalf("query digest: machine fold %x, speccheck %x", appDig, orcDig)
	}
	t.Logf("lockstep cmds=%d applied=%d foldHash=%x queryDigest=%x",
		len(lockstepCmds), m.Applied(), m.Fold().StateHash(), appDig)
}

func TestLockstepWithGapFill(t *testing.T) {
	m := newMachine(t, statemachine.Config{CatchUpEvery: core.Millisecond, GapAfter: 0})
	orc := speccheck.NewControlPlane()

	a := lockstepCmds[2]
	b := lockstepCmds[3]
	acts := step(m, core.NodeStarted{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 3, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{
			{Slot: 0, Data: stateupdater.Encode(a)},
			{Slot: 2, Data: stateupdater.Encode(b)},
		},
	})
	ap := acts[0].(core.LogAppend)
	if ap.Slot != 1 {
		t.Fatalf("fill slot = %d, want 1", ap.Slot)
	}
	step(m, core.LogAppendDone{Req: ap.Req, Slot: 1, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 3 {
		t.Fatalf("Applied = %d, want 3", m.Applied())
	}

	_ = orc.Exec(core.ControlPlaneCall{Req: 0, Op: a.Commit})
	_ = orc.Exec(core.ControlPlaneCall{Req: 2, Op: b.Commit})

	appDig := queryDigest(func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		return m.Fold().Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
	})
	orcDig := queryDigest(func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		return orc.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
	})
	if appDig != orcDig {
		t.Fatalf("query digest after gap-fill: machine %x, speccheck %x", appDig, orcDig)
	}
}
