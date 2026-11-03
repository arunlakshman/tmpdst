package replica_test

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/speccheck"
)

var lockstepCmds = []metadata.Entry{
	{ID: "0", Kind: metadata.KindCreateTopic, Op: core.CreateTopic{Name: "orders", Partitions: 2}},
	{ID: "1", Kind: metadata.KindNoop},
	{ID: "2", Kind: metadata.KindCommit, Op: core.CommitSegment{
		SegmentKey: "seg-a",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 3, Loc: core.SegmentRef{Key: "o1", Off: 0, Len: 30}},
			{TopicPartition: core.NewTopicPartition("orders", 1), Records: 2, Loc: core.SegmentRef{Key: "o1", Off: 30, Len: 20}},
		},
	}},
	{ID: "3", Kind: metadata.KindCommit, Op: core.CommitSegment{
		SegmentKey: "seg-b",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 1, Loc: core.SegmentRef{Key: "o2", Off: 0, Len: 10}},
		},
	}},
	{ID: "4", Kind: metadata.KindCommit, Op: core.CommitSegment{ // replay
		SegmentKey: "seg-a",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 3, Loc: core.SegmentRef{Key: "o1", Off: 0, Len: 30}},
			{TopicPartition: core.NewTopicPartition("orders", 1), Records: 2, Loc: core.SegmentRef{Key: "o1", Off: 30, Len: 20}},
		},
	}},
	{ID: "5", Kind: metadata.KindNoop},
	{ID: "6", Kind: metadata.KindCommit, Op: core.CommitSegment{
		SegmentKey: "seg-c",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 1), Records: 4, Loc: core.SegmentRef{Key: "o3", Off: 0, Len: 40}},
		},
	}},
	// Exclusive creates go last, so the indexes above stay the same.
	{ID: "7", Kind: metadata.KindCreateTopicExclusive, Op: core.CreateTopic{Name: "fresh", Partitions: 3, FailIfExists: true}},  // new: OK
	{ID: "8", Kind: metadata.KindCreateTopicExclusive, Op: core.CreateTopic{Name: "fresh", Partitions: 1, FailIfExists: true}},  // exists: ErrTopicExists
	{ID: "9", Kind: metadata.KindCreateTopicExclusive, Op: core.CreateTopic{Name: "orders", Partitions: 5, FailIfExists: true}}, // exists: ErrTopicExists
	{ID: "10", Kind: metadata.KindCreateTopic, Op: core.CreateTopic{Name: "fresh", Partitions: 6}},                              // plain create: OK, grows it
	{ID: "11", Kind: metadata.KindCreateTopicExclusive, Op: core.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}},      // invalid name: ErrInvalid
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
func feedContiguous(t *testing.T, m *replica.Replica, entries []metadata.Entry) []core.ControlPlaneDone {
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
		logs[i] = core.LogEntry{Slot: core.LogPosition(i), Data: metadata.Encode(e)}
	}
	step(m, core.LogReadDone{Req: read.Req, Err: core.OK, Outcome: core.Applied, Entries: logs})
	if m.Applied() != n {
		t.Fatalf("Applied = %d, want %d", m.Applied(), n)
	}
	out := make([]core.ControlPlaneDone, len(entries))
	twin := metadata.NewMem()
	t.Cleanup(func() { _ = twin.Close() })
	for i, e := range entries {
		out[i] = twin.Apply(int64(i), e)
	}
	if twin.StateHash() != m.Metadata().StateHash() {
		t.Fatalf("twin fold hash %x != machine fold %x", twin.StateHash(), m.Metadata().StateHash())
	}
	return out
}

func TestLockstepMachineVsSpecCheck(t *testing.T) {
	m := newMachine(t, replica.Config{CatchUpEvery: core.Millisecond})
	orc := speccheck.NewControlPlane()
	machineAnswers := feedContiguous(t, m, lockstepCmds)

	var orcAnswers []core.ControlPlaneDone
	for i, e := range lockstepCmds {
		switch e.Kind {
		case metadata.KindNoop:
			orcAnswers = append(orcAnswers, core.ControlPlaneDone{Req: core.RequestID(i), Outcome: core.Applied})
		case metadata.KindCreateTopic, metadata.KindCreateTopicExclusive:
			create := e.Op.(core.CreateTopic)
			orcAnswers = append(orcAnswers, core.ControlPlaneDone{Req: core.RequestID(i), Err: orc.CreateTopicIf(create.Name, create.Partitions, create.FailIfExists)})
		case metadata.KindCommit:
			orcAnswers = append(orcAnswers, orc.Exec(core.ControlPlaneCall{Req: core.RequestID(i), Op: e.Op}).(core.ControlPlaneDone))
		}
	}

	for i := range lockstepCmds {
		if lockstepCmds[i].Kind == metadata.KindNoop {
			continue
		}
		got, want := machineAnswers[i], orcAnswers[i]
		if got.Err != want.Err || !reflect.DeepEqual(got.Resp, want.Resp) {
			t.Fatalf("slot %d: machine %+v, speccheck %+v", i, got, want)
		}
	}

	appDig := queryDigest(func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		return m.Metadata().Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
	})
	orcDig := queryDigest(func(operation core.ControlPlaneOperation) core.ControlPlaneDone {
		return orc.Exec(core.ControlPlaneCall{Op: operation}).(core.ControlPlaneDone)
	})
	if appDig != orcDig {
		t.Fatalf("query digest: machine fold %x, speccheck %x", appDig, orcDig)
	}
	t.Logf("lockstep cmds=%d applied=%d foldHash=%x queryDigest=%x",
		len(lockstepCmds), m.Applied(), m.Metadata().StateHash(), appDig)
}
