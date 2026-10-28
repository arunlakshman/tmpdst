package applier_test

import (
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/core"
)

func TestCommitWithWriterBases(t *testing.T) {
	a := applier.NewMem()
	op := core.CommitSegment{
		SegmentKey: "seg/1",
		Batches: []core.PendingBatch{
			{TP: core.NewTopicPartition("t", 0), Records: 3, Loc: core.SegmentRef{Key: "o", Off: 0, Len: 1}},
		},
	}
	d := a.Apply(1, applier.Entry{
		ID: "w/1", Kind: applier.KindCommit, Commit: op,
		WriterBases: []core.Offset{5},
	})
	if d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatal(d)
	}
	resp := core.ResponseAs[core.CommitSegmentResp](d)
	if len(resp.Assigned) != 1 || resp.Assigned[0].Base != 5 || resp.Assigned[0].Count != 3 {
		t.Fatalf("%+v", resp)
	}
	if a.End(core.NewTopicPartition("t", 0)) != 8 {
		t.Fatal(a.End(core.NewTopicPartition("t", 0)))
	}
}
