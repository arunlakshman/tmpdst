package stateupdater_test

import (
	"testing"

	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

func TestCommitWithWriterBases(t *testing.T) {
	a := stateupdater.NewMem()
	operation := core.CommitSegment{
		SegmentKey: "seg/1",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("t", 0), Records: 3, Loc: core.SegmentRef{Key: "o", Off: 0, Len: 1}},
		},
	}
	d := a.Apply(1, stateupdater.Entry{
		ID: "w/1", Kind: stateupdater.KindCommit, Commit: operation,
		WriterBases: []core.Offset{5},
	})
	if d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatal(d)
	}
	response := core.ResponseAs[core.CommitSegmentResp](d)
	if len(response.Assigned) != 1 || response.Assigned[0].Base != 5 || response.Assigned[0].Count != 3 {
		t.Fatalf("%+v", response)
	}
	if a.End(core.NewTopicPartition("t", 0)) != 8 {
		t.Fatal(a.End(core.NewTopicPartition("t", 0)))
	}
}
