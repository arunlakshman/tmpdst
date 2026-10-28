package applier_test

import (
	"slices"
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/core"
)

func TestEntryCodecRoundTrip(t *testing.T) {
	es := []applier.Entry{
		{ID: "w/1", Kind: applier.KindNoop},
		{ID: "w/2", Kind: applier.KindCreateTopic, Topic: "t", Parts: 5},
		{ID: "w/3", Kind: applier.KindCommit, Commit: core.CommitSegment{
			SegmentKey: "k",
			Batches: []core.PendingBatch{
				{TP: core.NewTopicPartition("a", 0), Records: 3, Loc: core.SegmentRef{Key: "o", Off: 7, Len: 30}},
				{TP: core.NewTopicPartition("a", 1), Records: 1, Loc: core.SegmentRef{Key: "o", Off: 1, Len: 10}},
			},
		}},
	}
	for _, e := range es {
		got, err := applier.Decode(applier.Encode(e))
		if err != nil || got.ID != e.ID || got.Kind != e.Kind || got.Topic != e.Topic || got.Parts != e.Parts ||
			got.Commit.SegmentKey != e.Commit.SegmentKey || !slices.Equal(got.Commit.Batches, e.Commit.Batches) {
			t.Fatalf("round trip %+v -> %+v (%v)", e, got, err)
		}
	}
}

func TestApplyBytesNoopAndCommit(t *testing.T) {
	a := applier.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	noop, err := a.ApplyBytes(0, applier.Encode(applier.Entry{ID: "n", Kind: applier.KindNoop}))
	if err != nil || noop.Err != core.OK {
		t.Fatalf("noop: %+v %v", noop, err)
	}
	before := a.StateHash()
	d, err := a.ApplyBytes(1, applier.Encode(applier.Entry{
		ID: "c", Kind: applier.KindCommit,
		Commit: core.CommitSegment{
			SegmentKey: "s1",
			Batches:    []core.PendingBatch{{TP: core.NewTopicPartition("t", 0), Records: 2, Loc: core.SegmentRef{Key: "o", Off: 0, Len: 8}}},
		},
	}))
	if err != nil || d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatalf("commit: %+v %v", d, err)
	}
	if a.StateHash() == before {
		t.Fatal("commit left StateHash unchanged")
	}
	if a.End(core.NewTopicPartition("t", 0)) != 2 {
		t.Fatalf("end = %d, want 2", a.End(core.NewTopicPartition("t", 0)))
	}
}

func TestApplyBytesCorrupt(t *testing.T) {
	a := applier.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.ApplyBytes(0, []byte{9}); err == nil {
		t.Fatal("corrupt entry accepted")
	}
}
