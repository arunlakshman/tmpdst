package metadata_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
)

func TestEntryCodecRoundTrip(t *testing.T) {
	es := []metadata.Entry{
		{ID: "w/1", Kind: metadata.KindNoop},
		{ID: "w/2", Kind: metadata.KindCreateTopic, Op: core.CreateTopic{Name: "t", Partitions: 5}},
		{ID: "w/2x", Kind: metadata.KindCreateTopicExclusive, Op: core.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}},
		{ID: "w/3", Kind: metadata.KindCommit, Op: core.CommitSegment{
			SegmentKey: "k",
			Batches: []core.PendingBatch{
				{TopicPartition: core.NewTopicPartition("a", 0), Records: 3, Loc: core.SegmentRef{Key: "o", Off: 7, Len: 30}},
				{TopicPartition: core.NewTopicPartition("a", 1), Records: 1, Loc: core.SegmentRef{Key: "o", Off: 1, Len: 10}},
			},
		}},
	}
	for _, e := range es {
		got, err := metadata.Decode(metadata.Encode(e))
		if err != nil || got.ID != e.ID || got.Kind != e.Kind || !reflect.DeepEqual(got.Op, e.Op) {
			t.Fatalf("round trip %+v -> %+v (%v)", e, got, err)
		}
	}
}

func TestApplyBytesNoopAndCommit(t *testing.T) {
	a := metadata.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	noop, err := a.ApplyBytes(0, metadata.Encode(metadata.Entry{ID: "n", Kind: metadata.KindNoop}))
	if err != nil || noop.Err != core.OK {
		t.Fatalf("noop: %+v %v", noop, err)
	}
	before := a.StateHash()
	d, err := a.ApplyBytes(1, metadata.Encode(metadata.Entry{
		ID: "c", Kind: metadata.KindCommit,
		Op: core.CommitSegment{
			SegmentKey: "s1",
			Batches:    []core.PendingBatch{{TopicPartition: core.NewTopicPartition("t", 0), Records: 2, Loc: core.SegmentRef{Key: "o", Off: 0, Len: 8}}},
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
	a := metadata.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.ApplyBytes(0, []byte{9}); err == nil {
		t.Fatal("corrupt entry accepted")
	}
}

// Every kind belongs to one module, kinds do not repeat, and the no-op (zero) is
// the kernel's own. A repeated number would make a log entry mean two things.
func TestKindsAreUniqueAndNonZero(t *testing.T) {
	seen := map[metadata.Kind]bool{}
	for _, kind := range []metadata.Kind{
		metadata.KindCommit, metadata.KindCreateTopic, metadata.KindCreateTopicExclusive,
		metadata.KindGroupInstall, metadata.KindGroupAssign, metadata.KindGroupRemove, metadata.KindOffsetCommit,
	} {
		if kind == metadata.KindNoop {
			t.Fatal("a module kind must not be the no-op")
		}
		if seen[kind] {
			t.Fatalf("kind %d is used twice", kind)
		}
		seen[kind] = true
	}
}

func TestDecodeRejectsUnknownKindAndLeftoverBytes(t *testing.T) {
	if _, err := metadata.Decode([]byte{0xFE, 0}); !errors.Is(err, metadata.ErrCorrupt) {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err := metadata.Decode(append(metadata.Encode(metadata.Entry{ID: "n", Kind: metadata.KindNoop}), 0)); !errors.Is(err, metadata.ErrCorrupt) {
		t.Fatalf("leftover byte: %v", err)
	}
}
