package topics_test

import (
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/controlplane/topics"
	"github.com/manifesterio/mds/internal/core"
)

func roundTrip(t *testing.T, operation core.ControlPlaneOperation) core.ControlPlaneOperation {
	t.Helper()
	m := topics.Module{}
	kind, ok := m.KindOf(operation)
	if !ok {
		t.Fatalf("%T has no kind", operation)
	}
	w := kv.NewWriter()
	m.Encode(w, kind, operation)
	r := kv.NewReader(w.Bytes())
	got := m.Decode(r, kind)
	if !r.Done() {
		t.Fatalf("%T: the decoder did not read exactly the bytes written", operation)
	}
	return got
}

func TestCodecRoundTrip(t *testing.T) {
	tp := core.NewTopicPartition
	for _, operation := range []core.ControlPlaneOperation{
		core.CreateTopic{Name: "orders", Partitions: 3},
		core.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true},
		core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{
			{TopicPartition: tp("t", 0), Records: 3, Loc: core.SegmentRef{Key: "o", Off: 10, Len: 30}},
			{TopicPartition: tp("t", 1), Records: 2, Loc: core.SegmentRef{Key: "o", Off: 40, Len: 20}},
		}},
		core.CommitSegment{SegmentKey: "empty"},
	} {
		got := roundTrip(t, operation)
		// an empty batch list reads back as an empty, non-nil slice; compare by content
		if c, ok := operation.(core.CommitSegment); ok && len(c.Batches) == 0 {
			if g, ok := got.(core.CommitSegment); !ok || g.SegmentKey != c.SegmentKey || len(g.Batches) != 0 {
				t.Fatalf("empty commit: %+v", got)
			}
			continue
		}
		if !reflect.DeepEqual(got, operation) {
			t.Fatalf("got %+v, want %+v", got, operation)
		}
	}
}

func TestKindsAndMode(t *testing.T) {
	m := topics.Module{}
	if len(m.Kinds()) != 3 {
		t.Fatalf("kinds %v", m.Kinds())
	}
	if _, ok := m.KindOf(core.DescribeTopics{}); ok {
		t.Fatal("a read has no kind")
	}
	for _, c := range []struct {
		operation core.ControlPlaneOperation
		want      module.Mode
	}{
		{core.CommitSegment{}, module.Logged},
		{core.CreateTopic{}, module.Logged},
		{core.FindSegments{}, module.ReadOnly},
		{core.DescribeTopics{}, module.ReadOnly},
		{core.ListGroups{}, module.NotMine},
	} {
		if got := m.Mode(c.operation); got != c.want {
			t.Errorf("%T: mode %v, want %v", c.operation, got, c.want)
		}
	}
}

func TestDecodeRejectsDamage(t *testing.T) {
	m := topics.Module{}
	valid := kv.NewWriter()
	m.Encode(valid, topics.KindCommit, core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{
		{TopicPartition: core.NewTopicPartition("t", 0), Records: 1, Loc: core.SegmentRef{Key: "o", Len: 1}},
	}})
	whole := valid.Bytes()
	// every cut of a valid commit must fail the reader, and never panic
	for n := range len(whole) {
		r := kv.NewReader(whole[:n])
		m.Decode(r, topics.KindCommit)
		if r.Done() {
			t.Fatalf("a commit cut to %d of %d bytes was accepted", n, len(whole))
		}
	}
	// a batch that points past the string table
	bad := kv.NewWriter()
	bad.String("k")
	bad.Uvarint(1) // one string in the table
	bad.String("t")
	bad.Uvarint(1) // one batch
	bad.Uvarint(5) // topic index 5: out of range
	if r := kv.NewReader(bad.Bytes()); func() bool { m.Decode(r, topics.KindCommit); return r.OK() }() {
		t.Fatal("an index outside the string table was accepted")
	}
	// a kind this module does not own
	r := kv.NewReader(nil)
	if got := m.Decode(r, 99); got != nil || r.OK() {
		t.Fatal("an unknown kind must fail the reader")
	}
	// Encode of an operation this module does not own writes nothing
	w := kv.NewWriter()
	m.Encode(w, topics.KindCommit, core.ListGroups{})
	if len(w.Bytes()) != 0 {
		t.Fatal("encoded an operation that is not a topics operation")
	}
}
