package metadb_test

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func TestEntryCodecRoundTrip(t *testing.T) {
	es := []metadb.Entry{
		{ID: "w/1", Kind: metadb.KindNoop},
		{ID: "w/2", Kind: metadb.KindCreateTopic, Op: contract.CreateTopic{Name: "t", Partitions: 5}},
		{ID: "w/2x", Kind: metadb.KindCreateTopicExclusive, Op: contract.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}},
		{ID: "w/3", Kind: metadb.KindCommit, Op: contract.CommitSegment{
			SegmentKey: "k",
			Batches: []contract.PendingBatch{
				{TopicPartition: contract.NewTopicPartition("a", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o", Off: 7, Len: 30}},
				{TopicPartition: contract.NewTopicPartition("a", 1), Records: 1, Loc: runtime.SegmentRef{Key: "o", Off: 1, Len: 10}},
			},
		}},
	}
	for _, e := range es {
		got, err := metadb.Decode(metadb.Encode(e))
		if err != nil || got.ID != e.ID || got.Kind != e.Kind || !reflect.DeepEqual(got.Op, e.Op) {
			t.Fatalf("round trip %+v -> %+v (%v)", e, got, err)
		}
	}
}

func TestApplyBytesNoopAndCommit(t *testing.T) {
	a := metadb.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	noop, err := a.ApplyBytes(0, metadb.Encode(metadb.Entry{ID: "n", Kind: metadb.KindNoop}))
	if err != nil || noop.Err != runtime.OK {
		t.Fatalf("noop: %+v %v", noop, err)
	}
	before := a.StateHash()
	d, err := a.ApplyBytes(1, metadb.Encode(metadb.Entry{
		ID: "c", Kind: metadb.KindCommit,
		Op: contract.CommitSegment{
			SegmentKey: "s1",
			Batches:    []contract.PendingBatch{{TopicPartition: contract.NewTopicPartition("t", 0), Records: 2, Loc: runtime.SegmentRef{Key: "o", Off: 0, Len: 8}}},
		},
	}))
	if err != nil || d.Err != runtime.OK || d.Outcome != runtime.Applied {
		t.Fatalf("commit: %+v %v", d, err)
	}
	if a.StateHash() == before {
		t.Fatal("commit left StateHash unchanged")
	}
	if end := endOf(t, a, contract.NewTopicPartition("t", 0)); end != 2 {
		t.Fatalf("end = %d, want 2", end)
	}
}

func TestApplyBytesCorrupt(t *testing.T) {
	a := metadb.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.ApplyBytes(0, []byte{9}); err == nil {
		t.Fatal("corrupt entry accepted")
	}
}

// The kinds do not repeat, and only the no-op is zero.
func TestKindsAreUniqueAndNonZero(t *testing.T) {
	seen := map[metadb.Kind]bool{}
	for _, kind := range []metadb.Kind{
		metadb.KindCommit, metadb.KindCreateTopic, metadb.KindCreateTopicExclusive,
		metadb.KindGroupInstall, metadb.KindGroupAssign, metadb.KindGroupRemove, metadb.KindOffsetCommit,
	} {
		if kind == metadb.KindNoop {
			t.Fatal("a command kind must not be the no-op")
		}
		if seen[kind] {
			t.Fatalf("kind %d is used twice", kind)
		}
		seen[kind] = true
	}
}

// Decode refuses bytes that are not a protobuf Entry, and entries with a field
// that the log messages do not define, at any depth.
func TestDecodeRejectsBadBytes(t *testing.T) {
	noop := metadb.Encode(metadb.Entry{ID: "n", Kind: metadb.KindNoop})
	create := metadb.Encode(metadb.Entry{Op: contract.CreateTopic{Name: "t", Partitions: 1}})
	for _, c := range []struct {
		name string
		b    []byte
	}{
		{"bad wire type", []byte{0xFE, 0}},
		{"leftover byte", append(slices.Clone(noop), 0)},
		{"unknown field", append(slices.Clone(noop), 0x40, 1)},                    // field 8, varint 1
		{"unknown field in the command", []byte{0x1a, 0x02, 0x20, 0x01}},          // CreateTopic field 4
		{"unknown field in a list item", []byte{0x12, 0x04, 0x12, 0x02, 0x28, 1}}, // PendingBatch field 5
		{"unknown field two levels down", []byte{0x12, 0x06, 0x12, 0x04, 0x0a, 0x02, 0x18, 1}},
		{"not utf-8", []byte{0x0a, 0x01, 0xff}},
		{"cut", create[:len(create)-1]},
	} {
		if _, err := metadb.Decode(c.b); !errors.Is(err, metadb.ErrCorrupt) {
			t.Errorf("%s (%x): %v, want ErrCorrupt", c.name, c.b, err)
		}
	}
	if e, err := metadb.Decode(nil); err != nil || e.Kind != metadb.KindNoop || e.ID != "" {
		t.Fatalf("empty bytes are the no-op with no id: %+v %v", e, err)
	}
}

// Bytes that Encode would not write, but that are a valid Entry, decode by the
// rules of protobuf: a later value wins, and a value that does not fit an
// int32 is cut to 32 bits. Every replica reads them the same way.
func TestDecodeFollowsTheWireFormat(t *testing.T) {
	for _, c := range []struct {
		name string
		b    []byte
		want metadb.Entry
	}{
		{"zero written out", []byte{0x0a, 0x00}, metadb.Entry{}},
		{"fields out of order", []byte{0x1a, 0x00, 0x0a, 0x01, 'n'}, metadb.Entry{ID: "n", Kind: metadb.KindCreateTopic, Op: contract.CreateTopic{}}},
		{"long varint", []byte{0x1a, 0x03, 0x10, 0x81, 0x00}, metadb.Entry{Kind: metadb.KindCreateTopic, Op: contract.CreateTopic{Partitions: 1}}},
		{"too big for int32", []byte{0x1a, 0x06, 0x10, 0x81, 0x80, 0x80, 0x80, 0x10}, metadb.Entry{Kind: metadb.KindCreateTopic, Op: contract.CreateTopic{Partitions: 1}}},
		{"the later id wins", []byte{0x0a, 0x01, 'a', 0x0a, 0x01, 'b'}, metadb.Entry{ID: "b"}},
		{"the later command wins", []byte{0x1a, 0x00, 0x22, 0x00}, metadb.Entry{Kind: metadb.KindGroupInstall, Op: contract.GroupInstall{}}},
	} {
		got, err := metadb.Decode(c.b)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s (%x): %+v %v, want %+v", c.name, c.b, got, err, c.want)
		}
	}
}

// Every logged operation has a kind, and every other operation has none.
func TestEntryOfMatchesMode(t *testing.T) {
	a := metadb.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	for _, operation := range []runtime.ControlPlaneOperation{
		contract.CommitSegment{}, contract.CreateTopic{}, contract.CreateTopic{FailIfExists: true},
		contract.GroupInstall{}, contract.GroupAssign{}, contract.GroupRemove{}, contract.OffsetCommit{},
		contract.FindSegments{}, contract.DescribeTopics{}, contract.DescribeGroup{}, contract.OffsetFetch{}, contract.ListGroups{},
	} {
		_, logged := metadb.EntryOf(operation)
		if logged != (a.Mode(operation) == metadb.Logged) {
			t.Errorf("%T: EntryOf says logged %v, Mode says %v", operation, logged, a.Mode(operation))
		}
	}
}

// Encode of an operation that is not logged is a bug in the caller: it panics
// instead of writing an entry that means something else.
func TestEncodePanicsOnARead(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	metadb.Encode(metadb.Entry{ID: "r", Op: contract.DescribeTopics{}})
}
