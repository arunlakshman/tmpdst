package metadb_test

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	logv1 "github.com/manifesterio/mds/controlplane/api/controlplane/log/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// oneOfEachKind is one entry of every kind, with every field set.
func oneOfEachKind() []metadb.Entry {
	of := func(id string, op runtime.ControlPlaneOperation) metadb.Entry {
		e, ok := metadb.EntryOf(op)
		if !ok {
			panic("not a logged operation")
		}
		e.ID = id
		return e
	}
	tp := contract.NewTopicPartition
	return []metadb.Entry{
		{ID: "n1", Kind: metadb.KindNoop},
		of("c1", contract.CommitSegment{SegmentKey: "seg-1", Batches: []contract.PendingBatch{
			{TopicPartition: tp("t", 0), Records: 3, Loc: runtime.SegmentRef{Key: "obj-1", Off: 10, Len: 30}},
			{TopicPartition: tp("u", 1), Records: 2, Loc: runtime.SegmentRef{Key: "obj-2", Off: 40, Len: 20}},
		}}),
		of("x1", contract.CreateTopic{Name: "orders", Partitions: 3}),
		of("x2", contract.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true}),
		of("g1", contract.GroupInstall{Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 2, Leader: "m1", TimeMs: 1000,
			Members: []contract.GroupMember{
				{MemberID: "m1", ClientID: "c1", ClientHost: "/h", GroupInstanceID: "i1", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("sub1")},
				{MemberID: "m2", ClientID: "c2", ClientHost: "/h", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000},
			}}),
		of("g2", contract.GroupAssign{Group: "g", Generation: 2, Leader: "m1", TimeMs: 2000,
			Assignments: []contract.GroupAssignment{{Member: "m1", Bytes: []byte("a1")}, {Member: "m2", Bytes: []byte("a2")}}}),
		of("g3", contract.GroupRemove{Group: "g", Members: []string{"m1", "m2"}, TimeMs: 3000, Reason: "leave"}),
		of("g4", contract.OffsetCommit{Group: "g", Member: "m1", Generation: 2, TimeMs: 4000,
			Offsets: []contract.PartitionOffset{{TopicPartition: tp("t", 0), Offset: 5, LeaderEpoch: 7, Metadata: "m"}, {TopicPartition: tp("t", 1), Offset: 9, LeaderEpoch: -1}}}),
	}
}

// TestEntryBytesAreDeterministic: an entry has one byte string. Encode gives the
// same bytes on every call, and a log message built by setting its fields in
// the other order, or read back from the bytes, marshals to the same bytes.
func TestEntryBytesAreDeterministic(t *testing.T) {
	deterministic := proto.MarshalOptions{Deterministic: true}
	for _, e := range oneOfEachKind() {
		want := metadb.Encode(e)
		for range 200 {
			if got := metadb.Encode(e); !bytes.Equal(got, want) {
				t.Fatalf("kind %d: Encode gave %x, then %x", e.Kind, want, got)
			}
		}
		var read logv1.Entry
		if err := proto.Unmarshal(want, &read); err != nil {
			t.Fatal(err)
		}
		// Without map fields the plain Marshal gives the same bytes too.
		for i, m := range []proto.Message{&read, reverseCopy(&read)} {
			for _, marshal := range []func(proto.Message) ([]byte, error){deterministic.Marshal, proto.Marshal} {
				got, err := marshal(m)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("kind %d, message %d (0 read back, 1 built in reverse): %x (%v), want %x", e.Kind, i, got, err, want)
				}
			}
		}
	}
}

// reverseCopy is a new message with the fields of m, set from the highest field
// number to the lowest, and the same for every message inside it.
func reverseCopy(m proto.Message) proto.Message {
	return copyReversed(m.ProtoReflect()).Interface()
}

func copyReversed(src protoreflect.Message) protoreflect.Message {
	dst := src.New()
	fields := src.Descriptor().Fields()
	for i := fields.Len() - 1; i >= 0; i-- {
		fd := fields.Get(i)
		if !src.Has(fd) {
			continue
		}
		v := src.Get(fd)
		switch {
		case fd.IsList():
			list := dst.Mutable(fd).List()
			for j := range v.List().Len() {
				item := v.List().Get(j)
				if fd.Message() != nil {
					item = protoreflect.ValueOfMessage(copyReversed(item.Message()))
				}
				list.Append(item)
			}
		case fd.Message() != nil:
			dst.Set(fd, protoreflect.ValueOfMessage(copyReversed(v.Message())))
		default:
			dst.Set(fd, v)
		}
	}
	return dst
}
