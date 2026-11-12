package metadb_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// FuzzDecode: Decode returns an entry or an error on any bytes, and never
// panics. The log is the one input that a node reads from outside its own
// state, so a damaged entry has to be an error, not a crash. Bytes that Encode
// would not write may still decode (protobuf allows many encodings of one
// message), so a decoded entry is checked to encode to bytes that decode to the
// same entry and encode to the same bytes again.
func FuzzDecode(f *testing.F) {
	tp := contract.NewTopicPartition
	f.Add(metadb.Encode(metadb.Entry{Kind: metadb.KindNoop, ID: "n"}))
	for i, op := range []runtime.ControlPlaneOperation{
		contract.CreateTopic{Name: "orders", Partitions: 3},
		contract.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true},
		contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
			{TopicPartition: tp("t", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o", Off: 10, Len: 30}},
			{TopicPartition: tp("t", 1), Records: 2, Loc: runtime.SegmentRef{Key: "o", Off: 40, Len: 20}},
		}},
		contract.GroupInstall{Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 1, Leader: "m",
			Members: []contract.GroupMember{{MemberID: "m", ClientID: "c", SessionTimeoutMs: 10, RebalanceTimeoutMs: 20, Metadata: []byte("x")}}},
		contract.GroupAssign{Group: "g", Generation: 1, Leader: "m", Assignments: []contract.GroupAssignment{{Member: "m", Bytes: []byte("a")}}},
		contract.GroupRemove{Group: "g", Reason: "r", Members: []string{"m"}},
		contract.OffsetCommit{Group: "g", Member: "m", Generation: 1, Offsets: []contract.PartitionOffset{
			{TopicPartition: tp("t", 0), Offset: 5, LeaderEpoch: 1, Metadata: "md"}}},
	} {
		e, ok := metadb.EntryOf(op)
		if !ok {
			f.Fatalf("seed %d is not a logged operation", i)
		}
		e.ID = "id"
		f.Add(metadb.Encode(e))
	}
	f.Add([]byte{})
	f.Add([]byte{0x1a, 0x03, 0x10, 0x81, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		e, err := metadb.Decode(b)
		if err != nil {
			return
		}
		again := metadb.Encode(e)
		e2, err := metadb.Decode(again)
		if err != nil || !reflect.DeepEqual(e2, e) {
			t.Fatalf("Decode(Encode(e)) = %+v (%v), want %+v", e2, err, e)
		}
		if !bytes.Equal(metadb.Encode(e2), again) {
			t.Fatalf("Encode is not stable: %x, then %x", again, metadb.Encode(e2))
		}
	})
}
