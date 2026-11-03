package metadata_test

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
)

// goldenEntries are one entry of every kind. Their bytes are part of the log
// format: a log written by an older node must be read by a newer one, so these
// bytes may never change. want is the hex of metadata.Encode.
func goldenEntries() []struct {
	name  string
	entry metadata.Entry
	want  string
} {
	of := func(id string, op core.ControlPlaneOperation) metadata.Entry {
		e, ok := metadata.EntryOf(op)
		if !ok {
			panic("not a logged operation")
		}
		e.ID = id
		return e
	}
	tp := core.NewTopicPartition
	commit := core.CommitSegment{SegmentKey: "seg-1", Batches: []core.PendingBatch{
		{TopicPartition: tp("t", 0), Records: 3, Loc: core.SegmentRef{Key: "obj-1", Off: 10, Len: 30}},
		{TopicPartition: tp("t", 1), Records: 2, Loc: core.SegmentRef{Key: "obj-1", Off: 40, Len: 20}},
		{TopicPartition: tp("u", 0), Records: 1, Loc: core.SegmentRef{Key: "obj-2", Off: 0, Len: 9}},
	}}
	return []struct {
		name  string
		entry metadata.Entry
		want  string
	}{
		{"noop", metadata.Entry{ID: "n1", Kind: metadata.KindNoop}, "00026e31"},
		{"commit", of("c1", commit), "01026331057365672d31040174056f626a2d310175056f626a2d320300000601143c000204015028020002030012"},
		{"create topic", of("x1", core.CreateTopic{Name: "orders", Partitions: 3}), "02027831066f726465727306"},
		{"create topic exclusive", of("x2", core.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true}), "07027832066f726465727306"},
		{"group install", of("g1", core.GroupInstall{Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 2, Leader: "m1", TimeMs: 1000,
			Members: []core.GroupMember{
				{MemberID: "m1", ClientID: "c1", ClientHost: "/h", GroupInstanceID: "i1", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("sub1")},
				{MemberID: "m2", ClientID: "c2", ClientHost: "/h", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000},
			}}), "03026731016708636f6e73756d65720572616e676504026d31d00f02026d31026331022f68026931a09c01e0d4030473756231026d32026332022f6800a09c01e0d40300"},
		{"group assign", of("g2", core.GroupAssign{Group: "g", Generation: 2, Leader: "m1", TimeMs: 2000,
			Assignments: []core.GroupAssignment{{Member: "m1", Bytes: []byte("a1")}, {Member: "m2", Bytes: []byte("a2")}}}), "04026732016704026d31a01f02026d31026131026d32026132"},
		{"group remove", of("g3", core.GroupRemove{Group: "g", Members: []string{"m1", "m2"}, TimeMs: 3000, Reason: "leave"}), "050267330167f02e056c6561766502026d31026d32"},
		{"offset commit", of("g4", core.OffsetCommit{Group: "g", Member: "m1", Generation: 2, TimeMs: 4000,
			Offsets: []core.PartitionOffset{{TopicPartition: tp("t", 0), Offset: 5, LeaderEpoch: 7, Metadata: "m"}, {TopicPartition: tp("t", 1), Offset: 9, LeaderEpoch: -1}}}), "060267340167026d3104c03e020174000a0e016d017402120100"},
	}
}

func TestGoldenEntryBytes(t *testing.T) {
	for _, g := range goldenEntries() {
		t.Run(g.name, func(t *testing.T) {
			got := metadata.Encode(g.entry)
			if g.want == "" {
				t.Fatalf("no golden bytes yet; encoded: %s", hex.EncodeToString(got))
			}
			if hex.EncodeToString(got) != g.want {
				t.Fatalf("the log format changed:\n got %s\nwant %s", hex.EncodeToString(got), g.want)
			}
			back, err := metadata.Decode(got)
			if err != nil || !reflect.DeepEqual(back, g.entry) {
				t.Fatalf("decode: %+v %v, want %+v", back, err, g.entry)
			}
		})
	}
}
