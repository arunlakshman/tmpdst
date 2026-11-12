package metadb_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// groupMember is a group member with fixed client fields.
func groupMember(id string) contract.GroupMember {
	return contract.GroupMember{
		MemberID: id, ClientID: "client-" + id, ClientHost: "/10.0.0.1",
		SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("sub-" + id),
	}
}

// install is a GroupInstall of the members ids.
func install(group string, gen int32, leader string, ids ...string) contract.GroupInstall {
	operation := contract.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, Leader: leader, TimeMs: 1000}
	for _, id := range ids {
		operation.Members = append(operation.Members, groupMember(id))
	}
	return operation
}

// assign is a GroupAssign. pairs are member, bytes, member, bytes, and so on.
func assign(group string, gen int32, leader string, pairs ...string) contract.GroupAssign {
	operation := contract.GroupAssign{Group: group, Generation: gen, Leader: leader, TimeMs: 2000}
	for i := 0; i+1 < len(pairs); i += 2 {
		operation.Assignments = append(operation.Assignments, contract.GroupAssignment{Member: pairs[i], Bytes: []byte(pairs[i+1])})
	}
	return operation
}

// remove is a GroupRemove of the members ids.
func remove(group string, ids ...string) contract.GroupRemove {
	return contract.GroupRemove{Group: group, Members: ids, TimeMs: 3000, Reason: "leave"}
}

// pair is one partition offset.
func pair(topic string, p int32, off int64) contract.PartitionOffset {
	return contract.PartitionOffset{TopicPartition: contract.NewTopicPartition(topic, p), Offset: off, LeaderEpoch: 7, Metadata: "m"}
}

// commitOp is an OffsetCommit.
func commitOp(group, member string, gen int32, offs ...contract.PartitionOffset) contract.OffsetCommit {
	return contract.OffsetCommit{Group: group, Member: member, Generation: gen, TimeMs: 4000, Offsets: offs}
}

func exec(t testing.TB, a *metadb.DB, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	t.Helper()
	d, err := (&localStore{a: a}).Exec(t.Context(), operation)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func snapshotBytes(t *testing.T, a *metadb.DB) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 0); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// groupLog is a command list that uses every group command and a topic.
func groupLog() []metadb.Entry {
	return []metadb.Entry{
		{Kind: metadb.KindCreateTopic, Op: contract.CreateTopic{Name: "orders", Partitions: 2}},
		entryOf(install("g", 1, "m1", "m1", "m2")),
		entryOf(assign("g", 1, "m1", "m1", "a", "m2", "b")),
		entryOf(commitOp("g", "m1", 1, pair("orders", 0, 4), pair("orders", 1, 2))),
		entryOf(remove("g", "m2")),
		entryOf(install("g", 2, "m1", "m1", "m3")),
		entryOf(commitOp("solo", "", -1, pair("orders", 0, 1))),
		{Kind: metadb.KindCommit, Op: contract.CommitSegment{SegmentKey: "k1", Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("orders", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o1", Off: 0, Len: 9}},
		}}},
	}
}

func entryOf(operation runtime.ControlPlaneOperation) metadb.Entry {
	e, ok := metadb.EntryOf(operation)
	if !ok {
		panic("not a group op")
	}
	return e
}

// TestGroupReplayIsPure applies the same log twice from scratch, through the
// entry codec, and checks that both stores hold the same bytes.
func TestGroupReplayIsPure(t *testing.T) {
	run := func() []byte {
		a := metadb.NewMem()
		defer func() { _ = a.Close() }()
		for i, e := range groupLog() {
			data := metadb.Encode(e)
			if _, err := a.ApplyBytes(int64(i), data); err != nil {
				t.Fatal(err)
			}
		}
		return snapshotBytes(t, a)
	}
	if first, second := run(), run(); !bytes.Equal(first, second) {
		t.Fatal("two replays of one log differ")
	}
}

// TestGroupCheckpointRoundTrip checks that the G and O keys survive a checkpoint.
func TestGroupCheckpointRoundTrip(t *testing.T) {
	a := metadb.NewMem()
	defer func() { _ = a.Close() }()
	for i, e := range groupLog() {
		a.Apply(int64(i), e)
	}
	data := snapshotBytes(t, a)
	b := metadb.NewMem()
	defer func() { _ = b.Close() }()
	if _, err := b.Restore(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, snapshotBytes(t, b)) {
		t.Fatal("restored checkpoint is not the same bytes")
	}
	for _, operation := range []runtime.ControlPlaneOperation{
		contract.DescribeGroup{Group: "g"}, contract.OffsetFetch{Group: "g"}, contract.ListGroups{}, contract.OffsetFetch{Group: "solo"},
	} {
		if got, want := exec(t, b, operation), exec(t, a, operation); !reflect.DeepEqual(got, want) {
			t.Fatalf("%T after restore: %+v, want %+v", operation, got, want)
		}
	}
}

func TestGroupEntryCodecRoundTrip(t *testing.T) {
	for _, e := range groupLog() {
		got, err := metadb.Decode(metadb.Encode(e))
		if err != nil {
			t.Fatalf("kind %d: %v", e.Kind, err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Fatalf("kind %d: decoded %+v, want %+v", e.Kind, got, e)
		}
	}
}

func TestReadsAreNotLogged(t *testing.T) {
	for _, operation := range []runtime.ControlPlaneOperation{contract.DescribeTopics{}, contract.FindSegments{}, contract.OffsetFetch{}, contract.DescribeGroup{}, contract.ListGroups{}} {
		if _, ok := metadb.EntryOf(operation); ok {
			t.Fatalf("%T is a read and has no log entry", operation)
		}
	}
}

// TestGroupStoreAdapter runs the group calls through the DB adapter.
func TestGroupStoreAdapter(t *testing.T) {
	s := &localStore{a: metadb.NewMem()}
	ctx := context.Background()
	if code, err := callCode(ctx, s, install("g", 1, "m1", "m1")); code != runtime.OK || err != nil {
		t.Fatalf("install: %v %v", code, err)
	}
	if code, err := callCode(ctx, s, assign("g", 1, "m1", "m1", "a")); code != runtime.OK || err != nil {
		t.Fatalf("assign: %v %v", code, err)
	}
	if code, err := callCode(ctx, s, remove("g", "m1")); code != runtime.OK || err != nil {
		t.Fatalf("remove: %v %v", code, err)
	}
	if code, err := callCode(ctx, s, assign("g", 1, "m1", "m1", "a")); code != runtime.ErrUnknownMember || err != nil {
		t.Fatalf("assign after remove: %v %v", code, err)
	}
	response, code, err := callOp[contract.OffsetCommitResp](ctx, s, commitOp("g", "", -1, pair("orders", 0, 3)))
	if code != runtime.OK || err != nil || len(response.Errs) != 1 || response.Errs[0] != runtime.ErrUnknownPartition {
		t.Fatalf("commit to a topic that does not exist: %+v %v %v", response, code, err)
	}
	if _, err := readOp[contract.OffsetFetchResp](ctx, s, contract.OffsetFetch{Group: "g"}); err != nil {
		t.Fatal(err)
	}
	if d, err := readOp[contract.DescribeGroupResp](ctx, s, contract.DescribeGroup{Group: "g"}); err != nil || d.State != "Empty" {
		t.Fatalf("describe: %+v %v", d, err)
	}
	if ls, err := readOp[contract.ListGroupsResp](ctx, s, contract.ListGroups{}); err != nil || len(ls.Groups) != 1 {
		t.Fatalf("list: %+v %v", ls, err)
	}
}
