package metadata_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/groupfix"
	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/metadatastore"
	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/storecall"
	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
)

var (
	install  = groupfix.Install
	assign   = groupfix.Assign
	remove   = groupfix.Remove
	pair     = groupfix.Pair
	commitOp = groupfix.CommitOp
)

func exec(t testing.TB, a *metadata.Store, operation core.ControlPlaneOperation) core.ControlPlaneDone {
	t.Helper()
	return a.Exec(core.ControlPlaneCall{Req: 1, Op: operation}).(core.ControlPlaneDone)
}

func snapshotBytes(t *testing.T, a *metadata.Store) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := a.Snapshot(&buf, 0); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// groupLog is a command list that uses every group command and a topic.
func groupLog() []metadata.Entry {
	return []metadata.Entry{
		{Kind: metadata.KindCreateTopic, Topic: "orders", Parts: 2},
		entryOf(install("g", 1, "m1", "m1", "m2")),
		entryOf(assign("g", 1, "m1", "m1", "a", "m2", "b")),
		entryOf(commitOp("g", "m1", 1, pair("orders", 0, 4), pair("orders", 1, 2))),
		entryOf(remove("g", "m2")),
		entryOf(install("g", 2, "m1", "m1", "m3")),
		entryOf(commitOp("solo", "", -1, pair("orders", 0, 1))),
		{Kind: metadata.KindCommit, Commit: core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("orders", 0), Records: 3, Loc: core.SegmentRef{Key: "o1", Off: 0, Len: 9}},
		}}},
	}
}

func entryOf(operation core.ControlPlaneOperation) metadata.Entry {
	e, ok := metadata.GroupEntry(operation)
	if !ok {
		panic("not a group op")
	}
	return e
}

// TestGroupReplayIsPure applies the same log twice from scratch, through the
// entry codec, and checks that both stores hold the same bytes.
func TestGroupReplayIsPure(t *testing.T) {
	run := func() []byte {
		a := metadata.NewMem()
		defer func() { _ = a.Close() }()
		for i, e := range groupLog() {
			data := metadata.Encode(e)
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
	a := metadata.NewMem()
	defer func() { _ = a.Close() }()
	for i, e := range groupLog() {
		a.Apply(int64(i), e)
	}
	data := snapshotBytes(t, a)
	b := metadata.NewMem()
	defer func() { _ = b.Close() }()
	if _, err := b.Restore(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, snapshotBytes(t, b)) {
		t.Fatal("restored checkpoint is not the same bytes")
	}
	for _, operation := range []core.ControlPlaneOperation{
		core.DescribeGroup{Group: "g"}, core.OffsetFetch{Group: "g"}, core.ListGroups{}, core.OffsetFetch{Group: "solo"},
	} {
		if got, want := exec(t, b, operation), exec(t, a, operation); !reflect.DeepEqual(got, want) {
			t.Fatalf("%T after restore: %+v, want %+v", operation, got, want)
		}
	}
}

func TestGroupEntryCodecRoundTrip(t *testing.T) {
	for _, e := range groupLog() {
		got, err := metadata.Decode(metadata.Encode(e))
		if err != nil {
			t.Fatalf("kind %d: %v", e.Kind, err)
		}
		if !reflect.DeepEqual(got, e) {
			t.Fatalf("kind %d: decoded %+v, want %+v", e.Kind, got, e)
		}
	}
}

func TestGroupEntryNotAGroupOp(t *testing.T) {
	if _, ok := metadata.GroupEntry(core.CreateTopic{Name: "x", Partitions: 1}); ok {
		t.Fatal("CreateTopic is not a group entry")
	}
}

// TestGroupStoreAdapter runs the group calls through the Store adapter.
func TestGroupStoreAdapter(t *testing.T) {
	s := &metadatastore.Store{A: metadata.NewMem()}
	ctx := context.Background()
	if code, err := storecall.Code(ctx, s, install("g", 1, "m1", "m1")); code != core.OK || err != nil {
		t.Fatalf("install: %v %v", code, err)
	}
	if code, err := storecall.Code(ctx, s, assign("g", 1, "m1", "m1", "a")); code != core.OK || err != nil {
		t.Fatalf("assign: %v %v", code, err)
	}
	if code, err := storecall.Code(ctx, s, remove("g", "m1")); code != core.OK || err != nil {
		t.Fatalf("remove: %v %v", code, err)
	}
	if code, err := storecall.Code(ctx, s, assign("g", 1, "m1", "m1", "a")); code != core.ErrUnknownMember || err != nil {
		t.Fatalf("assign after remove: %v %v", code, err)
	}
	response, code, err := storecall.Call[core.OffsetCommitResp](ctx, s, commitOp("g", "", -1, pair("orders", 0, 3)))
	if code != core.OK || err != nil || len(response.Errs) != 1 || response.Errs[0] != core.ErrUnknownPartition {
		t.Fatalf("commit to a topic that does not exist: %+v %v %v", response, code, err)
	}
	if _, err := storecall.Read[core.OffsetFetchResp](ctx, s, core.OffsetFetch{Group: "g"}); err != nil {
		t.Fatal(err)
	}
	if d, err := storecall.Read[core.DescribeGroupResp](ctx, s, core.DescribeGroup{Group: "g"}); err != nil || d.State != "Empty" {
		t.Fatalf("describe: %+v %v", d, err)
	}
	if ls, err := storecall.Read[core.ListGroupsResp](ctx, s, core.ListGroups{}); err != nil || len(ls.Groups) != 1 {
		t.Fatalf("list: %+v %v", ls, err)
	}
}
