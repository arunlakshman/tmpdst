package updaterstore

import (
	"context"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/storecall"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
)

func newStore() *Store { return &Store{A: stateupdater.NewMem()} }

func TestSegmentAndTopicOperations(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	if code, err := storecall.Code(ctx, s, core.CreateTopic{Name: "t", Partitions: 3}); code != core.OK || err != nil {
		t.Fatal(code, err)
	}
	if code, _ := storecall.Code(ctx, s, core.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true}); code != core.ErrTopicExists {
		t.Fatalf("second create with FailIfExists: %v", code)
	}
	batch := core.PendingBatch{TopicPartition: core.NewTopicPartition("t", 2), Records: 4, Loc: core.SegmentRef{Key: "k", Len: 40}}
	response, code, err := storecall.Call[core.CommitSegmentResp](ctx, s, core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{batch}})
	if err != nil || code != core.OK || response.Assigned[0].Base != 0 {
		t.Fatalf("%+v %v %v", response, code, err)
	}
	// The slot counter moves on: a second key gets the next offsets.
	response, code, _ = storecall.Call[core.CommitSegmentResp](ctx, s, core.CommitSegment{SegmentKey: "k2", Batches: []core.PendingBatch{batch}})
	if code != core.OK || response.Assigned[0].Base != 4 {
		t.Fatalf("%+v %v", response, code)
	}
	found, code, _ := storecall.Call[core.FindSegmentsResp](ctx, s, core.FindSegments{TopicPartition: core.NewTopicPartition("t", 2)})
	if code != core.OK || found.End != 8 || len(found.Ranges) != 2 {
		t.Fatalf("%+v %v", found, code)
	}
	if _, code, _ = storecall.Call[core.FindSegmentsResp](ctx, s, core.FindSegments{From: -1}); code != core.ErrInvalid {
		t.Fatal(code)
	}
	described, err := storecall.Read[core.DescribeTopicsResp](ctx, s, core.DescribeTopics{})
	if err != nil || len(described.Topics) != 1 || described.Topics[0].Partitions != 3 {
		t.Fatalf("%+v %v", described, err)
	}
}

func TestGroupOperations(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	member := core.GroupMember{MemberID: "m1", ClientID: "c", SessionTimeoutMs: 1000, RebalanceTimeoutMs: 1000}
	install := core.GroupInstall{Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 1, Leader: "m1", TimeMs: 1, Members: []core.GroupMember{member}}
	if code, err := storecall.Code(ctx, s, install); code != core.OK || err != nil {
		t.Fatalf("install: %v %v", code, err)
	}
	assign := core.GroupAssign{Group: "g", Generation: 1, Leader: "m1", TimeMs: 2, Assignments: []core.GroupAssignment{{Member: "m1", Bytes: []byte("a")}}}
	if code, err := storecall.Code(ctx, s, assign); code != core.OK || err != nil {
		t.Fatalf("assign: %v %v", code, err)
	}
	if code, err := storecall.Code(ctx, s, core.GroupRemove{Group: "g", Members: []string{"m1"}, TimeMs: 3, Reason: "leave"}); code != core.OK || err != nil {
		t.Fatalf("remove: %v %v", code, err)
	}
	if code, _ := storecall.Code(ctx, s, assign); code != core.ErrUnknownMember {
		t.Fatalf("assign after remove: %v", code)
	}
	offsets := core.OffsetCommit{Group: "g", Generation: -1, TimeMs: 4, Offsets: []core.PartitionOffset{{TopicPartition: core.NewTopicPartition("orders", 0), Offset: 3}}}
	response, code, err := storecall.Call[core.OffsetCommitResp](ctx, s, offsets)
	if code != core.OK || err != nil || len(response.Errs) != 1 || response.Errs[0] != core.ErrUnknownPartition {
		t.Fatalf("commit to a topic that does not exist: %+v %v %v", response, code, err)
	}
	if _, err := storecall.Read[core.OffsetFetchResp](ctx, s, core.OffsetFetch{Group: "g"}); err != nil {
		t.Fatal(err)
	}
	if d, err := storecall.Read[core.DescribeGroupResp](ctx, s, core.DescribeGroup{Group: "g"}); err != nil || d.State != "Empty" {
		t.Fatalf("describe: %+v %v", d, err)
	}
	if l, err := storecall.Read[core.ListGroupsResp](ctx, s, core.ListGroups{}); err != nil || len(l.Groups) != 1 {
		t.Fatalf("list: %+v %v", l, err)
	}
}

func TestUnknownOperationAndCancelledContext(t *testing.T) {
	s := newStore()
	if d, err := s.Exec(t.Context(), core.GroupJoin{}); err != nil || d.Err != core.ErrInvalid {
		t.Fatalf("unknown operation: %+v %v", d, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Exec(ctx, core.ListGroups{}); err == nil {
		t.Fatal("want the context error")
	}
}
