package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

func commitRequest(group string, gen int32, member string, parts ...int32) *kmsg.OffsetCommitRequest {
	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group, r.Generation, r.MemberID = group, gen, member
	t := kmsg.OffsetCommitRequestTopic{Topic: "t"}
	for i, p := range parts {
		t.Partitions = append(t.Partitions, kmsg.OffsetCommitRequestTopicPartition{Partition: p, Offset: int64(10 + i), LeaderEpoch: -1})
	}
	r.Topics = []kmsg.OffsetCommitRequestTopic{t}
	return r
}

func TestOffsetCommitCodesPerPartition(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.OffsetCommitResp{Errs: []core.ErrCode{core.OK, core.ErrUnknownPartition, core.ErrInvalid}})
	})
	r := h.call(h.open(1), 8, commitRequest("g1", 4, "m-1", 0, 1, 2)).(*kmsg.OffsetCommitResponse)
	want := []kafkaErrorCode{errNone, errUnknownTopicOrPart, errInvalidRequest}
	for i, p := range r.Topics[0].Partitions {
		if kafkaErrorCode(p.ErrorCode) != want[i] {
			t.Errorf("partition %d: %d, want %d", i, p.ErrorCode, want[i])
		}
	}
	op := g.calls[0].Op.(core.OffsetCommit)
	if op.Member != "m-1" || op.Generation != 4 || len(op.Offsets) != 3 || op.Offsets[2].Offset != 12 {
		t.Fatalf("commit call %+v", op)
	}
	h.noViolations()
}

func TestOffsetCommitWholeCallErrorAndManual(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpErr(core.ErrIllegalGeneration)
	})
	c := h.open(1)
	r := h.call(c, 8, commitRequest("g1", 4, "m-1", 0, 1)).(*kmsg.OffsetCommitResponse)
	for _, p := range r.Topics[0].Partitions {
		if kafkaErrorCode(p.ErrorCode) != errIllegalGeneration {
			t.Fatalf("partition %d: %d, want 22", p.Partition, p.ErrorCode)
		}
	}
	// Version 0 has no generation: a manual commit, -1 with no member.
	h.call(c, 0, commitRequest("g1", 0, "", 0))
	if op := g.calls[1].Op.(core.OffsetCommit); op.Generation != -1 || op.Member != "" {
		t.Fatalf("version 0 commit %+v, want manual", op)
	}
	h.noViolations()
}

func TestOffsetCommitEmptyGroupIsInvalid(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpOK(nil) })
	r := h.call(h.open(1), 8, commitRequest("", 1, "m", 0)).(*kmsg.OffsetCommitResponse)
	if kafkaErrorCode(r.Topics[0].Partitions[0].ErrorCode) != errInvalidGroupID || len(g.calls) != 0 {
		t.Fatalf("answer %+v, calls %d", r.Topics, len(g.calls))
	}
}

// fetchRequest asks one topic "t" with the given partitions, at version v.
func fetchRequest(group string, parts ...int32) *kmsg.OffsetFetchRequest {
	r := kmsg.NewPtrOffsetFetchRequest()
	r.Group = group
	r.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "t", Partitions: parts}}
	return r
}

func TestOffsetFetchExplicitKeepsRequestOrder(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.OffsetFetchResp{Offsets: []core.PartitionOffset{
			{TP: core.NewTopicPartition("t", 2), Offset: 7, LeaderEpoch: 0},
			{TP: core.NewTopicPartition("t", 0), Offset: -1, LeaderEpoch: -1},
		}})
	})
	r := h.call(h.open(1), 7, fetchRequest("g1", 2, 0)).(*kmsg.OffsetFetchResponse)
	ps := r.Topics[0].Partitions
	if len(ps) != 2 || ps[0].Partition != 2 || ps[0].Offset != 7 || ps[1].Partition != 0 || ps[1].Offset != -1 {
		t.Fatalf("partitions %+v", ps)
	}
	h.noViolations()
}

func TestOffsetFetchAllCommitted(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.OffsetFetchResp{Offsets: []core.PartitionOffset{
			{TP: core.NewTopicPartition("t", 0), Offset: 1}, {TP: core.NewTopicPartition("t", 3), Offset: 2},
			{TP: core.NewTopicPartition("u", 1), Offset: 3},
		}})
	})
	// An empty topic list (version 2 and up) asks for every committed offset.
	all := kmsg.NewPtrOffsetFetchRequest()
	all.Group = "g1"
	r := h.call(h.open(1), 7, all).(*kmsg.OffsetFetchResponse)
	if op := g.calls[0].Op.(core.OffsetFetch); len(op.Partitions) != 0 {
		t.Fatalf("call %+v, want all", op)
	}
	if len(r.Topics) != 2 || r.Topics[0].Topic != "t" || len(r.Topics[0].Partitions) != 2 || r.Topics[1].Topic != "u" {
		t.Fatalf("topics %+v", r.Topics)
	}
	h.noViolations()
}

func TestOffsetFetchVersion1EmptyListAnswersNothing(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpOK(nil) })
	r := kmsg.NewPtrOffsetFetchRequest()
	r.Group = "g1"
	if got := h.call(h.open(1), 1, r).(*kmsg.OffsetFetchResponse); len(got.Topics) != 0 || len(g.calls) != 0 {
		t.Fatalf("answer %+v, calls %d", got.Topics, len(g.calls))
	}
}

func TestOffsetFetchBatchedGroups(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(op core.ControlPlaneOperation) core.ControlPlaneDone {
		if f := op.(core.OffsetFetch); f.Group == "g2" {
			return cpOK(core.OffsetFetchResp{Offsets: []core.PartitionOffset{{TP: core.NewTopicPartition("t", 0), Offset: 9}}})
		}
		return cpErr(core.ErrUnavailable)
	})
	r := kmsg.NewPtrOffsetFetchRequest()
	r.Groups = []kmsg.OffsetFetchRequestGroup{
		{Group: "g1", Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "t", Partitions: []int32{0}}}},
		{Group: "g2", Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "t", Partitions: []int32{0}}}},
	}
	got := h.call(h.open(1), 8, r).(*kmsg.OffsetFetchResponse)
	if len(got.Groups) != 2 || got.Groups[0].Group != "g1" || got.Groups[1].Group != "g2" {
		t.Fatalf("groups %+v", got.Groups)
	}
	if g1 := got.Groups[0]; g1.ErrorCode != 15 || kafkaErrorCode(g1.Topics[0].Partitions[0].ErrorCode) != errCoordNotAvailable {
		t.Errorf("g1 answer %+v, want COORDINATOR_NOT_AVAILABLE", g1)
	}
	if g2 := got.Groups[1]; g2.ErrorCode != 0 || g2.Topics[0].Partitions[0].Offset != 9 {
		t.Errorf("g2 answer %+v", g2)
	}
	h.noViolations()
}

func TestOffsetFetchEmptyGroupAndTimeout(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpErr(core.ErrTimeout)
	})
	c := h.open(1)
	empty := h.call(c, 7, fetchRequest("", 0)).(*kmsg.OffsetFetchResponse)
	if kafkaErrorCode(empty.ErrorCode) != errInvalidGroupID || kafkaErrorCode(empty.Topics[0].Partitions[0].ErrorCode) != errInvalidGroupID || len(g.calls) != 0 {
		t.Fatalf("empty group answer %+v, calls %d", empty, len(g.calls))
	}
	// A timeout is COORDINATOR_NOT_AVAILABLE, on the whole answer and per partition.
	r := h.call(c, 7, fetchRequest("g1", 0, 1)).(*kmsg.OffsetFetchResponse)
	if r.ErrorCode != 15 || kafkaErrorCode(r.Topics[0].Partitions[1].ErrorCode) != errCoordNotAvailable {
		t.Fatalf("timeout answer %+v", r)
	}
	h.noViolations()
}

func describeRequest(groups ...string) *kmsg.DescribeGroupsRequest {
	r := kmsg.NewPtrDescribeGroupsRequest()
	r.Groups = groups
	return r
}

func TestDescribeGroupsStatesAndDead(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.DescribeGroupsResp{Groups: []core.GroupDescription{
			{Name: "g1", DescribeGroupResp: core.DescribeGroupResp{Exists: true, State: "PreparingSync",
				ProtocolType: "consumer", Protocol: "range", Leader: "m-1", Members: []core.GroupMemberInfo{{
					GroupMember: core.GroupMember{MemberID: "m-1", ClientID: "c1", ClientHost: "/h1", Metadata: []byte("sub")},
					Assignment:  []byte("a1"),
				}}}},
			{Name: "nope"},
		}})
	})
	r := h.call(h.open(1), 5, describeRequest("g1", "nope")).(*kmsg.DescribeGroupsResponse)
	if len(r.Groups) != 2 {
		t.Fatalf("%d groups, want 2", len(r.Groups))
	}
	g1, dead := r.Groups[0], r.Groups[1]
	if g1.ErrorCode != 0 || g1.Group != "g1" || g1.State != "CompletingRebalance" || g1.Protocol != "range" {
		t.Fatalf("g1 %+v", g1)
	}
	if len(g1.Members) != 1 || g1.Members[0].ClientID != "c1" || string(g1.Members[0].MemberAssignment) != "a1" {
		t.Fatalf("g1 members %+v", g1.Members)
	}
	if dead.ErrorCode != 0 || dead.State != "Dead" || dead.Group != "nope" {
		t.Fatalf("unknown group %+v, want Dead with no error", dead)
	}
	h.noViolations()
}

func TestDescribeGroupsTimeoutPerGroup(t *testing.T) {
	h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpErr(core.ErrTimeout) })
	r := h.call(h.open(1), 5, describeRequest("a", "b")).(*kmsg.DescribeGroupsResponse)
	for _, g := range r.Groups {
		if kafkaErrorCode(g.ErrorCode) != errCoordNotAvailable {
			t.Fatalf("group %s: %d, want 15", g.Group, g.ErrorCode)
		}
	}
}
