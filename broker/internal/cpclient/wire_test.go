package cpclient

import (
	"reflect"
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
)

func TestCommitSegmentRoundTrip(t *testing.T) {
	operation := core.CommitSegment{
		SegmentKey: "seg/abc",
		Batches: []core.PendingBatch{{
			TopicPartition: core.NewTopicPartition("t", 1), Records: 3,
			Loc: core.SegmentRef{Key: "seg/abc", Off: 0, Len: 10},
		}},
	}
	got := commitSegmentToCore(commitSegmentFromCore(operation))
	if got.SegmentKey != operation.SegmentKey || len(got.Batches) != 1 {
		t.Fatalf("got %+v", got)
	}
	b := got.Batches[0]
	if b.TopicPartition != operation.Batches[0].TopicPartition || b.Records != 3 || b.Loc != operation.Batches[0].Loc {
		t.Fatalf("batch %+v", b)
	}
}

func TestFindSegmentsDescribeRoundTrip(t *testing.T) {
	lk := core.FindSegments{TopicPartition: core.NewTopicPartition("t", 2), From: 5, MaxBytes: 100}
	if got := findSegmentsToCore(findSegmentsFromCore(lk)); got != lk {
		t.Fatalf("findSegments %+v", got)
	}
	dt := core.DescribeTopics{Topics: []string{"a", "b"}}
	got := describeTopicsToCore(describeTopicsFromCore(dt))
	if len(got.Topics) != 2 || got.Topics[0] != "a" || got.Topics[1] != "b" {
		t.Fatalf("describe %+v", got)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	cr := core.CommitSegmentResp{Assigned: []core.BatchRange{{
		TopicPartition: core.NewTopicPartition("t", 0), Base: 10, Count: 2,
		Loc: core.SegmentRef{Key: "k", Off: 1, Len: 2},
	}}}
	got := commitSegmentRespToCore(commitSegmentRespFromCore(cr))
	if len(got.Assigned) != 1 || got.Assigned[0] != cr.Assigned[0] {
		t.Fatalf("commit resp %+v", got)
	}
	lr := core.FindSegmentsResp{Ranges: cr.Assigned, End: 12}
	lgot := findSegmentsRespToCore(findSegmentsRespFromCore(lr))
	if lgot.End != 12 || len(lgot.Ranges) != 1 {
		t.Fatalf("findSegments resp %+v", lgot)
	}
	dr := core.DescribeTopicsResp{Topics: []core.TopicInfo{{Name: "t", Partitions: 3}}}
	dgot := describeTopicsRespToCore(describeTopicsRespFromCore(dr))
	if len(dgot.Topics) != 1 || dgot.Topics[0] != dr.Topics[0] {
		t.Fatalf("describe resp %+v", dgot)
	}
}

func TestErrOutcomeMaps(t *testing.T) {
	for _, c := range []core.ErrCode{
		core.OK, core.ErrTimeout, core.ErrUnavailable, core.ErrNotFound,
		core.ErrConflict, core.ErrConnClosed, core.ErrTooLarge, core.ErrCorrupt,
		core.ErrInvalid, core.ErrIO,
		core.ErrUnknownMember, core.ErrIllegalGeneration, core.ErrUnknownPartition,
		core.ErrRebalanceInProgress, core.ErrInconsistentGroupProtocol, core.ErrTopicExists,
	} {
		if toCoreErr(fromCoreErr(c)) != c {
			t.Fatalf("err %v", c)
		}
	}
	for _, o := range []core.Outcome{core.NotApplied, core.MaybeApplied, core.Applied} {
		if toCoreOutcome(fromCoreOutcome(o)) != o {
			t.Fatalf("outcome %v", o)
		}
	}
}

func sameValue(t *testing.T, name string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s\n got  %+v\n want %+v", name, got, want)
	}
}

func TestGroupRequestsRoundTrip(t *testing.T) {
	join := core.GroupJoin{
		Group: "g", MemberID: "m", GroupInstanceID: "i", ClientID: "c", ClientHost: "h",
		SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, ProtocolType: "consumer",
		Protocols: []core.GroupProtocol{{Name: "range", Metadata: []byte{1}}, {Name: "rr", Metadata: []byte{2}}},
	}
	sameValue(t, "join", joinGroupToCore(joinGroupFromCore(join)), join)

	sync := core.GroupSync{
		Group: "g", MemberID: "m", Generation: 3,
		Assignments: []core.GroupAssignment{{Member: "a", Bytes: []byte{1}}, {Member: "b", Bytes: []byte{2}}},
	}
	sameValue(t, "sync", syncGroupToCore(syncGroupFromCore(sync)), sync)

	beat := core.GroupHeartbeat{Group: "g", MemberID: "m", Generation: 4}
	sameValue(t, "heartbeat", heartbeatToCore(heartbeatFromCore(beat)), beat)

	leave := core.GroupLeave{Group: "g", Members: []string{"a", "b"}}
	sameValue(t, "leave", leaveGroupToCore(leaveGroupFromCore(leave)), leave)

	commit := core.OffsetCommit{
		Group: "g", Member: "m", Generation: 5,
		Offsets: []core.PartitionOffset{
			{TopicPartition: core.NewTopicPartition("t", 0), Offset: 9, LeaderEpoch: 2, Metadata: "x"},
			{TopicPartition: core.NewTopicPartition("t", 1), Offset: 10},
		},
	}
	sameValue(t, "offset commit", offsetCommitToCore(offsetCommitFromCore(commit)), commit)

	fetch := core.OffsetFetch{
		Group:      "g",
		Partitions: []core.TopicPartition{core.NewTopicPartition("t", 0), core.NewTopicPartition("u", 7)},
	}
	sameValue(t, "offset fetch", offsetFetchToCore(offsetFetchFromCore(fetch)), fetch)

	names := []string{"a", "b"}
	sameValue(t, "describe groups", describeGroupsToCore(describeGroupsFromCore(names)), names)
}

func TestGroupResponsesRoundTrip(t *testing.T) {
	join := core.GroupJoinResp{
		MemberID: "m", Generation: 3, Protocol: "range", Leader: "l",
		Members: []core.GroupJoinMember{{MemberID: "a", GroupInstanceID: "i", Metadata: []byte{1}}, {MemberID: "b"}},
	}
	sameValue(t, "join resp", joinGroupRespToCore(joinGroupRespFromCore(join)), join)

	sync := core.GroupSyncResp{Assignment: []byte{7, 8}}
	sameValue(t, "sync resp", syncGroupRespToCore(syncGroupRespFromCore(sync)), sync)

	codes := []core.ErrCode{core.OK, core.ErrUnknownMember}
	sameValue(t, "leave resp", leaveGroupRespToCore(leaveGroupRespFromCore(codes)), codes)

	commit := core.OffsetCommitResp{Errs: []core.ErrCode{core.OK, core.ErrIllegalGeneration}}
	sameValue(t, "offset commit resp", offsetCommitRespToCore(offsetCommitRespFromCore(commit)), commit)

	fetch := core.OffsetFetchResp{Offsets: []core.PartitionOffset{
		{TopicPartition: core.NewTopicPartition("t", 2), Offset: 5, LeaderEpoch: 1, Metadata: "m"},
	}}
	sameValue(t, "offset fetch resp", offsetFetchRespToCore(offsetFetchRespFromCore(fetch)), fetch)

	described := []core.GroupDescription{{
		Name: "g",
		DescribeGroupResp: core.DescribeGroupResp{
			Exists: true, Generation: 2, ProtocolType: "consumer", Protocol: "range",
			Leader: "l", AssignedGeneration: 2, State: "Stable",
			Members: []core.GroupMemberInfo{{
				GroupMember: core.GroupMember{
					MemberID: "m", ClientID: "c", ClientHost: "h", GroupInstanceID: "i",
					SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, Metadata: []byte{1},
				},
				JoinedTimeMs: 99, Assignment: []byte{2},
			}},
		},
	}, {Name: "empty"}}
	sameValue(t, "describe groups resp", describeGroupsRespToCore(describeGroupsRespFromCore(described)), described)

	listed := []core.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}
	sameValue(t, "list groups resp", listGroupsRespToCore(listGroupsRespFromCore(listed)), listed)
}

func TestBrokerHeartbeatRoundTrip(t *testing.T) {
	self := core.BrokerAddr{NodeID: 3, Host: "h", Port: 9092}
	sameValue(t, "broker heartbeat", brokerHeartbeatToCore(brokerHeartbeatFromCore(self)), self)

	live := []core.BrokerAddr{self, {NodeID: 4, Host: "k", Port: 9093}}
	sameValue(t, "broker heartbeat resp", brokerHeartbeatRespToCore(brokerHeartbeatRespFromCore(live)), live)
}

func TestNilAndEmptyInputs(t *testing.T) {
	sameValue(t, "nil commit", commitSegmentToCore(nil), core.CommitSegment{})
	sameValue(t, "nil findSegments", findSegmentsToCore(nil), core.FindSegments{})
	sameValue(t, "nil describe", describeTopicsToCore(nil), core.DescribeTopics{})
	sameValue(t, "nil commit resp", commitSegmentRespToCore(nil), core.CommitSegmentResp{})
	sameValue(t, "nil findSegments resp", findSegmentsRespToCore(nil), core.FindSegmentsResp{})
	sameValue(t, "nil describe resp", describeTopicsRespToCore(nil), core.DescribeTopicsResp{})
	sameValue(t, "nil join", joinGroupToCore(nil), core.GroupJoin{})
	sameValue(t, "nil join resp", joinGroupRespToCore(nil), core.GroupJoinResp{})
	sameValue(t, "nil broker heartbeat", brokerHeartbeatToCore(nil), core.BrokerAddr{})
	sameValue(t, "empty live set", brokerHeartbeatRespToCore(brokerHeartbeatRespFromCore(nil)), []core.BrokerAddr(nil))
	sameValue(t, "nil describe groups resp", describeGroupsRespToCore(nil), []core.GroupDescription(nil))
	sameValue(t, "nil group description", groupDescriptionToCore(nil), core.GroupDescription{})
	sameValue(t, "group description without details", groupDescriptionToCore(&cpv1.GroupDescription{Name: "g"}), core.GroupDescription{Name: "g"})

	got := commitSegmentToCore(&cpv1.CommitSegmentRequest{SegmentKey: "k", Batches: []*sharedv1.PendingBatch{{Records: 2}}})
	sameValue(t, "partial batch", got, core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{{Records: 2}}})
}

func TestErrCodeTableIsComplete(t *testing.T) {
	for number, name := range cpv1.ErrCode_name {
		wire := cpv1.ErrCode(number)
		if back := fromCoreErr(toCoreErr(wire)); back != wire {
			t.Errorf("wire code %s comes back as %s", name, back)
		}
	}
	for code := core.OK; code <= core.ErrTopicExists; code++ {
		if strings.HasPrefix(code.String(), "ErrCode(") {
			continue
		}
		if back := toCoreErr(fromCoreErr(code)); back != code {
			t.Errorf("core code %s comes back as %s", code, back)
		}
	}
	sameValue(t, "unknown wire code", toCoreErr(cpv1.ErrCode(9999)), core.ErrIO)
	sameValue(t, "unknown core code", fromCoreErr(core.ErrCode(9999)), cpv1.ErrCode_ERR_CODE_IO)
}
