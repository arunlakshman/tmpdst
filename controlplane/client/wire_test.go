package client

import (
	"reflect"
	"strings"
	"testing"

	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

func TestCommitSegmentRoundTrip(t *testing.T) {
	operation := contract.CommitSegment{
		SegmentKey: "seg/abc",
		Batches: []contract.PendingBatch{{
			TopicPartition: contract.NewTopicPartition("t", 1), Records: 3,
			Loc: contract.SegmentRef{Key: "seg/abc", Off: 0, Len: 10},
		}},
	}
	got := commitSegmentToContract(commitSegmentFromContract(operation))
	if got.SegmentKey != operation.SegmentKey || len(got.Batches) != 1 {
		t.Fatalf("got %+v", got)
	}
	b := got.Batches[0]
	if b.TopicPartition != operation.Batches[0].TopicPartition || b.Records != 3 || b.Loc != operation.Batches[0].Loc {
		t.Fatalf("batch %+v", b)
	}
}

func TestFindSegmentsDescribeRoundTrip(t *testing.T) {
	lk := contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 2), From: 5, MaxBytes: 100}
	if got := findSegmentsToContract(findSegmentsFromContract(lk)); got != lk {
		t.Fatalf("findSegments %+v", got)
	}
	dt := contract.DescribeTopics{Topics: []string{"a", "b"}}
	got := describeTopicsToContract(describeTopicsFromContract(dt))
	if len(got.Topics) != 2 || got.Topics[0] != "a" || got.Topics[1] != "b" {
		t.Fatalf("describe %+v", got)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	cr := contract.CommitSegmentResp{Assigned: []contract.BatchRange{{
		TopicPartition: contract.NewTopicPartition("t", 0), Base: 10, Count: 2,
		Loc: contract.SegmentRef{Key: "k", Off: 1, Len: 2},
	}}}
	got := commitSegmentRespToContract(commitSegmentRespFromContract(cr))
	if len(got.Assigned) != 1 || got.Assigned[0] != cr.Assigned[0] {
		t.Fatalf("commit resp %+v", got)
	}
	lr := contract.FindSegmentsResp{Ranges: cr.Assigned, End: 12}
	lgot := findSegmentsRespToContract(findSegmentsRespFromContract(lr))
	if lgot.End != 12 || len(lgot.Ranges) != 1 {
		t.Fatalf("findSegments resp %+v", lgot)
	}
	dr := contract.DescribeTopicsResp{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}}
	dgot := describeTopicsRespToContract(describeTopicsRespFromContract(dr))
	if len(dgot.Topics) != 1 || dgot.Topics[0] != dr.Topics[0] {
		t.Fatalf("describe resp %+v", dgot)
	}
}

func TestErrOutcomeMaps(t *testing.T) {
	for _, c := range []contract.ErrCode{
		contract.OK, contract.ErrTimeout, contract.ErrUnavailable, contract.ErrNotFound,
		contract.ErrConflict, contract.ErrConnClosed, contract.ErrTooLarge, contract.ErrCorrupt,
		contract.ErrInvalid, contract.ErrIO,
		contract.ErrUnknownMember, contract.ErrIllegalGeneration, contract.ErrUnknownPartition,
		contract.ErrRebalanceInProgress, contract.ErrInconsistentGroupProtocol, contract.ErrTopicExists,
	} {
		if toContractErr(fromContractErr(c)) != c {
			t.Fatalf("err %v", c)
		}
	}
	for _, o := range []contract.Outcome{contract.NotApplied, contract.MaybeApplied, contract.Applied} {
		if toContractOutcome(fromContractOutcome(o)) != o {
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
	join := contract.GroupJoin{
		Group: "g", MemberID: "m", GroupInstanceID: "i", ClientID: "c", ClientHost: "h",
		SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, ProtocolType: "consumer",
		Protocols: []contract.GroupProtocol{{Name: "range", Metadata: []byte{1}}, {Name: "rr", Metadata: []byte{2}}},
	}
	sameValue(t, "join", joinGroupToContract(joinGroupFromContract(join)), join)

	sync := contract.GroupSync{
		Group: "g", MemberID: "m", Generation: 3,
		Assignments: []contract.GroupAssignment{{Member: "a", Bytes: []byte{1}}, {Member: "b", Bytes: []byte{2}}},
	}
	sameValue(t, "sync", syncGroupToContract(syncGroupFromContract(sync)), sync)

	beat := contract.GroupHeartbeat{Group: "g", MemberID: "m", Generation: 4}
	sameValue(t, "heartbeat", heartbeatToContract(heartbeatFromContract(beat)), beat)

	leave := contract.GroupLeave{Group: "g", Members: []string{"a", "b"}}
	sameValue(t, "leave", leaveGroupToContract(leaveGroupFromContract(leave)), leave)

	commit := contract.OffsetCommit{
		Group: "g", Member: "m", Generation: 5,
		Offsets: []contract.PartitionOffset{
			{TopicPartition: contract.NewTopicPartition("t", 0), Offset: 9, LeaderEpoch: 2, Metadata: "x"},
			{TopicPartition: contract.NewTopicPartition("t", 1), Offset: 10},
		},
	}
	sameValue(t, "offset commit", offsetCommitToContract(offsetCommitFromContract(commit)), commit)

	fetch := contract.OffsetFetch{
		Group:      "g",
		Partitions: []contract.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("u", 7)},
	}
	sameValue(t, "offset fetch", offsetFetchToContract(offsetFetchFromContract(fetch)), fetch)

	names := []string{"a", "b"}
	sameValue(t, "describe groups", describeGroupsToContract(describeGroupsFromContract(names)), names)
}

func TestGroupResponsesRoundTrip(t *testing.T) {
	join := contract.GroupJoinResp{
		MemberID: "m", Generation: 3, Protocol: "range", Leader: "l",
		Members: []contract.GroupJoinMember{{MemberID: "a", GroupInstanceID: "i", Metadata: []byte{1}}, {MemberID: "b"}},
	}
	sameValue(t, "join resp", joinGroupRespToContract(joinGroupRespFromContract(join)), join)

	sync := contract.GroupSyncResp{Assignment: []byte{7, 8}}
	sameValue(t, "sync resp", syncGroupRespToContract(syncGroupRespFromContract(sync)), sync)

	codes := []contract.ErrCode{contract.OK, contract.ErrUnknownMember}
	sameValue(t, "leave resp", leaveGroupRespToContract(leaveGroupRespFromContract(codes)), codes)

	commit := contract.OffsetCommitResp{Errs: []contract.ErrCode{contract.OK, contract.ErrIllegalGeneration}}
	sameValue(t, "offset commit resp", offsetCommitRespToContract(offsetCommitRespFromContract(commit)), commit)

	fetch := contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{
		{TopicPartition: contract.NewTopicPartition("t", 2), Offset: 5, LeaderEpoch: 1, Metadata: "m"},
	}}
	sameValue(t, "offset fetch resp", offsetFetchRespToContract(offsetFetchRespFromContract(fetch)), fetch)

	described := []contract.GroupDescription{{
		Name: "g",
		DescribeGroupResp: contract.DescribeGroupResp{
			Exists: true, Generation: 2, ProtocolType: "consumer", Protocol: "range",
			Leader: "l", AssignedGeneration: 2, State: "Stable",
			Members: []contract.GroupMemberInfo{{
				GroupMember: contract.GroupMember{
					MemberID: "m", ClientID: "c", ClientHost: "h", GroupInstanceID: "i",
					SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, Metadata: []byte{1},
				},
				JoinedTimeMs: 99, Assignment: []byte{2},
			}},
		},
	}, {Name: "empty"}}
	sameValue(t, "describe groups resp", describeGroupsRespToContract(describeGroupsRespFromContract(described)), described)

	listed := []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}
	sameValue(t, "list groups resp", listGroupsRespToContract(listGroupsRespFromContract(listed)), listed)
}

func TestBrokerHeartbeatRoundTrip(t *testing.T) {
	self := contract.BrokerAddr{NodeID: 3, Host: "h", Port: 9092}
	sameValue(t, "broker heartbeat", brokerHeartbeatToContract(brokerHeartbeatFromContract(self)), self)

	live := []contract.BrokerAddr{self, {NodeID: 4, Host: "k", Port: 9093}}
	sameValue(t, "broker heartbeat resp", brokerHeartbeatRespToContract(brokerHeartbeatRespFromContract(live)), live)
}

func TestNilAndEmptyInputs(t *testing.T) {
	sameValue(t, "nil commit", commitSegmentToContract(nil), contract.CommitSegment{})
	sameValue(t, "nil findSegments", findSegmentsToContract(nil), contract.FindSegments{})
	sameValue(t, "nil describe", describeTopicsToContract(nil), contract.DescribeTopics{})
	sameValue(t, "nil commit resp", commitSegmentRespToContract(nil), contract.CommitSegmentResp{})
	sameValue(t, "nil findSegments resp", findSegmentsRespToContract(nil), contract.FindSegmentsResp{})
	sameValue(t, "nil describe resp", describeTopicsRespToContract(nil), contract.DescribeTopicsResp{})
	sameValue(t, "nil join", joinGroupToContract(nil), contract.GroupJoin{})
	sameValue(t, "nil join resp", joinGroupRespToContract(nil), contract.GroupJoinResp{})
	sameValue(t, "nil broker heartbeat", brokerHeartbeatToContract(nil), contract.BrokerAddr{})
	sameValue(t, "empty live set", brokerHeartbeatRespToContract(brokerHeartbeatRespFromContract(nil)), []contract.BrokerAddr(nil))
	sameValue(t, "nil describe groups resp", describeGroupsRespToContract(nil), []contract.GroupDescription(nil))
	sameValue(t, "nil group description", groupDescriptionToContract(nil), contract.GroupDescription{})
	sameValue(t, "group description without details", groupDescriptionToContract(&cpv1.GroupDescription{Name: "g"}), contract.GroupDescription{Name: "g"})

	got := commitSegmentToContract(&cpv1.CommitSegmentRequest{SegmentKey: "k", Batches: []*sharedv1.PendingBatch{{Records: 2}}})
	sameValue(t, "partial batch", got, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{{Records: 2}}})
}

func TestErrCodeTableIsComplete(t *testing.T) {
	for number, name := range cpv1.ErrCode_name {
		wire := cpv1.ErrCode(number)
		if back := fromContractErr(toContractErr(wire)); back != wire {
			t.Errorf("wire code %s comes back as %s", name, back)
		}
	}
	for code := contract.OK; code <= contract.ErrTopicExists; code++ {
		if strings.HasPrefix(code.String(), "ErrCode(") {
			continue
		}
		if back := toContractErr(fromContractErr(code)); back != code {
			t.Errorf("core code %s comes back as %s", code, back)
		}
	}
	sameValue(t, "unknown wire code", toContractErr(cpv1.ErrCode(9999)), contract.ErrIO)
	sameValue(t, "unknown core code", fromContractErr(contract.ErrCode(9999)), cpv1.ErrCode_ERR_CODE_IO)
}
