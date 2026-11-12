package wire

import (
	"testing"

	"google.golang.org/protobuf/proto"

	sharedv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Every field of a group request reaches the machine input, with the call id.
func TestGroupRequestsToMachine(t *testing.T) {
	join := &cpv1.JoinGroupRequest{
		Group: "g", MemberId: "m", GroupInstanceId: "i", ClientId: "c", ClientHost: "h",
		SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, ProtocolType: "consumer",
		Protocols: []*cpv1.GroupProtocol{{Name: "range", Metadata: []byte{1}}, {Name: "rr", Metadata: []byte{2}}},
	}
	sameValue(t, "join", JoinToMachine(7, join), rebalance.Join{
		Call: 7, Group: "g", MemberID: "m", GroupInstanceID: "i", ClientID: "c", ClientHost: "h",
		SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, ProtocolType: "consumer",
		Protocols: []rebalance.Protocol{{Name: "range", Metadata: []byte{1}}, {Name: "rr", Metadata: []byte{2}}},
	})
	sync := &cpv1.SyncGroupRequest{Group: "g", MemberId: "m", Generation: 3,
		Assignments: []*cpv1.GroupAssignment{{MemberId: "m", Assignment: []byte{4}}}}
	sameValue(t, "sync", SyncToMachine(8, sync), rebalance.Sync{Call: 8, Group: "g", MemberID: "m", Generation: 3,
		Assignments: []rebalance.Assignment{{Member: "m", Bytes: []byte{4}}}})
	sameValue(t, "heartbeat", HeartbeatToMachine(9, &cpv1.HeartbeatRequest{Group: "g", MemberId: "m", Generation: 5}),
		rebalance.Heartbeat{Call: 9, Group: "g", MemberID: "m", Generation: 5})
	sameValue(t, "leave", LeaveToMachine(10, &cpv1.LeaveGroupRequest{Group: "g", Members: []string{"a", "b"}}),
		rebalance.Leave{Call: 10, Group: "g", Members: []string{"a", "b"}})
	commit := &cpv1.OffsetCommitRequest{Group: "g", MemberId: "m", Generation: 6, Offsets: []*cpv1.PartitionOffsetCommit{
		{Tp: &sharedv1.TopicPartition{Topic: "t", Partition: 1}, Offset: 4, LeaderEpoch: 2, Metadata: "x"}}}
	sameValue(t, "commit", OffsetCommitToMachine(11, commit), rebalance.OffsetCommit{Call: 11, Op: contract.OffsetCommit{
		Group: "g", Member: "m", Generation: 6, Offsets: []contract.PartitionOffset{
			{TopicPartition: contract.NewTopicPartition("t", 1), Offset: 4, LeaderEpoch: 2, Metadata: "x"}}}})
	sameValue(t, "describe", DescribeToMachine(12, &cpv1.DescribeGroupsRequest{Groups: []string{"a", "b"}}),
		rebalance.Describe{Call: 12, Groups: []string{"a", "b"}})
	sameValue(t, "list", ListToMachine(13, &cpv1.ListGroupsRequest{}), rebalance.List{Call: 13})
}

// Every field of a machine answer reaches the response. The generated
// converters of the contract types read the responses back.
func TestMachineAnswersToWire(t *testing.T) {
	join := JoinGroupResponseFromCore(rebalance.JoinAnswer{Generation: 2, Protocol: "range", Leader: "a", MemberID: "b",
		Members: []rebalance.Member{{ID: "a", InstanceID: "i", Metadata: []byte{1}}}})
	want := &cpv1.JoinGroupResponse{MemberId: "b", Generation: 2, Protocol: "range", Leader: "a",
		Members: []*cpv1.GroupMember{{MemberId: "a", GroupInstanceId: "i", Metadata: []byte{1}}}}
	if !proto.Equal(join, want) {
		t.Fatalf("join\n got  %v\n want %v", join, want)
	}
	sameValue(t, "sync", SyncGroupResponseFromCore(rebalance.SyncAnswer{Assignment: []byte{3}}).GetAssignment(), []byte{3})
	if !proto.Equal(HeartbeatAnswerToWire(nil), &cpv1.HeartbeatResponse{}) {
		t.Fatal("a heartbeat answer is an empty response")
	}
	errs := []runtime.ErrCode{runtime.OK, runtime.ErrUnknownMember}
	sameValue(t, "leave", mapSlice(LeaveGroupResponseFromCore(rebalance.LeaveAnswer{Errs: errs}).GetErrs(), ErrCodeToCore), errs)
	commit := contract.OffsetCommitResp{Errs: []runtime.ErrCode{runtime.OK, runtime.ErrUnknownPartition}}
	sameValue(t, "commit", OffsetCommitResponseToCore(CommitAnswerToWire(rebalance.CommitAnswer{OffsetCommitResp: commit})), commit)
	desc := contract.DescribeGroupResp{Exists: true, Generation: 4, ProtocolType: "consumer", Protocol: "range", Leader: "a",
		AssignedGeneration: 4, State: "Stable", Members: []contract.GroupMemberInfo{{
			GroupMember:  contract.GroupMember{MemberID: "a", ClientID: "c", ClientHost: "h", SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, Metadata: []byte{5}},
			JoinedTimeMs: 9, Assignment: []byte{6}}}}
	got := DescribeGroupsResponseFromCore(rebalance.DescribeAnswer{Groups: []rebalance.GroupDescription{{Name: "g", DescribeGroupResp: desc}}}).GetGroups()
	if len(got) != 1 || got[0].GetName() != "g" {
		t.Fatalf("describe: %v", got)
	}
	sameValue(t, "describe", GroupDetailsToCore(got[0].GetDetails()), desc)
	list := []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Empty"}}
	sameValue(t, "list", ListGroupsResponseToCore(ListAnswerToWire(rebalance.ListAnswer{Groups: list})).Groups, list)
}

// Nil requests give the zero input with the call id, never a panic.
func TestNilGroupRequestsToMachine(t *testing.T) {
	sameValue(t, "nil join", JoinToMachine(1, nil), rebalance.Join{Call: 1})
	sameValue(t, "nil sync", SyncToMachine(2, nil), rebalance.Sync{Call: 2})
	sameValue(t, "nil heartbeat", HeartbeatToMachine(3, nil), rebalance.Heartbeat{Call: 3})
	sameValue(t, "nil leave", LeaveToMachine(4, nil), rebalance.Leave{Call: 4})
	sameValue(t, "nil describe", DescribeToMachine(5, nil), rebalance.Describe{Call: 5})
}
