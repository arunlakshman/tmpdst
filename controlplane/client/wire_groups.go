package client

import (
	"slices"

	sharedv1 "github.com/manifesterio/mds/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

func joinGroupFromContract(r contract.GroupJoin) *cpv1.JoinGroupRequest {
	return &cpv1.JoinGroupRequest{
		Group: r.Group, MemberId: r.MemberID, GroupInstanceId: r.GroupInstanceID,
		ClientId: r.ClientID, ClientHost: r.ClientHost,
		SessionTimeoutMs: r.SessionTimeoutMs, RebalanceTimeoutMs: r.RebalanceTimeoutMs,
		ProtocolType: r.ProtocolType,
		Protocols: mapSlice(r.Protocols, func(p contract.GroupProtocol) *cpv1.GroupProtocol {
			return &cpv1.GroupProtocol{Name: p.Name, Metadata: p.Metadata}
		}),
	}
}

func joinGroupToContract(request *cpv1.JoinGroupRequest) contract.GroupJoin {
	return contract.GroupJoin{
		Group: request.GetGroup(), MemberID: request.GetMemberId(), GroupInstanceID: request.GetGroupInstanceId(),
		ClientID: request.GetClientId(), ClientHost: request.GetClientHost(),
		SessionTimeoutMs: request.GetSessionTimeoutMs(), RebalanceTimeoutMs: request.GetRebalanceTimeoutMs(),
		ProtocolType: request.GetProtocolType(),
		Protocols: mapSlice(request.GetProtocols(), func(p *cpv1.GroupProtocol) contract.GroupProtocol {
			return contract.GroupProtocol{Name: p.GetName(), Metadata: p.GetMetadata()}
		}),
	}
}

func joinGroupRespFromContract(r contract.GroupJoinResp) *cpv1.JoinGroupResponse {
	return &cpv1.JoinGroupResponse{
		MemberId: r.MemberID, Generation: r.Generation, Protocol: r.Protocol, Leader: r.Leader,
		Members: mapSlice(r.Members, func(m contract.GroupJoinMember) *cpv1.GroupMember {
			return &cpv1.GroupMember{MemberId: m.MemberID, GroupInstanceId: m.GroupInstanceID, Metadata: m.Metadata}
		}),
	}
}

func joinGroupRespToContract(r *cpv1.JoinGroupResponse) contract.GroupJoinResp {
	return contract.GroupJoinResp{
		MemberID: r.GetMemberId(), Generation: r.GetGeneration(), Protocol: r.GetProtocol(), Leader: r.GetLeader(),
		Members: mapSlice(r.GetMembers(), func(m *cpv1.GroupMember) contract.GroupJoinMember {
			return contract.GroupJoinMember{MemberID: m.GetMemberId(), GroupInstanceID: m.GetGroupInstanceId(), Metadata: m.GetMetadata()}
		}),
	}
}

func syncGroupFromContract(r contract.GroupSync) *cpv1.SyncGroupRequest {
	return &cpv1.SyncGroupRequest{
		Group: r.Group, MemberId: r.MemberID, Generation: r.Generation,
		Assignments: mapSlice(r.Assignments, func(a contract.GroupAssignment) *cpv1.GroupAssignment {
			return &cpv1.GroupAssignment{MemberId: a.Member, Assignment: a.Bytes}
		}),
	}
}

func syncGroupToContract(request *cpv1.SyncGroupRequest) contract.GroupSync {
	return contract.GroupSync{
		Group: request.GetGroup(), MemberID: request.GetMemberId(), Generation: request.GetGeneration(),
		Assignments: mapSlice(request.GetAssignments(), func(a *cpv1.GroupAssignment) contract.GroupAssignment {
			return contract.GroupAssignment{Member: a.GetMemberId(), Bytes: a.GetAssignment()}
		}),
	}
}

func syncGroupRespFromContract(r contract.GroupSyncResp) *cpv1.SyncGroupResponse {
	return &cpv1.SyncGroupResponse{Assignment: r.Assignment}
}

func syncGroupRespToContract(r *cpv1.SyncGroupResponse) contract.GroupSyncResp {
	return contract.GroupSyncResp{Assignment: r.GetAssignment()}
}

func heartbeatFromContract(r contract.GroupHeartbeat) *cpv1.HeartbeatRequest {
	return &cpv1.HeartbeatRequest{Group: r.Group, MemberId: r.MemberID, Generation: r.Generation}
}

func heartbeatToContract(request *cpv1.HeartbeatRequest) contract.GroupHeartbeat {
	return contract.GroupHeartbeat{Group: request.GetGroup(), MemberID: request.GetMemberId(), Generation: request.GetGeneration()}
}

func leaveGroupFromContract(r contract.GroupLeave) *cpv1.LeaveGroupRequest {
	return &cpv1.LeaveGroupRequest{Group: r.Group, Members: slices.Clone(r.Members)}
}

func leaveGroupToContract(request *cpv1.LeaveGroupRequest) contract.GroupLeave {
	return contract.GroupLeave{Group: request.GetGroup(), Members: slices.Clone(request.GetMembers())}
}

func leaveGroupRespFromContract(errs []contract.ErrCode) *cpv1.LeaveGroupResponse {
	return &cpv1.LeaveGroupResponse{Errs: mapSlice(errs, fromContractErr)}
}

func leaveGroupRespToContract(r *cpv1.LeaveGroupResponse) []contract.ErrCode {
	return mapSlice(r.GetErrs(), toContractErr)
}

func offsetCommitFromContract(operation contract.OffsetCommit) *cpv1.OffsetCommitRequest {
	return &cpv1.OffsetCommitRequest{
		Group: operation.Group, MemberId: operation.Member, Generation: operation.Generation,
		Offsets: mapSlice(operation.Offsets, partitionOffsetCommitFromContract),
	}
}

func offsetCommitToContract(request *cpv1.OffsetCommitRequest) contract.OffsetCommit {
	return contract.OffsetCommit{
		Group: request.GetGroup(), Member: request.GetMemberId(), Generation: request.GetGeneration(),
		Offsets: mapSlice(request.GetOffsets(), partitionOffsetCommitToContract),
	}
}

func offsetCommitRespFromContract(r contract.OffsetCommitResp) *cpv1.OffsetCommitResponse {
	return &cpv1.OffsetCommitResponse{Errs: mapSlice(r.Errs, fromContractErr)}
}

func offsetCommitRespToContract(r *cpv1.OffsetCommitResponse) contract.OffsetCommitResp {
	return contract.OffsetCommitResp{Errs: mapSlice(r.GetErrs(), toContractErr)}
}

func offsetFetchFromContract(operation contract.OffsetFetch) *cpv1.OffsetFetchRequest {
	return &cpv1.OffsetFetchRequest{
		Group:      operation.Group,
		Partitions: mapSlice(operation.Partitions, topicPartitionFromContract),
	}
}

func offsetFetchToContract(request *cpv1.OffsetFetchRequest) contract.OffsetFetch {
	return contract.OffsetFetch{
		Group:      request.GetGroup(),
		Partitions: mapSlice(request.GetPartitions(), topicPartitionToContract),
	}
}

func offsetFetchRespFromContract(r contract.OffsetFetchResp) *cpv1.OffsetFetchResponse {
	return &cpv1.OffsetFetchResponse{Offsets: mapSlice(r.Offsets, partitionOffsetFromContract)}
}

func offsetFetchRespToContract(r *cpv1.OffsetFetchResponse) contract.OffsetFetchResp {
	return contract.OffsetFetchResp{Offsets: mapSlice(r.GetOffsets(), partitionOffsetToContract)}
}

func partitionOffsetCommitFromContract(o contract.PartitionOffset) *cpv1.PartitionOffsetCommit {
	return &cpv1.PartitionOffsetCommit{
		Tp: topicPartitionFromContract(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

func partitionOffsetCommitToContract(o *cpv1.PartitionOffsetCommit) contract.PartitionOffset {
	return contract.PartitionOffset{
		TopicPartition: topicPartitionToContract(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
	}
}

func partitionOffsetFromContract(o contract.PartitionOffset) *sharedv1.PartitionOffset {
	return &sharedv1.PartitionOffset{
		Tp: topicPartitionFromContract(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

func partitionOffsetToContract(o *sharedv1.PartitionOffset) contract.PartitionOffset {
	return contract.PartitionOffset{
		TopicPartition: topicPartitionToContract(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
	}
}

func describeGroupsFromContract(names []string) *cpv1.DescribeGroupsRequest {
	return &cpv1.DescribeGroupsRequest{Groups: slices.Clone(names)}
}

func describeGroupsToContract(request *cpv1.DescribeGroupsRequest) []string {
	return slices.Clone(request.GetGroups())
}

func describeGroupsRespFromContract(groups []contract.GroupDescription) *cpv1.DescribeGroupsResponse {
	return &cpv1.DescribeGroupsResponse{Groups: mapSlice(groups, groupDescriptionFromContract)}
}

func describeGroupsRespToContract(r *cpv1.DescribeGroupsResponse) []contract.GroupDescription {
	return mapSlice(r.GetGroups(), groupDescriptionToContract)
}

func groupDescriptionFromContract(g contract.GroupDescription) *cpv1.GroupDescription {
	return &cpv1.GroupDescription{
		Name: g.Name,
		Details: &cpv1.GroupDetails{
			Exists: g.Exists, Generation: g.Generation, ProtocolType: g.ProtocolType,
			Protocol: g.Protocol, Leader: g.Leader, AssignedGeneration: g.AssignedGeneration, State: g.State,
			Members: mapSlice(g.Members, groupMemberInfoFromContract),
		},
	}
}

func groupDescriptionToContract(d *cpv1.GroupDescription) contract.GroupDescription {
	details := d.GetDetails()
	return contract.GroupDescription{
		Name: d.GetName(),
		DescribeGroupResp: contract.DescribeGroupResp{
			Exists: details.GetExists(), Generation: details.GetGeneration(), ProtocolType: details.GetProtocolType(),
			Protocol: details.GetProtocol(), Leader: details.GetLeader(), AssignedGeneration: details.GetAssignedGeneration(),
			State:   details.GetState(),
			Members: mapSlice(details.GetMembers(), groupMemberInfoToContract),
		},
	}
}

func groupMemberInfoFromContract(m contract.GroupMemberInfo) *cpv1.GroupMemberInfo {
	return &cpv1.GroupMemberInfo{
		Member: &cpv1.GroupMemberJoin{
			MemberId: m.MemberID, ClientId: m.ClientID, ClientHost: m.ClientHost,
			GroupInstanceId: m.GroupInstanceID, SessionTimeoutMs: m.SessionTimeoutMs,
			RebalanceTimeoutMs: m.RebalanceTimeoutMs, Metadata: m.Metadata,
		},
		JoinedTimeMs: m.JoinedTimeMs,
		Assignment:   m.Assignment,
	}
}

func groupMemberInfoToContract(m *cpv1.GroupMemberInfo) contract.GroupMemberInfo {
	member := m.GetMember()
	return contract.GroupMemberInfo{
		GroupMember: contract.GroupMember{
			MemberID: member.GetMemberId(), ClientID: member.GetClientId(), ClientHost: member.GetClientHost(),
			GroupInstanceID: member.GetGroupInstanceId(), SessionTimeoutMs: member.GetSessionTimeoutMs(),
			RebalanceTimeoutMs: member.GetRebalanceTimeoutMs(), Metadata: member.GetMetadata(),
		},
		JoinedTimeMs: m.GetJoinedTimeMs(),
		Assignment:   m.GetAssignment(),
	}
}

func listGroupsRespFromContract(groups []contract.GroupSummary) *cpv1.ListGroupsResponse {
	return &cpv1.ListGroupsResponse{Groups: mapSlice(groups, func(g contract.GroupSummary) *sharedv1.GroupSummary {
		return &sharedv1.GroupSummary{Name: g.Name, ProtocolType: g.ProtocolType, State: g.State}
	})}
}

func listGroupsRespToContract(r *cpv1.ListGroupsResponse) []contract.GroupSummary {
	return mapSlice(r.GetGroups(), func(g *sharedv1.GroupSummary) contract.GroupSummary {
		return contract.GroupSummary{Name: g.GetName(), ProtocolType: g.GetProtocolType(), State: g.GetState()}
	})
}
