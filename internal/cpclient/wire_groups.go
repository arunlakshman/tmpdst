package cpclient

import (
	"slices"

	sharedv1 "github.com/manifesterio/mds/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/api/controlplane/v1"
	"github.com/manifesterio/mds/internal/core"
)

func joinGroupFromCore(r core.GroupJoin) *cpv1.JoinGroupRequest {
	return &cpv1.JoinGroupRequest{
		Group: r.Group, MemberId: r.MemberID, GroupInstanceId: r.GroupInstanceID,
		ClientId: r.ClientID, ClientHost: r.ClientHost,
		SessionTimeoutMs: r.SessionTimeoutMs, RebalanceTimeoutMs: r.RebalanceTimeoutMs,
		ProtocolType: r.ProtocolType,
		Protocols: mapSlice(r.Protocols, func(p core.GroupProtocol) *cpv1.GroupProtocol {
			return &cpv1.GroupProtocol{Name: p.Name, Metadata: p.Metadata}
		}),
	}
}

func joinGroupToCore(request *cpv1.JoinGroupRequest) core.GroupJoin {
	return core.GroupJoin{
		Group: request.GetGroup(), MemberID: request.GetMemberId(), GroupInstanceID: request.GetGroupInstanceId(),
		ClientID: request.GetClientId(), ClientHost: request.GetClientHost(),
		SessionTimeoutMs: request.GetSessionTimeoutMs(), RebalanceTimeoutMs: request.GetRebalanceTimeoutMs(),
		ProtocolType: request.GetProtocolType(),
		Protocols: mapSlice(request.GetProtocols(), func(p *cpv1.GroupProtocol) core.GroupProtocol {
			return core.GroupProtocol{Name: p.GetName(), Metadata: p.GetMetadata()}
		}),
	}
}

func joinGroupRespFromCore(r core.GroupJoinResp) *cpv1.JoinGroupResponse {
	return &cpv1.JoinGroupResponse{
		MemberId: r.MemberID, Generation: r.Generation, Protocol: r.Protocol, Leader: r.Leader,
		Members: mapSlice(r.Members, func(m core.GroupJoinMember) *cpv1.GroupMember {
			return &cpv1.GroupMember{MemberId: m.MemberID, GroupInstanceId: m.GroupInstanceID, Metadata: m.Metadata}
		}),
	}
}

func joinGroupRespToCore(r *cpv1.JoinGroupResponse) core.GroupJoinResp {
	return core.GroupJoinResp{
		MemberID: r.GetMemberId(), Generation: r.GetGeneration(), Protocol: r.GetProtocol(), Leader: r.GetLeader(),
		Members: mapSlice(r.GetMembers(), func(m *cpv1.GroupMember) core.GroupJoinMember {
			return core.GroupJoinMember{MemberID: m.GetMemberId(), GroupInstanceID: m.GetGroupInstanceId(), Metadata: m.GetMetadata()}
		}),
	}
}

func syncGroupFromCore(r core.GroupSync) *cpv1.SyncGroupRequest {
	return &cpv1.SyncGroupRequest{
		Group: r.Group, MemberId: r.MemberID, Generation: r.Generation,
		Assignments: mapSlice(r.Assignments, func(a core.GroupAssignment) *cpv1.GroupAssignment {
			return &cpv1.GroupAssignment{MemberId: a.Member, Assignment: a.Bytes}
		}),
	}
}

func syncGroupToCore(request *cpv1.SyncGroupRequest) core.GroupSync {
	return core.GroupSync{
		Group: request.GetGroup(), MemberID: request.GetMemberId(), Generation: request.GetGeneration(),
		Assignments: mapSlice(request.GetAssignments(), func(a *cpv1.GroupAssignment) core.GroupAssignment {
			return core.GroupAssignment{Member: a.GetMemberId(), Bytes: a.GetAssignment()}
		}),
	}
}

func syncGroupRespFromCore(r core.GroupSyncResp) *cpv1.SyncGroupResponse {
	return &cpv1.SyncGroupResponse{Assignment: r.Assignment}
}

func syncGroupRespToCore(r *cpv1.SyncGroupResponse) core.GroupSyncResp {
	return core.GroupSyncResp{Assignment: r.GetAssignment()}
}

func heartbeatFromCore(r core.GroupHeartbeat) *cpv1.HeartbeatRequest {
	return &cpv1.HeartbeatRequest{Group: r.Group, MemberId: r.MemberID, Generation: r.Generation}
}

func heartbeatToCore(request *cpv1.HeartbeatRequest) core.GroupHeartbeat {
	return core.GroupHeartbeat{Group: request.GetGroup(), MemberID: request.GetMemberId(), Generation: request.GetGeneration()}
}

func leaveGroupFromCore(r core.GroupLeave) *cpv1.LeaveGroupRequest {
	return &cpv1.LeaveGroupRequest{Group: r.Group, Members: slices.Clone(r.Members)}
}

func leaveGroupToCore(request *cpv1.LeaveGroupRequest) core.GroupLeave {
	return core.GroupLeave{Group: request.GetGroup(), Members: slices.Clone(request.GetMembers())}
}

func leaveGroupRespFromCore(errs []core.ErrCode) *cpv1.LeaveGroupResponse {
	return &cpv1.LeaveGroupResponse{Errs: mapSlice(errs, fromCoreErr)}
}

func leaveGroupRespToCore(r *cpv1.LeaveGroupResponse) []core.ErrCode {
	return mapSlice(r.GetErrs(), toCoreErr)
}

func offsetCommitFromCore(operation core.OffsetCommit) *cpv1.OffsetCommitRequest {
	return &cpv1.OffsetCommitRequest{
		Group: operation.Group, MemberId: operation.Member, Generation: operation.Generation,
		Offsets: mapSlice(operation.Offsets, partitionOffsetCommitFromCore),
	}
}

func offsetCommitToCore(request *cpv1.OffsetCommitRequest) core.OffsetCommit {
	return core.OffsetCommit{
		Group: request.GetGroup(), Member: request.GetMemberId(), Generation: request.GetGeneration(),
		Offsets: mapSlice(request.GetOffsets(), partitionOffsetCommitToCore),
	}
}

func offsetCommitRespFromCore(r core.OffsetCommitResp) *cpv1.OffsetCommitResponse {
	return &cpv1.OffsetCommitResponse{Errs: mapSlice(r.Errs, fromCoreErr)}
}

func offsetCommitRespToCore(r *cpv1.OffsetCommitResponse) core.OffsetCommitResp {
	return core.OffsetCommitResp{Errs: mapSlice(r.GetErrs(), toCoreErr)}
}

func offsetFetchFromCore(operation core.OffsetFetch) *cpv1.OffsetFetchRequest {
	return &cpv1.OffsetFetchRequest{
		Group:      operation.Group,
		Partitions: mapSlice(operation.Partitions, topicPartitionFromCore),
	}
}

func offsetFetchToCore(request *cpv1.OffsetFetchRequest) core.OffsetFetch {
	return core.OffsetFetch{
		Group:      request.GetGroup(),
		Partitions: mapSlice(request.GetPartitions(), topicPartitionToCore),
	}
}

func offsetFetchRespFromCore(r core.OffsetFetchResp) *cpv1.OffsetFetchResponse {
	return &cpv1.OffsetFetchResponse{Offsets: mapSlice(r.Offsets, partitionOffsetFromCore)}
}

func offsetFetchRespToCore(r *cpv1.OffsetFetchResponse) core.OffsetFetchResp {
	return core.OffsetFetchResp{Offsets: mapSlice(r.GetOffsets(), partitionOffsetToCore)}
}

func partitionOffsetCommitFromCore(o core.PartitionOffset) *cpv1.PartitionOffsetCommit {
	return &cpv1.PartitionOffsetCommit{
		Tp: topicPartitionFromCore(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

func partitionOffsetCommitToCore(o *cpv1.PartitionOffsetCommit) core.PartitionOffset {
	return core.PartitionOffset{
		TopicPartition: topicPartitionToCore(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
	}
}

func partitionOffsetFromCore(o core.PartitionOffset) *sharedv1.PartitionOffset {
	return &sharedv1.PartitionOffset{
		Tp: topicPartitionFromCore(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

func partitionOffsetToCore(o *sharedv1.PartitionOffset) core.PartitionOffset {
	return core.PartitionOffset{
		TopicPartition: topicPartitionToCore(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
	}
}

func describeGroupsFromCore(names []string) *cpv1.DescribeGroupsRequest {
	return &cpv1.DescribeGroupsRequest{Groups: slices.Clone(names)}
}

func describeGroupsToCore(request *cpv1.DescribeGroupsRequest) []string {
	return slices.Clone(request.GetGroups())
}

func describeGroupsRespFromCore(groups []core.GroupDescription) *cpv1.DescribeGroupsResponse {
	return &cpv1.DescribeGroupsResponse{Groups: mapSlice(groups, groupDescriptionFromCore)}
}

func describeGroupsRespToCore(r *cpv1.DescribeGroupsResponse) []core.GroupDescription {
	return mapSlice(r.GetGroups(), groupDescriptionToCore)
}

func groupDescriptionFromCore(g core.GroupDescription) *cpv1.GroupDescription {
	return &cpv1.GroupDescription{
		Name: g.Name,
		Details: &cpv1.GroupDetails{
			Exists: g.Exists, Generation: g.Generation, ProtocolType: g.ProtocolType,
			Protocol: g.Protocol, Leader: g.Leader, AssignedGeneration: g.AssignedGeneration, State: g.State,
			Members: mapSlice(g.Members, groupMemberInfoFromCore),
		},
	}
}

func groupDescriptionToCore(d *cpv1.GroupDescription) core.GroupDescription {
	details := d.GetDetails()
	return core.GroupDescription{
		Name: d.GetName(),
		DescribeGroupResp: core.DescribeGroupResp{
			Exists: details.GetExists(), Generation: details.GetGeneration(), ProtocolType: details.GetProtocolType(),
			Protocol: details.GetProtocol(), Leader: details.GetLeader(), AssignedGeneration: details.GetAssignedGeneration(),
			State:   details.GetState(),
			Members: mapSlice(details.GetMembers(), groupMemberInfoToCore),
		},
	}
}

func groupMemberInfoFromCore(m core.GroupMemberInfo) *cpv1.GroupMemberInfo {
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

func groupMemberInfoToCore(m *cpv1.GroupMemberInfo) core.GroupMemberInfo {
	member := m.GetMember()
	return core.GroupMemberInfo{
		GroupMember: core.GroupMember{
			MemberID: member.GetMemberId(), ClientID: member.GetClientId(), ClientHost: member.GetClientHost(),
			GroupInstanceID: member.GetGroupInstanceId(), SessionTimeoutMs: member.GetSessionTimeoutMs(),
			RebalanceTimeoutMs: member.GetRebalanceTimeoutMs(), Metadata: member.GetMetadata(),
		},
		JoinedTimeMs: m.GetJoinedTimeMs(),
		Assignment:   m.GetAssignment(),
	}
}

func listGroupsRespFromCore(groups []core.GroupSummary) *cpv1.ListGroupsResponse {
	return &cpv1.ListGroupsResponse{Groups: mapSlice(groups, func(g core.GroupSummary) *sharedv1.GroupSummary {
		return &sharedv1.GroupSummary{Name: g.Name, ProtocolType: g.ProtocolType, State: g.State}
	})}
}

func listGroupsRespToCore(r *cpv1.ListGroupsResponse) []core.GroupSummary {
	return mapSlice(r.GetGroups(), func(g *sharedv1.GroupSummary) core.GroupSummary {
		return core.GroupSummary{Name: g.GetName(), ProtocolType: g.GetProtocolType(), State: g.GetState()}
	})
}
