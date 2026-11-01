package apiconvert

import (
	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/groupcoordinator"
	"manifester/internal/core"
)

// Group calls (slice 4b). Each pair maps one request or response both ways.

// JoinGroupFromCore builds a JoinGroupRequest from a join call.
func JoinGroupFromCore(r groupcoordinator.JoinReq) *cpv1.JoinGroupRequest {
	out := &cpv1.JoinGroupRequest{
		Group: r.Group, MemberId: r.MemberID, GroupInstanceId: r.GroupInstanceID,
		ClientId: r.ClientID, ClientHost: r.ClientHost,
		SessionTimeoutMs: r.SessionTimeoutMs, RebalanceTimeoutMs: r.RebalanceTimeoutMs,
		ProtocolType: r.ProtocolType,
	}
	for _, p := range r.Protocols {
		out.Protocols = append(out.Protocols, &cpv1.GroupProtocol{Name: p.Name, Metadata: p.Metadata})
	}
	return out
}

// JoinGroupToCore rebuilds a join call from a request.
func JoinGroupToCore(req *cpv1.JoinGroupRequest) groupcoordinator.JoinReq {
	out := groupcoordinator.JoinReq{
		Group: req.GetGroup(), MemberID: req.GetMemberId(), GroupInstanceID: req.GetGroupInstanceId(),
		ClientID: req.GetClientId(), ClientHost: req.GetClientHost(),
		SessionTimeoutMs: req.GetSessionTimeoutMs(), RebalanceTimeoutMs: req.GetRebalanceTimeoutMs(),
		ProtocolType: req.GetProtocolType(),
	}
	for _, p := range req.GetProtocols() {
		out.Protocols = append(out.Protocols, groupcoordinator.Protocol{Name: p.GetName(), Metadata: p.GetMetadata()})
	}
	return out
}

// JoinGroupRespFromCore builds a JoinGroupResponse. The caller checks Err first.
func JoinGroupRespFromCore(r groupcoordinator.JoinResp) *cpv1.JoinGroupResponse {
	out := &cpv1.JoinGroupResponse{
		MemberId: r.MemberID, Generation: r.Generation, Protocol: r.Protocol, Leader: r.Leader,
	}
	for _, m := range r.Members {
		out.Members = append(out.Members, &cpv1.GroupMember{MemberId: m.ID, GroupInstanceId: m.InstanceID, Metadata: m.Metadata})
	}
	return out
}

// JoinGroupRespToCore rebuilds a join answer from a response.
func JoinGroupRespToCore(r *cpv1.JoinGroupResponse) groupcoordinator.JoinResp {
	out := groupcoordinator.JoinResp{
		MemberID: r.GetMemberId(), Generation: r.GetGeneration(), Protocol: r.GetProtocol(), Leader: r.GetLeader(),
	}
	for _, m := range r.GetMembers() {
		out.Members = append(out.Members, groupcoordinator.Member{ID: m.GetMemberId(), InstanceID: m.GetGroupInstanceId(), Metadata: m.GetMetadata()})
	}
	return out
}

// SyncGroupFromCore builds a SyncGroupRequest from a sync call.
func SyncGroupFromCore(r groupcoordinator.SyncReq) *cpv1.SyncGroupRequest {
	out := &cpv1.SyncGroupRequest{Group: r.Group, MemberId: r.MemberID, Generation: r.Generation}
	for _, a := range r.Assignments {
		out.Assignments = append(out.Assignments, &cpv1.GroupAssignment{MemberId: a.Member, Assignment: a.Bytes})
	}
	return out
}

// SyncGroupToCore rebuilds a sync call from a request.
func SyncGroupToCore(req *cpv1.SyncGroupRequest) groupcoordinator.SyncReq {
	out := groupcoordinator.SyncReq{
		Group: req.GetGroup(), MemberID: req.GetMemberId(), Generation: req.GetGeneration(),
	}
	for _, a := range req.GetAssignments() {
		out.Assignments = append(out.Assignments, groupcoordinator.Assignment{Member: a.GetMemberId(), Bytes: a.GetAssignment()})
	}
	return out
}

// SyncGroupRespFromCore builds a SyncGroupResponse. The caller checks Err first.
func SyncGroupRespFromCore(r groupcoordinator.SyncResp) *cpv1.SyncGroupResponse {
	return &cpv1.SyncGroupResponse{Assignment: r.Assignment}
}

// SyncGroupRespToCore rebuilds a sync answer from a response.
func SyncGroupRespToCore(r *cpv1.SyncGroupResponse) groupcoordinator.SyncResp {
	return groupcoordinator.SyncResp{Assignment: r.GetAssignment()}
}

// HeartbeatFromCore builds a GroupHeartbeatRequest.
func HeartbeatFromCore(r groupcoordinator.HeartbeatReq) *cpv1.GroupHeartbeatRequest {
	return &cpv1.GroupHeartbeatRequest{Group: r.Group, MemberId: r.MemberID, Generation: r.Generation}
}

// HeartbeatToCore rebuilds a heartbeat call from a request.
func HeartbeatToCore(req *cpv1.GroupHeartbeatRequest) groupcoordinator.HeartbeatReq {
	return groupcoordinator.HeartbeatReq{Group: req.GetGroup(), MemberID: req.GetMemberId(), Generation: req.GetGeneration()}
}

// LeaveGroupFromCore builds a LeaveGroupRequest.
func LeaveGroupFromCore(r groupcoordinator.LeaveReq) *cpv1.LeaveGroupRequest {
	return &cpv1.LeaveGroupRequest{Group: r.Group, Members: append([]string(nil), r.Members...)}
}

// LeaveGroupToCore rebuilds a leave call from a request.
func LeaveGroupToCore(req *cpv1.LeaveGroupRequest) groupcoordinator.LeaveReq {
	return groupcoordinator.LeaveReq{Group: req.GetGroup(), Members: append([]string(nil), req.GetMembers()...)}
}

// LeaveGroupRespFromCore builds a LeaveGroupResponse from one code per member.
func LeaveGroupRespFromCore(errs []core.ErrCode) *cpv1.LeaveGroupResponse {
	out := &cpv1.LeaveGroupResponse{}
	for _, c := range errs {
		out.Errs = append(out.Errs, FromCoreErr(c))
	}
	return out
}

// LeaveGroupRespToCore rebuilds the codes of a leave answer.
func LeaveGroupRespToCore(r *cpv1.LeaveGroupResponse) []core.ErrCode {
	var out []core.ErrCode
	for _, c := range r.GetErrs() {
		out = append(out, ToCoreErr(c))
	}
	return out
}

// OffsetCommitFromCore builds a GroupOffsetCommitRequest from a commit.
func OffsetCommitFromCore(op core.OffsetCommit) *cpv1.GroupOffsetCommitRequest {
	out := &cpv1.GroupOffsetCommitRequest{Group: op.Group, MemberId: op.Member, Generation: op.Generation}
	for _, o := range op.Offsets {
		out.Offsets = append(out.Offsets, &cpv1.PartitionOffsetCommit{
			Tp: topicPartitionFromCore(o.TP), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
		})
	}
	return out
}

// OffsetCommitToCore rebuilds a commit from a request. TimeMs is set by the coordinator.
func OffsetCommitToCore(req *cpv1.GroupOffsetCommitRequest) core.OffsetCommit {
	out := core.OffsetCommit{Group: req.GetGroup(), Member: req.GetMemberId(), Generation: req.GetGeneration()}
	for _, o := range req.GetOffsets() {
		out.Offsets = append(out.Offsets, core.PartitionOffset{
			TP: topicPartitionToCore(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
		})
	}
	return out
}

// OffsetCommitRespFromCore builds a GroupOffsetCommitResponse from one code per partition.
func OffsetCommitRespFromCore(r core.OffsetCommitResp) *cpv1.GroupOffsetCommitResponse {
	out := &cpv1.GroupOffsetCommitResponse{}
	for _, c := range r.Errs {
		out.Errs = append(out.Errs, FromCoreErr(c))
	}
	return out
}

// OffsetCommitRespToCore rebuilds the codes of a commit answer.
func OffsetCommitRespToCore(r *cpv1.GroupOffsetCommitResponse) core.OffsetCommitResp {
	var out core.OffsetCommitResp
	for _, c := range r.GetErrs() {
		out.Errs = append(out.Errs, ToCoreErr(c))
	}
	return out
}

// OffsetFetchFromCore builds a GroupOffsetFetchRequest.
func OffsetFetchFromCore(op core.OffsetFetch) *cpv1.GroupOffsetFetchRequest {
	out := &cpv1.GroupOffsetFetchRequest{Group: op.Group}
	for _, tp := range op.Partitions {
		out.Partitions = append(out.Partitions, topicPartitionFromCore(tp))
	}
	return out
}

// OffsetFetchToCore rebuilds an offset fetch from a request.
func OffsetFetchToCore(req *cpv1.GroupOffsetFetchRequest) core.OffsetFetch {
	out := core.OffsetFetch{Group: req.GetGroup()}
	for _, tp := range req.GetPartitions() {
		out.Partitions = append(out.Partitions, topicPartitionToCore(tp))
	}
	return out
}

// OffsetFetchRespFromCore builds a GroupOffsetFetchResponse.
func OffsetFetchRespFromCore(r core.OffsetFetchResp) *cpv1.GroupOffsetFetchResponse {
	out := &cpv1.GroupOffsetFetchResponse{}
	for _, o := range r.Offsets {
		out.Offsets = append(out.Offsets, partitionOffsetFromCore(o))
	}
	return out
}

// OffsetFetchRespToCore rebuilds an offset fetch answer.
func OffsetFetchRespToCore(r *cpv1.GroupOffsetFetchResponse) core.OffsetFetchResp {
	var out core.OffsetFetchResp
	for _, o := range r.GetOffsets() {
		out.Offsets = append(out.Offsets, partitionOffsetToCore(o))
	}
	return out
}

// DescribeGroupsFromCore builds a DescribeGroupsRequest from group names.
func DescribeGroupsFromCore(names []string) *cpv1.DescribeGroupsRequest {
	return &cpv1.DescribeGroupsRequest{Groups: append([]string(nil), names...)}
}

// DescribeGroupsToCore rebuilds the group names of a request.
func DescribeGroupsToCore(req *cpv1.DescribeGroupsRequest) []string {
	return append([]string(nil), req.GetGroups()...)
}

// DescribeGroupsRespFromCore builds a DescribeGroupsResponse.
func DescribeGroupsRespFromCore(gs []groupcoordinator.GroupDescription) *cpv1.DescribeGroupsResponse {
	out := &cpv1.DescribeGroupsResponse{}
	for _, g := range gs {
		d := &cpv1.GroupDescription{
			Name: g.Name, Exists: g.Exists, Generation: g.Generation, ProtocolType: g.ProtocolType,
			Protocol: g.Protocol, Leader: g.Leader, AssignedGeneration: g.AssignedGeneration, State: g.State,
		}
		for _, m := range g.Members {
			d.Members = append(d.Members, &cpv1.GroupMemberInfo{
				MemberId: m.MemberID, ClientId: m.ClientID, ClientHost: m.ClientHost,
				GroupInstanceId: m.GroupInstanceID, SessionTimeoutMs: m.SessionTimeoutMs,
				RebalanceTimeoutMs: m.RebalanceTimeoutMs, JoinedTimeMs: m.JoinedTimeMs,
				Metadata: m.Metadata, Assignment: m.Assignment,
			})
		}
		out.Groups = append(out.Groups, d)
	}
	return out
}

// DescribeGroupsRespToCore rebuilds the described groups from a response.
func DescribeGroupsRespToCore(r *cpv1.DescribeGroupsResponse) []groupcoordinator.GroupDescription {
	var out []groupcoordinator.GroupDescription
	for _, d := range r.GetGroups() {
		g := groupcoordinator.GroupDescription{Name: d.GetName()}
		g.Exists = d.GetExists()
		g.Generation = d.GetGeneration()
		g.ProtocolType = d.GetProtocolType()
		g.Protocol = d.GetProtocol()
		g.Leader = d.GetLeader()
		g.AssignedGeneration = d.GetAssignedGeneration()
		g.State = d.GetState()
		for _, m := range d.GetMembers() {
			g.Members = append(g.Members, core.GroupMemberInfo{
				GroupMember: core.GroupMember{
					MemberID: m.GetMemberId(), ClientID: m.GetClientId(), ClientHost: m.GetClientHost(),
					GroupInstanceID: m.GetGroupInstanceId(), SessionTimeoutMs: m.GetSessionTimeoutMs(),
					RebalanceTimeoutMs: m.GetRebalanceTimeoutMs(), Metadata: m.GetMetadata(),
				},
				JoinedTimeMs: m.GetJoinedTimeMs(),
				Assignment:   m.GetAssignment(),
			})
		}
		out = append(out, g)
	}
	return out
}

// ListGroupsRespFromCore builds a ListGroupsResponse.
func ListGroupsRespFromCore(gs []core.GroupSummary) *cpv1.ListGroupsResponse {
	out := &cpv1.ListGroupsResponse{}
	for _, g := range gs {
		out.Groups = append(out.Groups, &cpv1.GroupSummary{Name: g.Name, ProtocolType: g.ProtocolType, State: g.State})
	}
	return out
}

// ListGroupsRespToCore rebuilds the group list from a response.
func ListGroupsRespToCore(r *cpv1.ListGroupsResponse) []core.GroupSummary {
	var out []core.GroupSummary
	for _, g := range r.GetGroups() {
		out = append(out, core.GroupSummary{Name: g.GetName(), ProtocolType: g.GetProtocolType(), State: g.GetState()})
	}
	return out
}

func topicPartitionFromCore(tp core.TopicPartition) *cpv1.TopicPartition {
	return &cpv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionToCore(tp *cpv1.TopicPartition) core.TopicPartition {
	return core.NewTopicPartition(tp.GetTopic(), tp.GetPartition())
}

func partitionOffsetFromCore(o core.PartitionOffset) *cpv1.PartitionOffset {
	return &cpv1.PartitionOffset{
		Tp: topicPartitionFromCore(o.TP), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

func partitionOffsetToCore(o *cpv1.PartitionOffset) core.PartitionOffset {
	return core.PartitionOffset{
		TP: topicPartitionToCore(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
	}
}
