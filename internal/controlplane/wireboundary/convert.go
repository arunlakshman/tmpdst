package wireboundary

import (
	"slices"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/groups"
	"github.com/manifesterio/mds/internal/core"
)

// ---- Error and outcome codes ----

// errCodePairs is the one list that ties each core error code to its wire code.
// Both directions read it. To add a code, add one line here, plus the new
// constant in core and the new value in the proto enum. TestErrCodeTableIsComplete
// fails if a code is missing.
var errCodePairs = [...]struct {
	core core.ErrCode
	wire cpv1.ErrCode
}{
	{core.OK, cpv1.ErrCode_ERR_CODE_OK},
	{core.ErrTimeout, cpv1.ErrCode_ERR_CODE_TIMEOUT},
	{core.ErrUnavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
	{core.ErrNotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
	{core.ErrConflict, cpv1.ErrCode_ERR_CODE_CONFLICT},
	{core.ErrConnClosed, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
	{core.ErrTooLarge, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
	{core.ErrCorrupt, cpv1.ErrCode_ERR_CODE_CORRUPT},
	{core.ErrInvalid, cpv1.ErrCode_ERR_CODE_INVALID},
	{core.ErrIO, cpv1.ErrCode_ERR_CODE_IO},
	{core.ErrUnknownMember, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
	{core.ErrIllegalGeneration, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
	{core.ErrUnknownPartition, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
	{core.ErrRebalanceInProgress, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
	{core.ErrInconsistentGroupProtocol, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
	{core.ErrTopicExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
}

// ToCoreErr maps a wire ErrCode to core.ErrCode. Unknown values become ErrIO.
func ToCoreErr(c cpv1.ErrCode) core.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return core.ErrIO
}

// FromCoreErr maps core.ErrCode to the wire enum. Unknown values become ERR_CODE_IO.
func FromCoreErr(c core.ErrCode) cpv1.ErrCode {
	for _, pair := range errCodePairs {
		if pair.core == c {
			return pair.wire
		}
	}
	return cpv1.ErrCode_ERR_CODE_IO
}

// ToCoreOutcome maps a wire Outcome to core.Outcome. Unknown values become NotApplied.
func ToCoreOutcome(o cpv1.Outcome) core.Outcome {
	switch o {
	case cpv1.Outcome_OUTCOME_MAYBE_APPLIED:
		return core.MaybeApplied
	case cpv1.Outcome_OUTCOME_APPLIED:
		return core.Applied
	default:
		return core.NotApplied
	}
}

// FromCoreOutcome maps core.Outcome to the wire enum.
func FromCoreOutcome(o core.Outcome) cpv1.Outcome {
	switch o {
	case core.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case core.Applied:
		return cpv1.Outcome_OUTCOME_APPLIED
	default:
		return cpv1.Outcome_OUTCOME_NOT_APPLIED
	}
}

// ---- Shared pieces ----

// Rule for every "ToCore" function in this package: read the wire message with
// its Get methods. They accept a nil message and give the zero value, so a
// missing part becomes a zero value and nothing panics.

// mapSlice converts every element. An empty input gives nil, the same as a
// loop that appends to a nil slice.
func mapSlice[From, To any](in []From, convert func(From) To) []To {
	if len(in) == 0 {
		return nil
	}
	out := make([]To, len(in))
	for i, item := range in {
		out[i] = convert(item)
	}
	return out
}

func topicPartitionFromCore(tp core.TopicPartition) *cpv1.TopicPartition {
	return &cpv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionToCore(tp *cpv1.TopicPartition) core.TopicPartition {
	return core.NewTopicPartition(tp.GetTopic(), tp.GetPartition())
}

func segmentRefFromCore(ref core.SegmentRef) *cpv1.SegmentRef {
	return &cpv1.SegmentRef{Key: ref.Key, Off: ref.Off, Len: ref.Len}
}

func segmentRefToCore(ref *cpv1.SegmentRef) core.SegmentRef {
	return core.SegmentRef{Key: ref.GetKey(), Off: ref.GetOff(), Len: ref.GetLen()}
}

func pendingBatchFromCore(b core.PendingBatch) *cpv1.PendingBatch {
	return &cpv1.PendingBatch{
		Tp:      topicPartitionFromCore(b.TopicPartition),
		Records: b.Records,
		Loc:     segmentRefFromCore(b.Loc),
	}
}

func pendingBatchToCore(b *cpv1.PendingBatch) core.PendingBatch {
	return core.PendingBatch{
		TopicPartition: topicPartitionToCore(b.GetTp()),
		Records:        b.GetRecords(),
		Loc:            segmentRefToCore(b.GetLoc()),
	}
}

func batchRangeFromCore(br core.BatchRange) *cpv1.BatchRange {
	return &cpv1.BatchRange{
		Tp:    topicPartitionFromCore(br.TopicPartition),
		Base:  int64(br.Base),
		Count: br.Count,
		Loc:   segmentRefFromCore(br.Loc),
	}
}

func batchRangeToCore(br *cpv1.BatchRange) core.BatchRange {
	return core.BatchRange{
		TopicPartition: topicPartitionToCore(br.GetTp()),
		Base:           core.Offset(br.GetBase()),
		Count:          br.GetCount(),
		Loc:            segmentRefToCore(br.GetLoc()),
	}
}

func topicInfoFromCore(t core.TopicInfo) *cpv1.TopicInfo {
	return &cpv1.TopicInfo{Name: t.Name, Partitions: t.Partitions}
}

func topicInfoToCore(t *cpv1.TopicInfo) core.TopicInfo {
	return core.TopicInfo{Name: t.GetName(), Partitions: t.GetPartitions()}
}

func brokerInfoFromCore(b core.BrokerAddr) *cpv1.BrokerInfo {
	return &cpv1.BrokerInfo{NodeId: b.NodeID, Host: b.Host, Port: b.Port}
}

func brokerInfoToCore(b *cpv1.BrokerInfo) core.BrokerAddr {
	return core.BrokerAddr{NodeID: b.GetNodeId(), Host: b.GetHost(), Port: b.GetPort()}
}

// ---- Data calls ----

// CommitSegmentFromCore builds a CommitSegmentRequest from a core op.
func CommitSegmentFromCore(operation core.CommitSegment) *cpv1.CommitSegmentRequest {
	return &cpv1.CommitSegmentRequest{
		SegmentKey: operation.SegmentKey,
		Batches:    mapSlice(operation.Batches, pendingBatchFromCore),
	}
}

// CommitSegmentToCore rebuilds a core.CommitSegment from a request.
func CommitSegmentToCore(request *cpv1.CommitSegmentRequest) core.CommitSegment {
	return core.CommitSegment{
		SegmentKey: request.GetSegmentKey(),
		Batches:    mapSlice(request.GetBatches(), pendingBatchToCore),
	}
}

// LookupFromCore builds a LookupRequest from a core op.
func LookupFromCore(operation core.FindSegments) *cpv1.LookupRequest {
	return &cpv1.LookupRequest{
		Tp:       topicPartitionFromCore(operation.TopicPartition),
		From:     int64(operation.From),
		MaxBytes: operation.MaxBytes,
	}
}

// LookupToCore rebuilds a core.FindSegments from a request.
func LookupToCore(request *cpv1.LookupRequest) core.FindSegments {
	return core.FindSegments{
		TopicPartition: topicPartitionToCore(request.GetTp()),
		From:           core.Offset(request.GetFrom()),
		MaxBytes:       request.GetMaxBytes(),
	}
}

// DescribeTopicsFromCore builds a DescribeTopicsRequest from a core op.
func DescribeTopicsFromCore(operation core.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: slices.Clone(operation.Topics)}
}

// DescribeTopicsToCore rebuilds a core.DescribeTopics from a request.
func DescribeTopicsToCore(request *cpv1.DescribeTopicsRequest) core.DescribeTopics {
	return core.DescribeTopics{Topics: slices.Clone(request.GetTopics())}
}

// CommitSegmentRespFromCore builds a CommitSegmentResponse from a core response.
func CommitSegmentRespFromCore(r core.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	return &cpv1.CommitSegmentResponse{Assigned: mapSlice(r.Assigned, batchRangeFromCore)}
}

// CommitSegmentRespToCore rebuilds a core.CommitSegmentResp from a response.
func CommitSegmentRespToCore(r *cpv1.CommitSegmentResponse) core.CommitSegmentResp {
	return core.CommitSegmentResp{Assigned: mapSlice(r.GetAssigned(), batchRangeToCore)}
}

// LookupRespFromCore builds a LookupResponse from a core response.
func LookupRespFromCore(r core.FindSegmentsResp) *cpv1.LookupResponse {
	return &cpv1.LookupResponse{End: int64(r.End), Ranges: mapSlice(r.Ranges, batchRangeFromCore)}
}

// LookupRespToCore rebuilds a core.FindSegmentsResp from a response.
func LookupRespToCore(r *cpv1.LookupResponse) core.FindSegmentsResp {
	return core.FindSegmentsResp{End: core.Offset(r.GetEnd()), Ranges: mapSlice(r.GetRanges(), batchRangeToCore)}
}

// DescribeTopicsRespFromCore builds a DescribeTopicsResponse from a core response.
func DescribeTopicsRespFromCore(r core.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	return &cpv1.DescribeTopicsResponse{Topics: mapSlice(r.Topics, topicInfoFromCore)}
}

// DescribeTopicsRespToCore rebuilds a core.DescribeTopicsResp from a response.
func DescribeTopicsRespToCore(r *cpv1.DescribeTopicsResponse) core.DescribeTopicsResp {
	return core.DescribeTopicsResp{Topics: mapSlice(r.GetTopics(), topicInfoToCore)}
}

// ---- Broker heartbeat ----

// BrokerHeartbeatFromCore builds a BrokerHeartbeatRequest for a broker's own address.
func BrokerHeartbeatFromCore(b core.BrokerAddr) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: brokerInfoFromCore(b)}
}

// BrokerHeartbeatToCore rebuilds the calling broker's address from a request.
func BrokerHeartbeatToCore(request *cpv1.BrokerHeartbeatRequest) core.BrokerAddr {
	return brokerInfoToCore(request.GetBroker())
}

// BrokerHeartbeatRespFromCore builds a BrokerHeartbeatResponse from the live set.
func BrokerHeartbeatRespFromCore(live []core.BrokerAddr) *cpv1.BrokerHeartbeatResponse {
	return &cpv1.BrokerHeartbeatResponse{Brokers: mapSlice(live, brokerInfoFromCore)}
}

// BrokerHeartbeatRespToCore rebuilds the live set from a response.
func BrokerHeartbeatRespToCore(r *cpv1.BrokerHeartbeatResponse) []core.BrokerAddr {
	return mapSlice(r.GetBrokers(), brokerInfoToCore)
}

// ---- Consumer group calls ----

// JoinGroupFromCore builds a JoinGroupRequest from a join call.
func JoinGroupFromCore(r groups.JoinReq) *cpv1.JoinGroupRequest {
	return &cpv1.JoinGroupRequest{
		Group: r.Group, MemberId: r.MemberID, GroupInstanceId: r.GroupInstanceID,
		ClientId: r.ClientID, ClientHost: r.ClientHost,
		SessionTimeoutMs: r.SessionTimeoutMs, RebalanceTimeoutMs: r.RebalanceTimeoutMs,
		ProtocolType: r.ProtocolType,
		Protocols: mapSlice(r.Protocols, func(p groups.Protocol) *cpv1.GroupProtocol {
			return &cpv1.GroupProtocol{Name: p.Name, Metadata: p.Metadata}
		}),
	}
}

// JoinGroupToCore rebuilds a join call from a request.
func JoinGroupToCore(request *cpv1.JoinGroupRequest) groups.JoinReq {
	return groups.JoinReq{
		Group: request.GetGroup(), MemberID: request.GetMemberId(), GroupInstanceID: request.GetGroupInstanceId(),
		ClientID: request.GetClientId(), ClientHost: request.GetClientHost(),
		SessionTimeoutMs: request.GetSessionTimeoutMs(), RebalanceTimeoutMs: request.GetRebalanceTimeoutMs(),
		ProtocolType: request.GetProtocolType(),
		Protocols: mapSlice(request.GetProtocols(), func(p *cpv1.GroupProtocol) groups.Protocol {
			return groups.Protocol{Name: p.GetName(), Metadata: p.GetMetadata()}
		}),
	}
}

// JoinGroupRespFromCore builds a JoinGroupResponse. The caller checks Err first.
func JoinGroupRespFromCore(r groups.JoinResp) *cpv1.JoinGroupResponse {
	return &cpv1.JoinGroupResponse{
		MemberId: r.MemberID, Generation: r.Generation, Protocol: r.Protocol, Leader: r.Leader,
		Members: mapSlice(r.Members, func(m groups.Member) *cpv1.GroupMember {
			return &cpv1.GroupMember{MemberId: m.ID, GroupInstanceId: m.InstanceID, Metadata: m.Metadata}
		}),
	}
}

// JoinGroupRespToCore rebuilds a join answer from a response.
func JoinGroupRespToCore(r *cpv1.JoinGroupResponse) groups.JoinResp {
	return groups.JoinResp{
		MemberID: r.GetMemberId(), Generation: r.GetGeneration(), Protocol: r.GetProtocol(), Leader: r.GetLeader(),
		Members: mapSlice(r.GetMembers(), func(m *cpv1.GroupMember) groups.Member {
			return groups.Member{ID: m.GetMemberId(), InstanceID: m.GetGroupInstanceId(), Metadata: m.GetMetadata()}
		}),
	}
}

// SyncGroupFromCore builds a SyncGroupRequest from a sync call.
func SyncGroupFromCore(r groups.SyncReq) *cpv1.SyncGroupRequest {
	return &cpv1.SyncGroupRequest{
		Group: r.Group, MemberId: r.MemberID, Generation: r.Generation,
		Assignments: mapSlice(r.Assignments, func(a groups.Assignment) *cpv1.GroupAssignment {
			return &cpv1.GroupAssignment{MemberId: a.Member, Assignment: a.Bytes}
		}),
	}
}

// SyncGroupToCore rebuilds a sync call from a request.
func SyncGroupToCore(request *cpv1.SyncGroupRequest) groups.SyncReq {
	return groups.SyncReq{
		Group: request.GetGroup(), MemberID: request.GetMemberId(), Generation: request.GetGeneration(),
		Assignments: mapSlice(request.GetAssignments(), func(a *cpv1.GroupAssignment) groups.Assignment {
			return groups.Assignment{Member: a.GetMemberId(), Bytes: a.GetAssignment()}
		}),
	}
}

// SyncGroupRespFromCore builds a SyncGroupResponse. The caller checks Err first.
func SyncGroupRespFromCore(r groups.SyncResp) *cpv1.SyncGroupResponse {
	return &cpv1.SyncGroupResponse{Assignment: r.Assignment}
}

// SyncGroupRespToCore rebuilds a sync answer from a response.
func SyncGroupRespToCore(r *cpv1.SyncGroupResponse) groups.SyncResp {
	return groups.SyncResp{Assignment: r.GetAssignment()}
}

// HeartbeatFromCore builds a GroupHeartbeatRequest.
func HeartbeatFromCore(r groups.HeartbeatReq) *cpv1.GroupHeartbeatRequest {
	return &cpv1.GroupHeartbeatRequest{Group: r.Group, MemberId: r.MemberID, Generation: r.Generation}
}

// HeartbeatToCore rebuilds a heartbeat call from a request.
func HeartbeatToCore(request *cpv1.GroupHeartbeatRequest) groups.HeartbeatReq {
	return groups.HeartbeatReq{Group: request.GetGroup(), MemberID: request.GetMemberId(), Generation: request.GetGeneration()}
}

// LeaveGroupFromCore builds a LeaveGroupRequest.
func LeaveGroupFromCore(r groups.LeaveReq) *cpv1.LeaveGroupRequest {
	return &cpv1.LeaveGroupRequest{Group: r.Group, Members: slices.Clone(r.Members)}
}

// LeaveGroupToCore rebuilds a leave call from a request.
func LeaveGroupToCore(request *cpv1.LeaveGroupRequest) groups.LeaveReq {
	return groups.LeaveReq{Group: request.GetGroup(), Members: slices.Clone(request.GetMembers())}
}

// LeaveGroupRespFromCore builds a LeaveGroupResponse from one code per member.
func LeaveGroupRespFromCore(errs []core.ErrCode) *cpv1.LeaveGroupResponse {
	return &cpv1.LeaveGroupResponse{Errs: mapSlice(errs, FromCoreErr)}
}

// LeaveGroupRespToCore rebuilds the codes of a leave answer.
func LeaveGroupRespToCore(r *cpv1.LeaveGroupResponse) []core.ErrCode {
	return mapSlice(r.GetErrs(), ToCoreErr)
}

// OffsetCommitFromCore builds a GroupOffsetCommitRequest from a commit.
func OffsetCommitFromCore(operation core.OffsetCommit) *cpv1.GroupOffsetCommitRequest {
	return &cpv1.GroupOffsetCommitRequest{
		Group: operation.Group, MemberId: operation.Member, Generation: operation.Generation,
		Offsets: mapSlice(operation.Offsets, partitionOffsetCommitFromCore),
	}
}

// OffsetCommitToCore rebuilds a commit from a request. TimeMs is set by the coordinator.
func OffsetCommitToCore(request *cpv1.GroupOffsetCommitRequest) core.OffsetCommit {
	return core.OffsetCommit{
		Group: request.GetGroup(), Member: request.GetMemberId(), Generation: request.GetGeneration(),
		Offsets: mapSlice(request.GetOffsets(), partitionOffsetCommitToCore),
	}
}

// OffsetCommitRespFromCore builds a GroupOffsetCommitResponse from one code per partition.
func OffsetCommitRespFromCore(r core.OffsetCommitResp) *cpv1.GroupOffsetCommitResponse {
	return &cpv1.GroupOffsetCommitResponse{Errs: mapSlice(r.Errs, FromCoreErr)}
}

// OffsetCommitRespToCore rebuilds the codes of a commit answer.
func OffsetCommitRespToCore(r *cpv1.GroupOffsetCommitResponse) core.OffsetCommitResp {
	return core.OffsetCommitResp{Errs: mapSlice(r.GetErrs(), ToCoreErr)}
}

// OffsetFetchFromCore builds a GroupOffsetFetchRequest.
func OffsetFetchFromCore(operation core.OffsetFetch) *cpv1.GroupOffsetFetchRequest {
	return &cpv1.GroupOffsetFetchRequest{
		Group:      operation.Group,
		Partitions: mapSlice(operation.Partitions, topicPartitionFromCore),
	}
}

// OffsetFetchToCore rebuilds an offset fetch from a request.
func OffsetFetchToCore(request *cpv1.GroupOffsetFetchRequest) core.OffsetFetch {
	return core.OffsetFetch{
		Group:      request.GetGroup(),
		Partitions: mapSlice(request.GetPartitions(), topicPartitionToCore),
	}
}

// OffsetFetchRespFromCore builds a GroupOffsetFetchResponse.
func OffsetFetchRespFromCore(r core.OffsetFetchResp) *cpv1.GroupOffsetFetchResponse {
	return &cpv1.GroupOffsetFetchResponse{Offsets: mapSlice(r.Offsets, partitionOffsetFromCore)}
}

// OffsetFetchRespToCore rebuilds an offset fetch answer.
func OffsetFetchRespToCore(r *cpv1.GroupOffsetFetchResponse) core.OffsetFetchResp {
	return core.OffsetFetchResp{Offsets: mapSlice(r.GetOffsets(), partitionOffsetToCore)}
}

// The commit request and the fetch answer carry the same fields in two wire
// types, PartitionOffsetCommit and PartitionOffset.
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

func partitionOffsetFromCore(o core.PartitionOffset) *cpv1.PartitionOffset {
	return &cpv1.PartitionOffset{
		Tp: topicPartitionFromCore(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

func partitionOffsetToCore(o *cpv1.PartitionOffset) core.PartitionOffset {
	return core.PartitionOffset{
		TopicPartition: topicPartitionToCore(o.GetTp()), Offset: o.GetOffset(), LeaderEpoch: o.GetLeaderEpoch(), Metadata: o.GetMetadata(),
	}
}

// DescribeGroupsFromCore builds a DescribeGroupsRequest from group names.
func DescribeGroupsFromCore(names []string) *cpv1.DescribeGroupsRequest {
	return &cpv1.DescribeGroupsRequest{Groups: slices.Clone(names)}
}

// DescribeGroupsToCore rebuilds the group names of a request.
func DescribeGroupsToCore(request *cpv1.DescribeGroupsRequest) []string {
	return slices.Clone(request.GetGroups())
}

// DescribeGroupsRespFromCore builds a DescribeGroupsResponse.
func DescribeGroupsRespFromCore(groups []groups.GroupDescription) *cpv1.DescribeGroupsResponse {
	return &cpv1.DescribeGroupsResponse{Groups: mapSlice(groups, groupDescriptionFromCore)}
}

// DescribeGroupsRespToCore rebuilds the described groups from a response.
func DescribeGroupsRespToCore(r *cpv1.DescribeGroupsResponse) []groups.GroupDescription {
	return mapSlice(r.GetGroups(), groupDescriptionToCore)
}

func groupDescriptionFromCore(g groups.GroupDescription) *cpv1.GroupDescription {
	return &cpv1.GroupDescription{
		Name: g.Name, Exists: g.Exists, Generation: g.Generation, ProtocolType: g.ProtocolType,
		Protocol: g.Protocol, Leader: g.Leader, AssignedGeneration: g.AssignedGeneration, State: g.State,
		Members: mapSlice(g.Members, func(m core.GroupMemberInfo) *cpv1.GroupMemberInfo {
			return &cpv1.GroupMemberInfo{
				MemberId: m.MemberID, ClientId: m.ClientID, ClientHost: m.ClientHost,
				GroupInstanceId: m.GroupInstanceID, SessionTimeoutMs: m.SessionTimeoutMs,
				RebalanceTimeoutMs: m.RebalanceTimeoutMs, JoinedTimeMs: m.JoinedTimeMs,
				Metadata: m.Metadata, Assignment: m.Assignment,
			}
		}),
	}
}

// groupDescriptionToCore fills the embedded DescribeGroupResp by name:
// Go 1.26 does not allow promoted fields in a struct literal.
func groupDescriptionToCore(d *cpv1.GroupDescription) groups.GroupDescription {
	return groups.GroupDescription{
		Name: d.GetName(),
		DescribeGroupResp: core.DescribeGroupResp{
			Exists: d.GetExists(), Generation: d.GetGeneration(), ProtocolType: d.GetProtocolType(),
			Protocol: d.GetProtocol(), Leader: d.GetLeader(), AssignedGeneration: d.GetAssignedGeneration(),
			State: d.GetState(),
			Members: mapSlice(d.GetMembers(), func(m *cpv1.GroupMemberInfo) core.GroupMemberInfo {
				return core.GroupMemberInfo{
					GroupMember: core.GroupMember{
						MemberID: m.GetMemberId(), ClientID: m.GetClientId(), ClientHost: m.GetClientHost(),
						GroupInstanceID: m.GetGroupInstanceId(), SessionTimeoutMs: m.GetSessionTimeoutMs(),
						RebalanceTimeoutMs: m.GetRebalanceTimeoutMs(), Metadata: m.GetMetadata(),
					},
					JoinedTimeMs: m.GetJoinedTimeMs(),
					Assignment:   m.GetAssignment(),
				}
			}),
		},
	}
}

// ListGroupsRespFromCore builds a ListGroupsResponse.
func ListGroupsRespFromCore(groups []core.GroupSummary) *cpv1.ListGroupsResponse {
	return &cpv1.ListGroupsResponse{Groups: mapSlice(groups, func(g core.GroupSummary) *cpv1.GroupSummary {
		return &cpv1.GroupSummary{Name: g.Name, ProtocolType: g.ProtocolType, State: g.State}
	})}
}

// ListGroupsRespToCore rebuilds the group list from a response.
func ListGroupsRespToCore(r *cpv1.ListGroupsResponse) []core.GroupSummary {
	return mapSlice(r.GetGroups(), func(g *cpv1.GroupSummary) core.GroupSummary {
		return core.GroupSummary{Name: g.GetName(), ProtocolType: g.GetProtocolType(), State: g.GetState()}
	})
}
