package core

// ControlPlaneOperation is a control-plane operation as seen by a broker (decision 3). The wire
// protocol for new work is gRPC (`controlplane/api/v1`, decision 57); ControlPlaneOperation remains
// the in-process vocabulary until cutover. The metadata store is still invisible
// to the broker. The invariants below are not provisional.
//
// Brokers are stateless (decision 44): there is no registration, epoch or
// fencing, and no idempotent producers (decision 41). The control plane
// assigns offsets (decision 42): the broker never proposes one. How a store
// stays correct under concurrent committers (a compare-and-set on the run base
// with an internal retry) is the store's business, not part of this contract.
type ControlPlaneOperation interface{ isControlPlaneOperation() }

// ControlPlaneResponse is the answer to a ControlPlaneOperation.
type ControlPlaneResponse interface{ isControlPlaneResponse() }

// CommitSegment registers one uploaded object holding batches for many
// partitions and assigns their offsets atomically: every batch is assigned or
// none is.
//
// Each batch gets Base = the partition's next offset, the end of its newest
// run (0 for an empty log), in request order: a later batch of the same
// partition continues after the earlier one. Offsets are contiguous per
// partition and never reused, whatever the interleaving of committers.
//
// MUST be idempotent per SegmentKey: a replay of a committed key with an
// identical request returns the originally assigned ranges, never new ones
// (so a retry after MaybeApplied is safe); the same key with a different
// request is ErrConflict. That is the only use of ErrConflict.
type CommitSegment struct {
	SegmentKey string
	Batches    []PendingBatch
}

// PendingBatch describes one uploaded batch awaiting offset assignment.
type PendingBatch struct {
	TopicPartition TopicPartition
	Records        int64 // record count in the batch
	Loc            SegmentRef
}

// CommitSegmentResp has one entry per request batch, in request order.
type CommitSegmentResp struct{ Assigned []BatchRange }

// BatchRange is one batch of a partition's log: records [Base, Base+Count)
// stored at Loc.
type BatchRange struct {
	TopicPartition TopicPartition
	Base           Offset
	Count          int64
	Loc            SegmentRef
}

// FindSegments finds which segments hold offsets >= From for a partition.
type FindSegments struct {
	TopicPartition TopicPartition
	From           Offset
	MaxBytes       int32
}

// FindSegmentsResp lists the batches holding offsets >= From in offset order. End is
// the partition end offset (the next offset to be assigned, ListOffsets
// latest), derived from the newest run.
type FindSegmentsResp struct {
	Ranges []BatchRange
	End    Offset
}

// DescribeTopics answers Kafka Metadata requests.
type DescribeTopics struct{ Topics []string } // empty = all

// DescribeTopicsResp answers DescribeTopics.
type DescribeTopicsResp struct{ Topics []TopicInfo }

// TopicInfo describes one topic.
type TopicInfo struct {
	Name       string
	Partitions int32
}

// CreateTopic creates a topic with at least Partitions partitions. ErrInvalid
// for an empty name or Partitions <= 0. The reply has no response.
//
// With FailIfExists the create is exclusive: if the topic already exists the
// answer is ErrTopicExists and nothing changes. The check and the create are
// one step in the control plane, so two creates of the same new name cannot
// both succeed. Without it an existing topic is not an error, and the topic
// keeps the larger of its old and new partition counts.
type CreateTopic struct {
	Name         string
	Partitions   int32
	FailIfExists bool
}

// GroupMember is one member of a consumer group, as the join phase gives it.
// Metadata is the subscription of the protocol the group chose.
type GroupMember struct {
	MemberID           string
	ClientID           string
	ClientHost         string
	GroupInstanceID    string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	Metadata           []byte
}

// GroupMemberInfo is a member as the state keeps it: its join time and the
// assignment the leader stored for it (empty until GroupAssign).
type GroupMemberInfo struct {
	GroupMember
	JoinedTimeMs int64
	Assignment   []byte
}

// GroupInstall is the result of a join phase. It starts generation
// Generation with exactly Members and clears every assignment. Generation
// must be the current generation + 1 (a new group is at generation 0), else
// ErrIllegalGeneration. Members must be non-empty, Leader must be one of
// them, and member ids must be unique (else ErrInvalid). The reply has no
// response.
type GroupInstall struct {
	Group        string
	ProtocolType string
	Protocol     string
	Generation   int32
	Leader       string
	TimeMs       int64
	Members      []GroupMember
}

// GroupAssign stores the leader's assignment (SyncGroup) for generation
// Generation. Generation must be the current one (else ErrIllegalGeneration),
// Leader must be the group leader (else ErrUnknownMember), and every member
// in Assignments must be a member (else ErrUnknownMember). Every member
// gets the bytes listed for it, or none. The reply has no response.
type GroupAssign struct {
	Group       string
	Generation  int32
	Leader      string
	TimeMs      int64
	Assignments []GroupAssignment
}

// GroupAssignment is the assignment bytes of one member.
type GroupAssignment struct {
	Member string
	Bytes  []byte
}

// GroupRemove removes members from a group (a leave, or a logged expiry).
// Unknown members are ignored. Reason is "leave" or "expire" and is kept
// nowhere. The generation does not change. The reply has no response.
type GroupRemove struct {
	Group   string
	Members []string
	TimeMs  int64
	Reason  string
}

// OffsetCommit stores committed offsets for a group. Generation >= 0 is a
// managed commit: the group must exist (else ErrUnknownMember), Member must
// be a member (else ErrUnknownMember) and Generation must be the current one
// (else ErrIllegalGeneration). Generation -1 with an empty Member is a manual
// commit, allowed only when the group has no members (else
// ErrIllegalGeneration). Any other generation is ErrIllegalGeneration.
// Each offset is checked alone: an unknown partition gets ErrUnknownPartition
// and an offset below 0 gets ErrInvalid, and the others still commit.
type OffsetCommit struct {
	Group      string
	Member     string
	Generation int32
	TimeMs     int64
	Offsets    []PartitionOffset
}

// PartitionOffset is one committed offset of a group. A read of a partition
// that has no commit gives Offset -1 and LeaderEpoch -1.
type PartitionOffset struct {
	TopicPartition TopicPartition
	Offset         int64
	LeaderEpoch    int32
	Metadata       string
}

// OffsetCommitResp has one error per request offset, in request order.
type OffsetCommitResp struct{ Errs []ErrCode }

// OffsetFetch reads committed offsets of a group. Empty Partitions means
// every committed partition, sorted by topic and partition.
type OffsetFetch struct {
	Group      string
	Partitions []TopicPartition
}

// OffsetFetchResp has one entry per partition, in the order asked.
type OffsetFetchResp struct{ Offsets []PartitionOffset }

// DescribeGroup reads one group. A group that was never installed has
// Exists false. State is "Empty" with no members, "Stable" when the stored
// assignment is for the current generation, else "PreparingSync".
type DescribeGroup struct{ Group string }

// DescribeGroupResp answers DescribeGroup. Members are sorted by member id.
type DescribeGroupResp struct {
	Exists             bool
	Generation         int32
	ProtocolType       string
	Protocol           string
	Leader             string
	AssignedGeneration int32
	State              string
	Members            []GroupMemberInfo
}

// ListGroups lists every group that was ever installed.
type ListGroups struct{}

// ListGroupsResp answers ListGroups, sorted by group name.
type ListGroupsResp struct{ Groups []GroupSummary }

// GroupSummary is one line of ListGroupsResp.
type GroupSummary struct {
	Name         string
	ProtocolType string
	State        string
}

// GroupProtocol is one protocol a join offers, with its metadata.
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// GroupJoin is a JoinGroup call (classic protocol, decision Q9). An empty
// MemberID asks for a new member. The answer comes when the join round ends,
// so the call can last a whole rebalance: the broker sets a long Timeout.
type GroupJoin struct {
	Group              string
	MemberID           string
	GroupInstanceID    string
	ClientID           string
	ClientHost         string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	ProtocolType       string
	Protocols          []GroupProtocol
}

// GroupJoinMember is one member of a join answer. Only the leader gets the list.
type GroupJoinMember struct {
	MemberID        string
	GroupInstanceID string
	Metadata        []byte
}

// GroupJoinResp answers GroupJoin.
type GroupJoinResp struct {
	Generation int32
	Protocol   string
	Leader     string
	MemberID   string
	Members    []GroupJoinMember
}

// GroupSync is a SyncGroup call. The leader sends the assignments. A follower
// sends none and waits for its own. The answer comes when the leader has
// synced, so the call can wait for a rebalance too.
type GroupSync struct {
	Group       string
	MemberID    string
	Generation  int32
	Assignments []GroupAssignment
}

// GroupSyncResp answers GroupSync with the member's assignment.
type GroupSyncResp struct{ Assignment []byte }

// GroupHeartbeat renews a member's session. The reply has no response.
type GroupHeartbeat struct {
	Group      string
	MemberID   string
	Generation int32
}

// GroupLeave removes members that leave a group. Unknown members get
// ErrUnknownMember in their answer.
type GroupLeave struct {
	Group   string
	Members []string
}

// GroupLeaveResp has one code per member, in request order.
type GroupLeaveResp struct{ Errs []ErrCode }

// DescribeGroups reads several groups in one call.
type DescribeGroups struct{ Groups []string }

// GroupDescription is one group of a DescribeGroupsResp.
type GroupDescription struct {
	Name string
	DescribeGroupResp
}

// DescribeGroupsResp has one entry per name asked, in request order.
type DescribeGroupsResp struct{ Groups []GroupDescription }

func (CommitSegment) isControlPlaneOperation()  {}
func (FindSegments) isControlPlaneOperation()   {}
func (DescribeTopics) isControlPlaneOperation() {}
func (CreateTopic) isControlPlaneOperation()    {}
func (GroupInstall) isControlPlaneOperation()   {}
func (GroupAssign) isControlPlaneOperation()    {}
func (GroupRemove) isControlPlaneOperation()    {}
func (OffsetCommit) isControlPlaneOperation()   {}
func (OffsetFetch) isControlPlaneOperation()    {}
func (DescribeGroup) isControlPlaneOperation()  {}
func (ListGroups) isControlPlaneOperation()     {}
func (GroupJoin) isControlPlaneOperation()      {}
func (GroupSync) isControlPlaneOperation()      {}
func (GroupHeartbeat) isControlPlaneOperation() {}
func (GroupLeave) isControlPlaneOperation()     {}
func (DescribeGroups) isControlPlaneOperation() {}

func (CommitSegmentResp) isControlPlaneResponse()  {}
func (FindSegmentsResp) isControlPlaneResponse()   {}
func (DescribeTopicsResp) isControlPlaneResponse() {}
func (OffsetCommitResp) isControlPlaneResponse()   {}
func (OffsetFetchResp) isControlPlaneResponse()    {}
func (DescribeGroupResp) isControlPlaneResponse()  {}
func (ListGroupsResp) isControlPlaneResponse()     {}
func (GroupJoinResp) isControlPlaneResponse()      {}
func (GroupSyncResp) isControlPlaneResponse()      {}
func (GroupLeaveResp) isControlPlaneResponse()     {}
func (DescribeGroupsResp) isControlPlaneResponse() {}
