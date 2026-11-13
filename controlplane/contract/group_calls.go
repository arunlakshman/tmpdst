package contract

// The group protocol calls of a broker: join, sync, heartbeat, leave and
// describe-many. The log-level group operations (GroupInstall, GroupAssign,
// GroupRemove) and the offset operations are in wire_types_gen.go. These calls
// are written by hand because the control plane answers them from the rebalance
// machine (domain/rebalance), which keeps its own input types with a call id.

// GroupProtocol is one protocol a join offers, with its metadata.
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// GroupJoin is a JoinGroup call (classic protocol). An empty MemberID asks for
// a new member. The answer comes when the join round ends, so the call can
// last a whole rebalance: the broker sets a long Timeout.
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
