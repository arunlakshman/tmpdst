package rebalance

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// CallID names one RPC. The host gives each call its own id, unique in one
// run of the process. Every call gets one Reply, unless it is abandoned.
type CallID uint64

// The plain call types of the group protocol (Protocol, Member, Assignment) and
// the answers JoinAnswer, SyncAnswer, LeaveAnswer, DescribeAnswer and
// GroupDescription are generated from api/controlplane/v1/controlplane.proto
// into wire_types_gen.go. The inputs below carry a call id, so they are written
// by hand.

// The inputs (docs/rebalance.md section 3). TimerFired is runtime.TimerFired.
type (
	// Join is a JoinGroup call. An empty MemberID asks for a new member.
	// RebalanceTimeoutMs 0 means the session timeout.
	Join struct {
		Call               CallID
		Group              string
		MemberID           string
		GroupInstanceID    string
		ClientID           string
		ClientHost         string
		SessionTimeoutMs   int32
		RebalanceTimeoutMs int32
		ProtocolType       string
		Protocols          []Protocol // in the member's order of preference
	}
	// Sync is a SyncGroup call. Only the leader's Assignments are used.
	Sync struct {
		Call        CallID
		Group       string
		MemberID    string
		Generation  int32
		Assignments []Assignment
	}
	// Heartbeat is a Heartbeat call.
	Heartbeat struct {
		Call       CallID
		Group      string
		MemberID   string
		Generation int32
	}
	// Leave is a LeaveGroup call. The answer has one code per member.
	Leave struct {
		Call    CallID
		Group   string
		Members []string
	}
	// OffsetCommit is an OffsetCommit call. The machine sets Op.TimeMs.
	OffsetCommit struct {
		Call CallID
		Op   contract.OffsetCommit
	}
	// Describe is a DescribeGroups call.
	Describe struct {
		Call   CallID
		Groups []string
	}
	// List is a ListGroups call.
	List struct {
		Call CallID
	}
	// CallAbandoned says the RPC context of Call ended. The call gets no reply.
	CallAbandoned struct {
		Call CallID
	}
	// RoleChanged says whether the replica is now the active appender and Ready.
	RoleChanged struct {
		Active bool
	}
	// ReplicaReply is the replica's answer to a SubmitCommand or a SubmitRead.
	ReplicaReply struct {
		Req  machine.RequestID
		Done runtime.ControlPlaneDone
	}
)

// The outputs other than sim.SetTimer and sim.CancelTimer.
type (
	// SubmitCommand asks the replica to log Op: GroupInstall, GroupAssign,
	// GroupRemove or OffsetCommit. The answer comes back as a ReplicaReply.
	SubmitCommand struct {
		Req machine.RequestID
		Op  runtime.ControlPlaneOperation
	}
	// SubmitRead asks the replica to read Op: ListGroups or DescribeGroup.
	// Fresh false is a LocalRead. Fresh true is a read that waits for a fresh
	// check of the log tail (a ControlPlaneDeliver read).
	SubmitRead struct {
		Req   machine.RequestID
		Op    runtime.ControlPlaneOperation
		Fresh bool
	}
	// Reply answers Call. Body is nil when Err is not OK, and for a heartbeat.
	Reply struct {
		Call CallID
		Err  runtime.ErrCode
		Body Answer
	}
)

// Answer is the body of a Reply: JoinAnswer, SyncAnswer, LeaveAnswer,
// CommitAnswer, DescribeAnswer or ListAnswer.
type Answer interface{ isAnswer() }

type (
	// CommitAnswer has one code per offset of the call, in order.
	CommitAnswer struct {
		contract.OffsetCommitResp
	}
	// ListAnswer has every group in memory, by name.
	ListAnswer struct {
		Groups []contract.GroupSummary
	}
)

func (JoinAnswer) isAnswer()     {}
func (SyncAnswer) isAnswer()     {}
func (LeaveAnswer) isAnswer()    {}
func (CommitAnswer) isAnswer()   {}
func (DescribeAnswer) isAnswer() {}
func (ListAnswer) isAnswer()     {}
