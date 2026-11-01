package broker

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

// groupCP is a fake control plane for the group calls of one test. answer
// gives the answer to each call. Every call is recorded, with its deadline.
type groupCP struct {
	calls  []core.ControlPlaneCall
	answer func(op core.ControlPlaneOperation) core.ControlPlaneDone
}

func (g *groupCP) hook(a core.ControlPlaneCall, _ func() core.IOResult) core.IOResult {
	g.calls = append(g.calls, a)
	d := g.answer(a.Op)
	d.Req = a.Req
	return d
}

// cpOK is a successful answer. cpErr is a failed one.
func cpOK(resp core.ControlPlaneResponse) core.ControlPlaneDone {
	return core.ControlPlaneDone{Resp: resp}
}
func cpErr(code core.ErrCode) core.ControlPlaneDone { return core.ControlPlaneDone{Err: code} }

// withGroupCP starts a broker whose group calls are answered by answer.
func withGroupCP(t *testing.T, cfg Config, answer func(core.ControlPlaneOperation) core.ControlPlaneDone) (*harness, *groupCP) {
	t.Helper()
	h := startedHarness(t, cfg)
	g := &groupCP{answer: answer}
	h.cpHook = g.hook
	return h, g
}

func joinRequest(group string, session, rebalance int32) *kmsg.JoinGroupRequest {
	r := kmsg.NewPtrJoinGroupRequest()
	r.Group, r.SessionTimeoutMillis, r.RebalanceTimeoutMillis, r.ProtocolType = group, session, rebalance, "consumer"
	r.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("sub")}}
	return r
}

func syncRequest(group, member string, gen int32, assigns ...kmsg.SyncGroupRequestGroupAssignment) *kmsg.SyncGroupRequest {
	r := kmsg.NewPtrSyncGroupRequest()
	r.Group, r.Generation, r.MemberID, r.GroupAssignment = group, gen, member, assigns
	return r
}

func heartbeatRequest(group, member string, gen int32) *kmsg.HeartbeatRequest {
	r := kmsg.NewPtrHeartbeatRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	return r
}

func TestJoinGroupSuccess(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.GroupJoinResp{Generation: 4, Protocol: "range", Leader: "m-1", MemberID: "m-1",
			Members: []core.GroupJoinMember{{MemberID: "m-1", Metadata: []byte("sub")}}})
	})
	c := h.open(1)
	r := h.call(c, 7, joinRequest("g1", 10000, 30000)).(*kmsg.JoinGroupResponse)
	if r.ErrorCode != 0 || r.Generation != 4 || r.MemberID != "m-1" || r.LeaderID != "m-1" {
		t.Fatalf("join answer %+v", r)
	}
	if r.Protocol == nil || *r.Protocol != "range" || r.ProtocolType == nil || *r.ProtocolType != "consumer" {
		t.Fatalf("protocol %v %v", r.Protocol, r.ProtocolType)
	}
	if len(r.Members) != 1 || string(r.Members[0].ProtocolMetadata) != "sub" {
		t.Fatalf("members %+v", r.Members)
	}
	// The call is the join, with the client host from the peer and the
	// rebalance timeout plus the margin as its deadline.
	op := g.calls[0].Op.(core.GroupJoin)
	if op.Group != "g1" || op.ClientHost != "/test" || op.RebalanceTimeoutMs != 30000 || len(op.Protocols) != 1 {
		t.Fatalf("join call %+v", op)
	}
	if want := core.Duration(40 * time.Second); g.calls[0].Timeout != want {
		t.Fatalf("deadline %v, want 40s", time.Duration(g.calls[0].Timeout))
	}
	h.noViolations()
}

func TestJoinGroupVersion0UsesSessionTimeout(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.GroupJoinResp{MemberID: "m-2", Leader: "m-2", Generation: 1})
	})
	h.call(h.open(1), 0, joinRequest("g1", 10000, 0))
	if want := core.Duration(20 * time.Second); g.calls[0].Timeout != want { // session 10 s + margin
		t.Fatalf("deadline %v, want 20s", time.Duration(g.calls[0].Timeout))
	}
	h.noViolations()
}

func TestJoinGroupEmptyGroupIsInvalid(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpOK(nil) })
	r := h.call(h.open(1), 7, joinRequest("", 10000, 30000)).(*kmsg.JoinGroupResponse)
	if kafkaErrorCode(r.ErrorCode) != errInvalidGroupID || len(g.calls) != 0 {
		t.Fatalf("answer %d, calls %d", r.ErrorCode, len(g.calls))
	}
}

func TestSyncGroupLeaderAndFollower(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(op core.ControlPlaneOperation) core.ControlPlaneDone {
		if s, ok := op.(core.GroupSync); ok && s.MemberID == "m-1" {
			return cpOK(core.GroupSyncResp{})
		}
		return cpOK(core.GroupSyncResp{Assignment: []byte("for-m-2")})
	})
	c := h.open(1)
	leader := syncRequest("g1", "m-1", 4,
		kmsg.SyncGroupRequestGroupAssignment{MemberID: "m-1", MemberAssignment: []byte("for-m-1")},
		kmsg.SyncGroupRequestGroupAssignment{MemberID: "m-2", MemberAssignment: []byte("for-m-2")})
	if r := h.call(c, 5, leader).(*kmsg.SyncGroupResponse); r.ErrorCode != 0 {
		t.Fatalf("leader answer %d", r.ErrorCode)
	}
	if got := g.calls[0].Op.(core.GroupSync); len(got.Assignments) != 2 || got.Assignments[1].Member != "m-2" {
		t.Fatalf("leader call %+v", got)
	}
	if want := core.Duration(70 * time.Second); g.calls[0].Timeout != want {
		t.Fatalf("sync deadline %v, want 70s", time.Duration(g.calls[0].Timeout))
	}
	follower := syncRequest("g1", "m-2", 4)
	r := h.call(c, 5, follower).(*kmsg.SyncGroupResponse)
	if r.ErrorCode != 0 || string(r.MemberAssignment) != "for-m-2" {
		t.Fatalf("follower answer %+v", r)
	}
	h.noViolations()
}

func TestHeartbeatAnswers(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpOK(nil) })
	c := h.open(1)
	if r := h.call(c, 4, heartbeatRequest("g1", "m-1", 4)).(*kmsg.HeartbeatResponse); r.ErrorCode != 0 {
		t.Fatalf("heartbeat %d", r.ErrorCode)
	}
	if op := g.calls[0].Op.(core.GroupHeartbeat); op.Group != "g1" || op.Generation != 4 || g.calls[0].Timeout != 0 {
		t.Fatalf("heartbeat call %+v timeout %d", op, g.calls[0].Timeout)
	}
	h.noViolations()
}

// groupErrorTable is the Kafka code each control plane code maps to. A failure
// that is not a group decision becomes COORDINATOR_NOT_AVAILABLE.
var groupErrorTable = []struct {
	code core.ErrCode
	want kafkaErrorCode
}{
	{core.ErrUnknownMember, errUnknownMemberID},
	{core.ErrIllegalGeneration, errIllegalGeneration},
	{core.ErrRebalanceInProgress, errRebalanceInProgress},
	{core.ErrInconsistentGroupProtocol, errInconsistentProtocol},
	{core.ErrUnknownPartition, errUnknownTopicOrPart},
	{core.ErrInvalid, errInvalidRequest},
	{core.ErrTimeout, errCoordNotAvailable},
	{core.ErrUnavailable, errCoordNotAvailable},
	{core.ErrIO, errCoordNotAvailable},
	{core.ErrConnClosed, errCoordNotAvailable},
}

func TestGroupErrorCodesOnEveryCall(t *testing.T) {
	for _, tt := range groupErrorTable {
		t.Run(tt.code.String(), func(t *testing.T) {
			h, _ := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpErr(tt.code) })
			c := h.open(1)
			if r := h.call(c, 7, joinRequest("g1", 10000, 30000)).(*kmsg.JoinGroupResponse); kafkaErrorCode(r.ErrorCode) != tt.want {
				t.Errorf("join: %d, want %d", r.ErrorCode, tt.want)
			}
			if r := h.call(c, 5, syncRequest("g1", "m-1", 1)).(*kmsg.SyncGroupResponse); kafkaErrorCode(r.ErrorCode) != tt.want {
				t.Errorf("sync: %d, want %d", r.ErrorCode, tt.want)
			}
			if r := h.call(c, 4, heartbeatRequest("g1", "m-1", 1)).(*kmsg.HeartbeatResponse); kafkaErrorCode(r.ErrorCode) != tt.want {
				t.Errorf("heartbeat: %d, want %d", r.ErrorCode, tt.want)
			}
			h.noViolations()
		})
	}
}

func TestLeaveGroupVersion3CodesPerMember(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.GroupLeaveResp{Errs: []core.ErrCode{core.OK, core.ErrUnknownMember}})
	})
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = "g1"
	r.Members = []kmsg.LeaveGroupRequestMember{{MemberID: "m-1"}, {MemberID: "m-9"}}
	got := h.call(h.open(1), 3, r).(*kmsg.LeaveGroupResponse)
	if got.ErrorCode != 0 || len(got.Members) != 2 || got.Members[0].ErrorCode != 0 || got.Members[1].ErrorCode != 25 {
		t.Fatalf("leave answer %+v", got.Members)
	}
	if names := g.calls[0].Op.(core.GroupLeave).Members; len(names) != 2 || names[1] != "m-9" {
		t.Fatalf("leave call %v", names)
	}
	h.noViolations()
}

func TestLeaveGroupVersion0SingleMember(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone {
		return cpOK(core.GroupLeaveResp{Errs: []core.ErrCode{core.ErrUnknownMember}})
	})
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group, r.MemberID = "g1", "m-1"
	got := h.call(h.open(1), 0, r).(*kmsg.LeaveGroupResponse)
	if got.ErrorCode != 25 {
		t.Fatalf("top level error %d, want 25", got.ErrorCode)
	}
	if names := g.calls[0].Op.(core.GroupLeave).Members; len(names) != 1 || names[0] != "m-1" {
		t.Fatalf("leave call %v", names)
	}
	h.noViolations()
}

func TestLeaveGroupWithNoMemberSendsNothing(t *testing.T) {
	h, g := withGroupCP(t, DefaultConfig(), func(core.ControlPlaneOperation) core.ControlPlaneDone { return cpOK(nil) })
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = "g1"
	if got := h.call(h.open(1), 3, r).(*kmsg.LeaveGroupResponse); got.ErrorCode != 0 || len(g.calls) != 0 {
		t.Fatalf("answer %d, calls %d", got.ErrorCode, len(g.calls))
	}
}
