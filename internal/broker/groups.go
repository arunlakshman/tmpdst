package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

// Group APIs (decision Q3). The broker keeps no group state. Each call is one
// control plane call, and every broker serves every group call. Classic
// protocol only (decision Q9).
//
// A group call is an operation of kind opGroup. It has no retry: a retry of a
// JoinGroup would make a second member. A control plane failure is answered
// as COORDINATOR_NOT_AVAILABLE, so the client retries on its own.

// groupCallMargin is added to a rebalance timeout for the control plane deadline,
// so the control plane answers (with its own timeout) before the broker gives up.
const groupCallMargin = 10 * core.Second

// syncWait is the wait a SyncGroup allows for the leader. The broker keeps no
// group state, so it does not know the group's rebalance timeout and uses this.
const syncWait = 60 * core.Second

// dispatchGroup routes the group APIs. It reports false for other requests.
func (b *Broker) dispatchGroup(s *slot, req core.Request) ([]core.Action, bool) {
	switch r := req.(type) {
	case *kmsg.FindCoordinatorRequest:
		return b.findCoordinator(s, r), true
	case *kmsg.JoinGroupRequest:
		return b.joinGroup(s, r), true
	case *kmsg.SyncGroupRequest:
		return b.syncGroup(s, r), true
	case *kmsg.HeartbeatRequest:
		return b.heartbeat(s, r), true
	case *kmsg.LeaveGroupRequest:
		return b.leaveGroup(s, r), true
	case *kmsg.OffsetCommitRequest:
		return b.offsetCommit(s, r), true
	case *kmsg.OffsetFetchRequest:
		return b.offsetFetch(s, r), true
	case *kmsg.DescribeGroupsRequest:
		return b.describeGroups(s, r), true
	case *kmsg.ListGroupsRequest:
		return b.listGroups(s, r), true
	}
	return nil, false
}

// groupCall starts one group call as a part of slot s. fill writes the answer
// into the response when the control plane replies. timeout 0 takes the default.
func (b *Broker) groupCall(s *slot, op core.ControlPlaneOperation, timeout core.Duration, fill func(core.ControlPlaneDone)) []core.Action {
	return b.startOp(&operation{kind: opGroup, s: s, gop: op, gtimeout: timeout, gfill: fill})
}

// onGroupCP answers a group call that the control plane replied to.
func (b *Broker) onGroupCP(o *operation, e core.ControlPlaneDone) []core.Action {
	o.gfill(e)
	return b.endOp(o)
}

// failGroupCalls answers the group calls still at the control plane when the
// broker stops, as if the control plane were down. The late answers are then
// ignored: the operations are gone, so findOp finds nothing.
func (b *Broker) failGroupCalls() []core.Action {
	var acts []core.Action
	for _, o := range append([]*operation(nil), b.ops...) {
		if o.kind == opGroup && o.state == opControlPlane {
			o.gfill(core.ControlPlaneDone{Err: core.ErrUnavailable})
			acts = append(acts, b.endOp(o)...)
		}
	}
	return acts
}

// groupErrCode maps the control plane code of a group call to a Kafka code.
// A failure that is not a group decision (a timeout, an unavailable control
// plane) becomes COORDINATOR_NOT_AVAILABLE, so the client retries.
func groupErrCode(c core.ErrCode) kafkaErrorCode {
	switch c {
	case core.OK:
		return errNone
	case core.ErrUnknownMember:
		return errUnknownMemberID
	case core.ErrIllegalGeneration:
		return errIllegalGeneration
	case core.ErrRebalanceInProgress:
		return errRebalanceInProgress
	case core.ErrInconsistentGroupProtocol:
		return errInconsistentProtocol
	case core.ErrUnknownPartition:
		return errUnknownTopicOrPart
	case core.ErrInvalid:
		return errInvalidRequest
	}
	return errCoordNotAvailable
}

// joinGroup answers JoinGroup (key 11). The call waits for the join round to
// end, so it gets the rebalance timeout as its deadline, plus a margin. Version
// 0 has no rebalance timeout, so the session timeout is used.
func (b *Broker) joinGroup(s *slot, r *kmsg.JoinGroupRequest) []core.Action {
	resp := kmsg.NewPtrJoinGroupResponse()
	s.resp = resp
	if r.Group == "" {
		resp.ErrorCode = int16(errInvalidGroupID)
		return nil
	}
	rebalance := r.RebalanceTimeoutMillis
	if r.Version == 0 {
		rebalance = r.SessionTimeoutMillis
	}
	call := core.GroupJoin{
		Group: r.Group, MemberID: r.MemberID, GroupInstanceID: derefOr(r.InstanceID),
		ClientID: s.h.ClientID, ClientHost: s.conn.host,
		SessionTimeoutMs: r.SessionTimeoutMillis, RebalanceTimeoutMs: rebalance, ProtocolType: r.ProtocolType,
	}
	for _, p := range r.Protocols {
		call.Protocols = append(call.Protocols, core.GroupProtocol{Name: p.Name, Metadata: p.Metadata})
	}
	fill := func(e core.ControlPlaneDone) {
		resp.ErrorCode = int16(groupErrCode(e.Err))
		if e.Err != core.OK {
			return
		}
		j := core.ResponseAs[core.GroupJoinResp](e)
		resp.Generation, resp.LeaderID, resp.MemberID = j.Generation, j.Leader, j.MemberID
		resp.ProtocolType, resp.Protocol = new(r.ProtocolType), new(j.Protocol)
		for _, m := range j.Members {
			resp.Members = append(resp.Members, kmsg.JoinGroupResponseMember{
				MemberID: m.MemberID, InstanceID: optStr(m.GroupInstanceID), ProtocolMetadata: m.Metadata})
		}
	}
	return b.groupCall(s, call, millisDuration(rebalance)+groupCallMargin, fill)
}

// syncGroup answers SyncGroup (key 14). The leader's call stores the
// assignments. A follower's call waits for the leader, so it gets syncWait as
// its deadline, plus a margin.
func (b *Broker) syncGroup(s *slot, r *kmsg.SyncGroupRequest) []core.Action {
	resp := kmsg.NewPtrSyncGroupResponse()
	s.resp = resp
	if r.Group == "" {
		resp.ErrorCode = int16(errInvalidGroupID)
		return nil
	}
	call := core.GroupSync{Group: r.Group, MemberID: r.MemberID, Generation: r.Generation}
	for _, a := range r.GroupAssignment {
		call.Assignments = append(call.Assignments, core.GroupAssignment{Member: a.MemberID, Bytes: a.MemberAssignment})
	}
	fill := func(e core.ControlPlaneDone) {
		resp.ErrorCode = int16(groupErrCode(e.Err))
		if e.Err == core.OK {
			resp.MemberAssignment = core.ResponseAs[core.GroupSyncResp](e).Assignment
			resp.ProtocolType, resp.Protocol = r.ProtocolType, r.Protocol
		}
	}
	return b.groupCall(s, call, syncWait+groupCallMargin, fill)
}

// heartbeat answers Heartbeat (key 12). A rebalance answers REBALANCE_IN_PROGRESS,
// which tells the member to rejoin.
func (b *Broker) heartbeat(s *slot, r *kmsg.HeartbeatRequest) []core.Action {
	resp := kmsg.NewPtrHeartbeatResponse()
	s.resp = resp
	if r.Group == "" {
		resp.ErrorCode = int16(errInvalidGroupID)
		return nil
	}
	call := core.GroupHeartbeat{Group: r.Group, MemberID: r.MemberID, Generation: r.Generation}
	fill := func(e core.ControlPlaneDone) { resp.ErrorCode = int16(groupErrCode(e.Err)) }
	return b.groupCall(s, call, 0, fill)
}

// leaveGroup answers LeaveGroup (key 13). Version 0 to 2 name one member
// (MemberID). Version 3 and up name a list, and each member gets its own code.
func (b *Broker) leaveGroup(s *slot, r *kmsg.LeaveGroupRequest) []core.Action {
	resp := kmsg.NewPtrLeaveGroupResponse()
	s.resp = resp
	if r.Group == "" {
		resp.ErrorCode = int16(errInvalidGroupID)
		return nil
	}
	var names []string
	if r.Version < 3 {
		resp.Members = []kmsg.LeaveGroupResponseMember{{MemberID: r.MemberID}}
		names = []string{r.MemberID}
	}
	for _, m := range r.Members {
		resp.Members = append(resp.Members, kmsg.LeaveGroupResponseMember{MemberID: m.MemberID, InstanceID: m.InstanceID})
		names = append(names, m.MemberID)
	}
	if len(names) == 0 {
		return nil
	}
	call := core.GroupLeave{Group: r.Group, Members: names}
	fill := func(e core.ControlPlaneDone) {
		codes := core.ResponseAs[core.GroupLeaveResp](e).Errs
		for i := range resp.Members {
			resp.Members[i].ErrorCode = int16(groupErrCode(leaveCode(e, codes, i)))
		}
		if r.Version < 3 { // version 0 to 2 has one top level code
			resp.ErrorCode = resp.Members[0].ErrorCode
		}
	}
	return b.groupCall(s, call, 0, fill)
}

// leaveCode is the code of member i: the group failure, else the member's code.
func leaveCode(e core.ControlPlaneDone, codes []core.ErrCode, i int) core.ErrCode {
	switch {
	case e.Err != core.OK:
		return e.Err
	case i < len(codes):
		return codes[i]
	}
	return core.ErrIO
}

// derefOr returns the string behind p, or "" when p is nil.
func derefOr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// optStr is s as a nullable string: nil when s is empty.
func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func millisDuration(ms int32) core.Duration {
	return core.Duration(ms) * core.Millisecond
}
