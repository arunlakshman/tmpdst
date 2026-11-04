package server

import (
	"context"
	"time"

	"github.com/manifesterio/mds/internal/controlplane"
	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/groups"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// Where the pieces of the consumer group feature live:
//
//   - groups/coordinator.go, group.go, clock.go: the coordinator
//     logic. It keeps the phase of each group, the parked calls and the timers.
//   - server/groups.go: this file. It holds the gRPC group handlers and the
//     log and read side of the coordinator (nodeGroups, over Node.Exec in
//     server/topics.go).
//   - controlplaneclient/groups.go: the broker-side group calls.
//   - wireboundary/groups.go: maps group requests and responses to and from the
//     wire types.
//   - groups/storage/module.go, write.go: the group module: the fold of
//     GroupInstall, GroupAssign, GroupRemove and OffsetCommit, on the key-value store.
//   - groups/storage/read.go: the group reads (OffsetFetch, DescribeGroup
//     and ListGroups).
//   - metadata/metadata.go: the kernel routes each group operation to the module.
//   - metadata/entry.go: the log format of the group entries.
//   - replica/client.go: passes the group calls to the replica.
//   - replica/commit.go: encodes and decodes the group entries in the log.
//   - replica/replay.go: decodes group entries when it replays the log.
//   - core/controlplane.go: the group types (GroupInstall, GroupAssign,
//     GroupRemove, OffsetCommit and the others).

// groupWatchEvery is how often the node checks whether it is the active
// appender, and so whether the group coordinator should serve.
const groupWatchEvery = 100 * time.Millisecond

// nodeGroups is the log and read side of the group coordinator. It is Node.Exec
// plus one rule: a node that is not ready answers a read with an error, so the
// coordinator refuses the call.
type nodeGroups struct{ n *Node }

var _ groups.Store = nodeGroups{}

// Exec runs one group operation through Node.Exec.
func (s nodeGroups) Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	d, err := s.n.Exec(ctx, operation)
	if isCommand(operation) {
		return d, err
	}
	if err != nil {
		return core.ControlPlaneDone{}, err
	}
	if d.Err != core.OK {
		return core.ControlPlaneDone{}, controlplane.ErrUnavailable
	}
	return d, nil
}

// groupRole reports whether this node is the active appender and ready.
func (n *Node) groupRole(ctx context.Context) (bool, error) {
	active := false
	err := n.onLoop(ctx, func(m *replica.Replica) []core.Action {
		active = m.IsActive() && m.Ready()
		return nil
	})
	return active, err
}

// watchGroups keeps the group coordinator in step with the active role. While
// this node is active and the coordinator is not serving (at start, after a
// takeover, or after a broken write), it rebuilds from the log. A failed
// rebuild is tried again on the next tick. A standby drops its memory.
func (n *Node) watchGroups(ctx context.Context) {
	for {
		tick := make(chan struct{})
		timer := n.config.Clock.AfterFunc(core.Duration(groupWatchEvery), func() { close(tick) })
		select {
		case <-ctx.Done():
			timer.Stop()
			n.groups.Stop()
			return
		case <-tick:
		}
		active, err := n.groupRole(ctx)
		if err != nil {
			continue // the loop is stopping; ctx ends the watch
		}
		if !active {
			n.groups.Stop()
			continue
		}
		if !n.groups.Serving() {
			n.groups.Stop()
			_ = n.groups.Rebuild(ctx) // a failure is tried again on the next tick
		}
	}
}

// groupCoordinator returns the coordinator when this node serves groups now.
// Otherwise it answers Unavailable, so the client tries the next address.
func (n *Node) groupCoordinator(ctx context.Context) (*groups.Coordinator, error) {
	active, err := n.groupRole(ctx)
	if err != nil {
		return nil, toRPCError(core.ErrIO, core.NotApplied)
	}
	if !active || !n.groups.Serving() {
		return nil, toRPCError(core.ErrUnavailable, core.NotApplied)
	}
	return n.groups, nil
}

// groupOutcome is the outcome of a group answer that failed. An unavailable
// answer may come after a log write, so it is MaybeApplied.
func groupOutcome(code core.ErrCode) core.Outcome {
	if code == core.ErrUnavailable {
		return core.MaybeApplied
	}
	return core.NotApplied
}

// groupCtxErr maps a call that ended with its context.
func groupCtxErr() error {
	return toRPCError(core.ErrTimeout, core.MaybeApplied)
}

// groupReplyErr maps a coordinator answer to an RPC error. A call that
// ended with its context gives a timeout. A failed answer gives its code.
func groupReplyErr(code core.ErrCode, err error) error {
	if err != nil {
		return groupCtxErr()
	}
	if code != core.OK {
		return toRPCError(code, groupOutcome(code))
	}
	return nil
}

// groupReplyCall describes one group RPC whose coordinator call returns the
// answer and an error, with the business code inside the answer. The error means
// that the call ended with its context. codeOf reads the code, and convert makes
// the wire type Out.
type groupReplyCall[Op, R, Out any] struct {
	call    func(*groups.Coordinator, context.Context, Op) (R, error)
	codeOf  func(R) core.ErrCode
	convert func(R) Out
}

// run checks that this node serves groups, runs the call, maps a failed answer
// to an RPC error and converts the answer.
func (g groupReplyCall[Op, R, Out]) run(ctx context.Context, n *Node, op Op) (Out, error) {
	var zero Out
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return zero, err
	}
	r, err := g.call(c, ctx, op)
	if err := groupReplyErr(g.codeOf(r), err); err != nil {
		return zero, err
	}
	return g.convert(r), nil
}

// groupCodeCall describes one group RPC whose coordinator call returns the
// answer and a code: how to call the coordinator, what the outcome of a failed
// code is (it differs between a write and a read), and how to convert the answer
// to the wire type Out.
type groupCodeCall[Op, R, Out any] struct {
	call    func(*groups.Coordinator, context.Context, Op) (R, core.ErrCode)
	outcome func(core.ErrCode) core.Outcome
	convert func(R) Out
}

// run checks that this node serves groups, runs the call, maps a failed code to
// an RPC error and converts the answer.
func (g groupCodeCall[Op, R, Out]) run(ctx context.Context, n *Node, op Op) (Out, error) {
	var zero Out
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return zero, err
	}
	r, code := g.call(c, ctx, op)
	if code != core.OK {
		return zero, toRPCError(code, g.outcome(code))
	}
	return g.convert(r), nil
}

var (
	groupJoin = groupReplyCall[groups.JoinReq, groups.JoinResp, *cpv1.JoinGroupResponse]{
		(*groups.Coordinator).JoinGroup, func(r groups.JoinResp) core.ErrCode { return r.Err }, wireboundary.JoinGroupRespFromCore}
	groupSync = groupReplyCall[groups.SyncReq, groups.SyncResp, *cpv1.SyncGroupResponse]{
		(*groups.Coordinator).SyncGroup, func(r groups.SyncResp) core.ErrCode { return r.Err }, wireboundary.SyncGroupRespFromCore}
	groupOffsetCommit = groupCodeCall[core.OffsetCommit, core.OffsetCommitResp, *cpv1.GroupOffsetCommitResponse]{
		(*groups.Coordinator).OffsetCommit, groupOutcome, wireboundary.OffsetCommitRespFromCore}
	groupOffsetFetch = groupCodeCall[core.OffsetFetch, core.OffsetFetchResp, *cpv1.GroupOffsetFetchResponse]{
		(*groups.Coordinator).OffsetFetch, notApplied, wireboundary.OffsetFetchRespFromCore}
	groupList = groupCodeCall[struct{}, []core.GroupSummary, *cpv1.ListGroupsResponse]{
		func(c *groups.Coordinator, ctx context.Context, _ struct{}) ([]core.GroupSummary, core.ErrCode) {
			return c.ListGroups(ctx)
		}, notApplied, wireboundary.ListGroupsRespFromCore}
)

// notApplied is the outcome of a failed read.
func notApplied(core.ErrCode) core.Outcome { return core.NotApplied }

// JoinGroup implements ControlPlane.
func (n *Node) JoinGroup(ctx context.Context, request *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	return groupJoin.run(ctx, n, wireboundary.JoinGroupToCore(request))
}

// SyncGroup implements ControlPlane.
func (n *Node) SyncGroup(ctx context.Context, request *cpv1.SyncGroupRequest) (*cpv1.SyncGroupResponse, error) {
	return groupSync.run(ctx, n, wireboundary.SyncGroupToCore(request))
}

// GroupHeartbeat implements ControlPlane.
func (n *Node) GroupHeartbeat(ctx context.Context, request *cpv1.GroupHeartbeatRequest) (*cpv1.GroupHeartbeatResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	if code := c.Heartbeat(ctx, wireboundary.HeartbeatToCore(request)); code != core.OK {
		return nil, toRPCError(code, groupOutcome(code))
	}
	return &cpv1.GroupHeartbeatResponse{}, nil
}

// LeaveGroup implements ControlPlane. The answer has one code per member.
func (n *Node) LeaveGroup(ctx context.Context, request *cpv1.LeaveGroupRequest) (*cpv1.LeaveGroupResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	return wireboundary.LeaveGroupRespFromCore(c.LeaveGroup(ctx, wireboundary.LeaveGroupToCore(request))), nil
}

// GroupOffsetCommit implements ControlPlane. The answer has one code per partition.
func (n *Node) GroupOffsetCommit(ctx context.Context, request *cpv1.GroupOffsetCommitRequest) (*cpv1.GroupOffsetCommitResponse, error) {
	return groupOffsetCommit.run(ctx, n, wireboundary.OffsetCommitToCore(request))
}

// GroupOffsetFetch implements ControlPlane.
func (n *Node) GroupOffsetFetch(ctx context.Context, request *cpv1.GroupOffsetFetchRequest) (*cpv1.GroupOffsetFetchResponse, error) {
	return groupOffsetFetch.run(ctx, n, wireboundary.OffsetFetchToCore(request))
}

// DescribeGroups implements ControlPlane. Any group that fails fails the call.
func (n *Node) DescribeGroups(ctx context.Context, request *cpv1.DescribeGroupsRequest) (*cpv1.DescribeGroupsResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	var out []groups.GroupDescription
	for _, name := range wireboundary.DescribeGroupsToCore(request) {
		d, code := c.DescribeGroup(ctx, name)
		if code != core.OK {
			return nil, toRPCError(code, core.NotApplied)
		}
		out = append(out, groups.GroupDescription{Name: name, DescribeGroupResp: d})
	}
	return wireboundary.DescribeGroupsRespFromCore(out), nil
}

// ListGroups implements ControlPlane.
func (n *Node) ListGroups(ctx context.Context, _ *cpv1.ListGroupsRequest) (*cpv1.ListGroupsResponse, error) {
	return groupList.run(ctx, n, struct{}{})
}
