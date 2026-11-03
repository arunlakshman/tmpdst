package server

import (
	"context"
	"time"

	"github.com/manifesterio/mds/internal/controlplane"
	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/groupcoordinator"
	"github.com/manifesterio/mds/internal/controlplane/statemachine"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// Where the pieces of the consumer group feature live:
//
//   - groupcoordinator/coordinator.go, group.go, clock.go: the coordinator
//     logic. It keeps the phase of each group, the parked calls and the timers.
//   - server/groups.go: this file. It holds the gRPC group handlers and the
//     log and read side of the coordinator (nodeGroups).
//   - controlplaneclient/groups.go: the broker-side group calls.
//   - wireboundary/convert.go: maps group requests and responses to and from the
//     wire types.
//   - stateupdater/groups.go: the fold of GroupInstall, GroupAssign,
//     GroupRemove and OffsetCommit.
//   - stateupdater/groupquery.go: the group reads (OffsetFetch, DescribeGroup
//     and ListGroups).
//   - statemachine/client.go: passes the group calls to the state machine.
//   - statemachine/commit.go: encodes and decodes the group entries in the log.
//   - statemachine/replay.go: decodes group entries when it replays the log.
//   - core/controlplane.go: the group types (GroupInstall, GroupAssign,
//     GroupRemove, OffsetCommit and the others).

// groupWatchEvery is how often the node checks whether it is the active
// appender, and so whether the group coordinator should serve.
const groupWatchEvery = 100 * time.Millisecond

// nodeGroups is the log and read side of the group coordinator. A write is a
// log entry offered to the run loop. A read is answered by the fold.
type nodeGroups struct{ n *Node }

var _ controlplane.GroupStore = nodeGroups{}

// write offers one group decision to the log. A business answer comes back in
// the Done; err is only for the run loop or the context.
func (s nodeGroups) write(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	entry, _ := stateupdater.GroupEntry(operation)
	return s.n.offer(ctx, entry)
}

// GroupInstall implements controlplane.GroupStore.
func (s nodeGroups) GroupInstall(ctx context.Context, operation core.GroupInstall) (core.ErrCode, error) {
	d, err := s.write(ctx, operation)
	return d.Err, err
}

// GroupAssign implements controlplane.GroupStore.
func (s nodeGroups) GroupAssign(ctx context.Context, operation core.GroupAssign) (core.ErrCode, error) {
	d, err := s.write(ctx, operation)
	return d.Err, err
}

// GroupRemove implements controlplane.GroupStore.
func (s nodeGroups) GroupRemove(ctx context.Context, operation core.GroupRemove) (core.ErrCode, error) {
	d, err := s.write(ctx, operation)
	return d.Err, err
}

// OffsetCommit implements controlplane.GroupStore.
func (s nodeGroups) OffsetCommit(ctx context.Context, operation core.OffsetCommit) (core.OffsetCommitResp, core.ErrCode, error) {
	d, err := s.write(ctx, operation)
	return core.ResponseAs[core.OffsetCommitResp](d), d.Err, err
}

// readFold runs one group read on the loop. A node that is not ready answers
// with an error, so the coordinator refuses the call.
func readFold(ctx context.Context, n *Node, fn func(*statemachine.Machine) (core.ControlPlaneDone, error)) (core.ControlPlaneDone, error) {
	d, err := n.read(ctx, fn)
	if err != nil {
		return core.ControlPlaneDone{}, err
	}
	if d.Err != core.OK {
		return core.ControlPlaneDone{}, controlplane.ErrUnavailable
	}
	return d, nil
}

// readGroup runs one group read on the fold and turns the answer into R. The
// read method is passed in. On an error the result is the zero R.
func readGroup[R, Op any](ctx context.Context, n *Node, op Op, read func(*stateupdater.StateUpdater, Op) (core.ControlPlaneDone, error)) (R, error) {
	d, err := readFold(ctx, n, func(m *statemachine.Machine) (core.ControlPlaneDone, error) {
		return read(m.Fold(), op)
	})
	return core.ResponseAs[R](d), err
}

// OffsetFetch implements controlplane.GroupStore.
func (s nodeGroups) OffsetFetch(ctx context.Context, operation core.OffsetFetch) (core.OffsetFetchResp, error) {
	return readGroup[core.OffsetFetchResp](ctx, s.n, operation, (*stateupdater.StateUpdater).OffsetFetch)
}

// DescribeGroup implements controlplane.GroupStore.
func (s nodeGroups) DescribeGroup(ctx context.Context, operation core.DescribeGroup) (core.DescribeGroupResp, error) {
	return readGroup[core.DescribeGroupResp](ctx, s.n, operation, (*stateupdater.StateUpdater).DescribeGroup)
}

// ListGroups implements controlplane.GroupStore.
func (s nodeGroups) ListGroups(ctx context.Context, operation core.ListGroups) (core.ListGroupsResp, error) {
	return readGroup[core.ListGroupsResp](ctx, s.n, operation, (*stateupdater.StateUpdater).ListGroups)
}

// groupRole reports whether this node is the active appender and ready.
func (n *Node) groupRole(ctx context.Context) (bool, error) {
	active := false
	err := n.onLoop(ctx, func(m *statemachine.Machine) []core.Action {
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
	t := time.NewTicker(groupWatchEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			n.groups.Stop()
			return
		case <-t.C:
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
func (n *Node) groupCoordinator(ctx context.Context) (*groupcoordinator.Coordinator, error) {
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

// groupRPC runs one coordinator call for an RPC. It checks that this node
// serves groups, runs call, maps a failed answer to an RPC error and converts
// the answer to the wire type Out.
func groupRPC[Op, R, Out any](ctx context.Context, n *Node, op Op,
	call func(*groupcoordinator.Coordinator, context.Context, Op) (R, error),
	codeOf func(R) core.ErrCode, convert func(R) Out,
) (Out, error) {
	var zero Out
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return zero, err
	}
	r, err := call(c, ctx, op)
	if err := groupReplyErr(codeOf(r), err); err != nil {
		return zero, err
	}
	return convert(r), nil
}

// joinCode returns the answer code of a join.
func joinCode(r groupcoordinator.JoinResp) core.ErrCode { return r.Err }

// syncCode returns the answer code of a sync.
func syncCode(r groupcoordinator.SyncResp) core.ErrCode { return r.Err }

// JoinGroup implements ControlPlane.
func (n *Node) JoinGroup(ctx context.Context, request *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	return groupRPC(ctx, n, wireboundary.JoinGroupToCore(request),
		(*groupcoordinator.Coordinator).JoinGroup, joinCode, wireboundary.JoinGroupRespFromCore)
}

// SyncGroup implements ControlPlane.
func (n *Node) SyncGroup(ctx context.Context, request *cpv1.SyncGroupRequest) (*cpv1.SyncGroupResponse, error) {
	operation := wireboundary.SyncGroupToCore(request)
	return groupRPC(ctx, n, operation,
		(*groupcoordinator.Coordinator).SyncGroup, syncCode, wireboundary.SyncGroupRespFromCore)
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
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	r, code := c.OffsetCommit(ctx, wireboundary.OffsetCommitToCore(request))
	if code != core.OK {
		return nil, toRPCError(code, groupOutcome(code))
	}
	return wireboundary.OffsetCommitRespFromCore(r), nil
}

// GroupOffsetFetch implements ControlPlane.
func (n *Node) GroupOffsetFetch(ctx context.Context, request *cpv1.GroupOffsetFetchRequest) (*cpv1.GroupOffsetFetchResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	r, code := c.OffsetFetch(ctx, wireboundary.OffsetFetchToCore(request))
	if code != core.OK {
		return nil, toRPCError(code, core.NotApplied)
	}
	return wireboundary.OffsetFetchRespFromCore(r), nil
}

// DescribeGroups implements ControlPlane. Any group that fails fails the call.
func (n *Node) DescribeGroups(ctx context.Context, request *cpv1.DescribeGroupsRequest) (*cpv1.DescribeGroupsResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	var out []groupcoordinator.GroupDescription
	for _, name := range wireboundary.DescribeGroupsToCore(request) {
		d, code := c.DescribeGroup(ctx, name)
		if code != core.OK {
			return nil, toRPCError(code, core.NotApplied)
		}
		out = append(out, groupcoordinator.GroupDescription{Name: name, DescribeGroupResp: d})
	}
	return wireboundary.DescribeGroupsRespFromCore(out), nil
}

// ListGroups implements ControlPlane.
func (n *Node) ListGroups(ctx context.Context, _ *cpv1.ListGroupsRequest) (*cpv1.ListGroupsResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	gs, code := c.ListGroups(ctx)
	if code != core.OK {
		return nil, toRPCError(code, core.NotApplied)
	}
	return wireboundary.ListGroupsRespFromCore(gs), nil
}
