package server

import (
	"context"
	"time"

	"mds/internal/controlplane"
	cpv1 "mds/internal/controlplane/api/v1"
	"mds/internal/controlplane/apiconvert"
	"mds/internal/controlplane/groupcoordinator"
	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

// groupWatchEvery is how often the node checks whether it is the active
// appender, and so whether the group coordinator should serve.
const groupWatchEvery = 100 * time.Millisecond

// nodeGroups is the log and read side of the group coordinator. A write is a
// log entry offered to the run loop. A read is answered by the fold.
type nodeGroups struct{ n *Node }

var _ controlplane.GroupStore = nodeGroups{}

// write offers one group decision to the log. A business answer comes back in
// the Done; err is only for the run loop or the context.
func (s nodeGroups) write(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	entry, _ := stateupdater.GroupEntry(op)
	return s.n.offer(ctx, entry)
}

// GroupInstall implements controlplane.GroupStore.
func (s nodeGroups) GroupInstall(ctx context.Context, op core.GroupInstall) (core.ErrCode, error) {
	d, err := s.write(ctx, op)
	return d.Err, err
}

// GroupAssign implements controlplane.GroupStore.
func (s nodeGroups) GroupAssign(ctx context.Context, op core.GroupAssign) (core.ErrCode, error) {
	d, err := s.write(ctx, op)
	return d.Err, err
}

// GroupRemove implements controlplane.GroupStore.
func (s nodeGroups) GroupRemove(ctx context.Context, op core.GroupRemove) (core.ErrCode, error) {
	d, err := s.write(ctx, op)
	return d.Err, err
}

// OffsetCommit implements controlplane.GroupStore.
func (s nodeGroups) OffsetCommit(ctx context.Context, op core.OffsetCommit) (core.OffsetCommitResp, core.ErrCode, error) {
	d, err := s.write(ctx, op)
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

// OffsetFetch implements controlplane.GroupStore.
//
//nolint:dupl // each call keeps its own conversion
func (s nodeGroups) OffsetFetch(ctx context.Context, op core.OffsetFetch) (core.OffsetFetchResp, error) {
	d, err := readFold(ctx, s.n, func(m *statemachine.Machine) (core.ControlPlaneDone, error) {
		return m.Fold().OffsetFetch(op)
	})
	return core.ResponseAs[core.OffsetFetchResp](d), err
}

// DescribeGroup implements controlplane.GroupStore.
//
//nolint:dupl // each call keeps its own conversion
func (s nodeGroups) DescribeGroup(ctx context.Context, op core.DescribeGroup) (core.DescribeGroupResp, error) {
	d, err := readFold(ctx, s.n, func(m *statemachine.Machine) (core.ControlPlaneDone, error) {
		return m.Fold().DescribeGroup(op)
	})
	return core.ResponseAs[core.DescribeGroupResp](d), err
}

// ListGroups implements controlplane.GroupStore.
//
//nolint:dupl // each call keeps its own conversion
func (s nodeGroups) ListGroups(ctx context.Context, op core.ListGroups) (core.ListGroupsResp, error) {
	d, err := readFold(ctx, s.n, func(m *statemachine.Machine) (core.ControlPlaneDone, error) {
		return m.Fold().ListGroups(op)
	})
	return core.ResponseAs[core.ListGroupsResp](d), err
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

// JoinGroup implements ControlPlane.
//
//nolint:dupl // each call keeps its own conversion
func (n *Node) JoinGroup(ctx context.Context, request *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	r, err := c.JoinGroup(ctx, apiconvert.JoinGroupToCore(request))
	if err != nil {
		return nil, groupCtxErr()
	}
	if r.Err != core.OK {
		return nil, toRPCError(r.Err, groupOutcome(r.Err))
	}
	return apiconvert.JoinGroupRespFromCore(r), nil
}

// SyncGroup implements ControlPlane.
//
//nolint:dupl // each call keeps its own conversion
func (n *Node) SyncGroup(ctx context.Context, request *cpv1.SyncGroupRequest) (*cpv1.SyncGroupResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	r, err := c.SyncGroup(ctx, apiconvert.SyncGroupToCore(request))
	if err != nil {
		return nil, groupCtxErr()
	}
	if r.Err != core.OK {
		return nil, toRPCError(r.Err, groupOutcome(r.Err))
	}
	return apiconvert.SyncGroupRespFromCore(r), nil
}

// GroupHeartbeat implements ControlPlane.
func (n *Node) GroupHeartbeat(ctx context.Context, request *cpv1.GroupHeartbeatRequest) (*cpv1.GroupHeartbeatResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	if code := c.Heartbeat(ctx, apiconvert.HeartbeatToCore(request)); code != core.OK {
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
	return apiconvert.LeaveGroupRespFromCore(c.LeaveGroup(ctx, apiconvert.LeaveGroupToCore(request))), nil
}

// GroupOffsetCommit implements ControlPlane. The answer has one code per partition.
func (n *Node) GroupOffsetCommit(ctx context.Context, request *cpv1.GroupOffsetCommitRequest) (*cpv1.GroupOffsetCommitResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	r, code := c.OffsetCommit(ctx, apiconvert.OffsetCommitToCore(request))
	if code != core.OK {
		return nil, toRPCError(code, groupOutcome(code))
	}
	return apiconvert.OffsetCommitRespFromCore(r), nil
}

// GroupOffsetFetch implements ControlPlane.
func (n *Node) GroupOffsetFetch(ctx context.Context, request *cpv1.GroupOffsetFetchRequest) (*cpv1.GroupOffsetFetchResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	r, code := c.OffsetFetch(ctx, apiconvert.OffsetFetchToCore(request))
	if code != core.OK {
		return nil, toRPCError(code, core.NotApplied)
	}
	return apiconvert.OffsetFetchRespFromCore(r), nil
}

// DescribeGroups implements ControlPlane. Any group that fails fails the call.
func (n *Node) DescribeGroups(ctx context.Context, request *cpv1.DescribeGroupsRequest) (*cpv1.DescribeGroupsResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	var out []groupcoordinator.GroupDescription
	for _, name := range apiconvert.DescribeGroupsToCore(request) {
		d, code := c.DescribeGroup(ctx, name)
		if code != core.OK {
			return nil, toRPCError(code, core.NotApplied)
		}
		out = append(out, groupcoordinator.GroupDescription{Name: name, DescribeGroupResp: d})
	}
	return apiconvert.DescribeGroupsRespFromCore(out), nil
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
	return apiconvert.ListGroupsRespFromCore(gs), nil
}
