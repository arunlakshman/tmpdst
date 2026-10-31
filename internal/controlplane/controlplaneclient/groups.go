package controlplaneclient

import (
	"context"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/apiconvert"
	"manifester/internal/controlplane/groupcoordinator"
	"manifester/internal/core"
)

// Group calls (slice 4b). A JoinGroup or SyncGroup can block for a whole
// rebalance, so these calls use only the caller's context, with no fixed
// timeout. A business error comes back as *Error, as for every call here.

// JoinGroup joins a consumer group and waits for the join round to end.
//
//nolint:dupl // each group call keeps its own conversion
func (c *Client) JoinGroup(ctx context.Context, req groupcoordinator.JoinReq) (groupcoordinator.JoinResp, error) {
	var out *cpv1.JoinGroupResponse
	err := c.do(ctx, "JoinGroup", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.JoinGroup(ctx, apiconvert.JoinGroupFromCore(req))
		return e
	})
	if err != nil {
		return groupcoordinator.JoinResp{}, err
	}
	return apiconvert.JoinGroupRespToCore(out), nil
}

// SyncGroup sends the leader's assignment, or waits for it as a follower.
//
//nolint:dupl // each group call keeps its own conversion
func (c *Client) SyncGroup(ctx context.Context, req groupcoordinator.SyncReq) (groupcoordinator.SyncResp, error) {
	var out *cpv1.SyncGroupResponse
	err := c.do(ctx, "SyncGroup", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.SyncGroup(ctx, apiconvert.SyncGroupFromCore(req))
		return e
	})
	if err != nil {
		return groupcoordinator.SyncResp{}, err
	}
	return apiconvert.SyncGroupRespToCore(out), nil
}

// GroupHeartbeat renews a member's session.
func (c *Client) GroupHeartbeat(ctx context.Context, req groupcoordinator.HeartbeatReq) error {
	return c.do(ctx, "GroupHeartbeat", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.GroupHeartbeat(ctx, apiconvert.HeartbeatFromCore(req))
		return err
	})
}

// LeaveGroup removes members. It returns one code per member, in request order.
//
//nolint:dupl // each group call keeps its own conversion
func (c *Client) LeaveGroup(ctx context.Context, req groupcoordinator.LeaveReq) ([]core.ErrCode, error) {
	var out *cpv1.LeaveGroupResponse
	err := c.do(ctx, "LeaveGroup", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.LeaveGroup(ctx, apiconvert.LeaveGroupFromCore(req))
		return e
	})
	if err != nil {
		return nil, err
	}
	return apiconvert.LeaveGroupRespToCore(out), nil
}

// GroupOffsetCommit stores committed offsets. It returns one code per partition.
//
//nolint:dupl // each group call keeps its own conversion
func (c *Client) GroupOffsetCommit(ctx context.Context, op core.OffsetCommit) (core.OffsetCommitResp, error) {
	var out *cpv1.GroupOffsetCommitResponse
	err := c.do(ctx, "GroupOffsetCommit", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.GroupOffsetCommit(ctx, apiconvert.OffsetCommitFromCore(op))
		return e
	})
	if err != nil {
		return core.OffsetCommitResp{}, err
	}
	return apiconvert.OffsetCommitRespToCore(out), nil
}

// GroupOffsetFetch reads committed offsets.
//
//nolint:dupl // each group call keeps its own conversion
func (c *Client) GroupOffsetFetch(ctx context.Context, op core.OffsetFetch) (core.OffsetFetchResp, error) {
	var out *cpv1.GroupOffsetFetchResponse
	err := c.do(ctx, "GroupOffsetFetch", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.GroupOffsetFetch(ctx, apiconvert.OffsetFetchFromCore(op))
		return e
	})
	if err != nil {
		return core.OffsetFetchResp{}, err
	}
	return apiconvert.OffsetFetchRespToCore(out), nil
}

// DescribeGroups describes the named groups, in request order.
//
//nolint:dupl // each group call keeps its own conversion
func (c *Client) DescribeGroups(ctx context.Context, names []string) ([]groupcoordinator.GroupDescription, error) {
	var out *cpv1.DescribeGroupsResponse
	err := c.do(ctx, "DescribeGroups", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.DescribeGroups(ctx, apiconvert.DescribeGroupsFromCore(names))
		return e
	})
	if err != nil {
		return nil, err
	}
	return apiconvert.DescribeGroupsRespToCore(out), nil
}

// ListGroups lists the groups in the log, sorted by name.
func (c *Client) ListGroups(ctx context.Context) ([]core.GroupSummary, error) {
	var out *cpv1.ListGroupsResponse
	err := c.do(ctx, "ListGroups", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.ListGroups(ctx, &cpv1.ListGroupsRequest{})
		return e
	})
	if err != nil {
		return nil, err
	}
	return apiconvert.ListGroupsRespToCore(out), nil
}

// callGroup performs one group operation of the broker (Call). A business
// error comes back as *Error, as for every other call.
func (c *Client) callGroup(ctx context.Context, op core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	switch op := op.(type) {
	case core.GroupJoin:
		r, err := c.JoinGroup(ctx, joinReq(op))
		if err != nil {
			return nil, err
		}
		return joinResp(r), nil
	case core.GroupSync:
		r, err := c.SyncGroup(ctx, syncReq(op))
		if err != nil {
			return nil, err
		}
		return core.GroupSyncResp{Assignment: r.Assignment}, nil
	case core.GroupHeartbeat:
		return nil, c.GroupHeartbeat(ctx, groupcoordinator.HeartbeatReq{Group: op.Group, MemberID: op.MemberID, Generation: op.Generation})
	case core.GroupLeave:
		errs, err := c.LeaveGroup(ctx, groupcoordinator.LeaveReq{Group: op.Group, Members: op.Members})
		if err != nil {
			return nil, err
		}
		return core.GroupLeaveResp{Errs: errs}, nil
	case core.OffsetCommit:
		return c.GroupOffsetCommit(ctx, op)
	case core.OffsetFetch:
		return c.GroupOffsetFetch(ctx, op)
	case core.ListGroups:
		gs, err := c.ListGroups(ctx)
		if err != nil {
			return nil, err
		}
		return core.ListGroupsResp{Groups: gs}, nil
	case core.DescribeGroups:
		ds, err := c.DescribeGroups(ctx, op.Groups)
		if err != nil {
			return nil, err
		}
		return describeGroupsResp(ds), nil
	}
	return nil, &Error{Code: core.ErrInvalid}
}

func joinReq(op core.GroupJoin) groupcoordinator.JoinReq {
	r := groupcoordinator.JoinReq{
		Group: op.Group, MemberID: op.MemberID, GroupInstanceID: op.GroupInstanceID,
		ClientID: op.ClientID, ClientHost: op.ClientHost,
		SessionTimeoutMs: op.SessionTimeoutMs, RebalanceTimeoutMs: op.RebalanceTimeoutMs,
		ProtocolType: op.ProtocolType,
	}
	for _, p := range op.Protocols {
		r.Protocols = append(r.Protocols, groupcoordinator.Protocol{Name: p.Name, Metadata: p.Metadata})
	}
	return r
}

func joinResp(r groupcoordinator.JoinResp) core.GroupJoinResp {
	out := core.GroupJoinResp{Generation: r.Generation, Protocol: r.Protocol, Leader: r.Leader, MemberID: r.MemberID}
	for _, m := range r.Members {
		out.Members = append(out.Members, core.GroupJoinMember{MemberID: m.ID, GroupInstanceID: m.InstanceID, Metadata: m.Metadata})
	}
	return out
}

func syncReq(op core.GroupSync) groupcoordinator.SyncReq {
	r := groupcoordinator.SyncReq{Group: op.Group, MemberID: op.MemberID, Generation: op.Generation}
	for _, a := range op.Assignments {
		r.Assignments = append(r.Assignments, groupcoordinator.Assignment{Member: a.Member, Bytes: a.Bytes})
	}
	return r
}

func describeGroupsResp(ds []groupcoordinator.GroupDescription) core.DescribeGroupsResp {
	var out core.DescribeGroupsResp
	for _, d := range ds {
		out.Groups = append(out.Groups, core.GroupDescription{Name: d.Name, DescribeGroupResp: d.DescribeGroupResp})
	}
	return out
}
