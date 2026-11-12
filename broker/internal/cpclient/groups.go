package cpclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds/broker/api/controlplane/v1"
	"github.com/manifesterio/mds/broker/internal/core"
)

// Group calls (slice 4b). A JoinGroup or SyncGroup can block for a whole
// rebalance, so these calls use only the caller's context, with no fixed
// timeout. A business error comes back as *Error, as for every call here.

// JoinGroup joins a consumer group and waits for the join round to end.
func (c *Client) JoinGroup(ctx context.Context, request core.GroupJoin) (core.GroupJoinResp, error) {
	return callRPC(ctx, c, "JoinGroup", cpv1.ControlPlaneClient.JoinGroup,
		joinGroupFromCore(request), joinGroupRespToCore)
}

// SyncGroup sends the leader's assignment, or waits for it as a follower.
func (c *Client) SyncGroup(ctx context.Context, request core.GroupSync) (core.GroupSyncResp, error) {
	return callRPC(ctx, c, "SyncGroup", cpv1.ControlPlaneClient.SyncGroup,
		syncGroupFromCore(request), syncGroupRespToCore)
}

// Heartbeat renews a member's session.
func (c *Client) Heartbeat(ctx context.Context, request core.GroupHeartbeat) error {
	return c.do(ctx, "Heartbeat", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.Heartbeat(ctx, heartbeatFromCore(request))
		return err
	})
}

// LeaveGroup removes members. It returns one code per member, in request order.
func (c *Client) LeaveGroup(ctx context.Context, request core.GroupLeave) ([]core.ErrCode, error) {
	return callRPC(ctx, c, "LeaveGroup", cpv1.ControlPlaneClient.LeaveGroup,
		leaveGroupFromCore(request), leaveGroupRespToCore)
}

// OffsetCommit stores committed offsets. It returns one code per partition.
func (c *Client) OffsetCommit(ctx context.Context, operation core.OffsetCommit) (core.OffsetCommitResp, error) {
	return callRPC(ctx, c, "OffsetCommit", cpv1.ControlPlaneClient.OffsetCommit,
		offsetCommitFromCore(operation), offsetCommitRespToCore)
}

// OffsetFetch reads committed offsets.
func (c *Client) OffsetFetch(ctx context.Context, operation core.OffsetFetch) (core.OffsetFetchResp, error) {
	return callRPC(ctx, c, "OffsetFetch", cpv1.ControlPlaneClient.OffsetFetch,
		offsetFetchFromCore(operation), offsetFetchRespToCore)
}

// DescribeGroups describes the named groups, in request order.
func (c *Client) DescribeGroups(ctx context.Context, names []string) ([]core.GroupDescription, error) {
	return callRPC(ctx, c, "DescribeGroups", cpv1.ControlPlaneClient.DescribeGroups,
		describeGroupsFromCore(names), describeGroupsRespToCore)
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
	return listGroupsRespToCore(out), nil
}

// callGroup performs one group operation of the broker (Call). A business
// error comes back as *Error, as for every other call.
func (c *Client) callGroup(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	switch operation := operation.(type) {
	case core.GroupJoin:
		r, err := c.JoinGroup(ctx, operation)
		if err != nil {
			return nil, err
		}
		return r, nil
	case core.GroupSync:
		r, err := c.SyncGroup(ctx, operation)
		if err != nil {
			return nil, err
		}
		return r, nil
	case core.GroupHeartbeat:
		return nil, c.Heartbeat(ctx, operation)
	case core.GroupLeave:
		errs, err := c.LeaveGroup(ctx, operation)
		if err != nil {
			return nil, err
		}
		return core.GroupLeaveResp{Errs: errs}, nil
	case core.OffsetCommit:
		return callOp(ctx, c, "OffsetCommit", cpv1.ControlPlaneClient.OffsetCommit,
			offsetCommitFromCore(operation), offsetCommitRespToCore)
	case core.OffsetFetch:
		return callOp(ctx, c, "OffsetFetch", cpv1.ControlPlaneClient.OffsetFetch,
			offsetFetchFromCore(operation), offsetFetchRespToCore)
	case core.ListGroups:
		gs, err := c.ListGroups(ctx)
		if err != nil {
			return nil, err
		}
		return core.ListGroupsResp{Groups: gs}, nil
	case core.DescribeGroups:
		ds, err := c.DescribeGroups(ctx, operation.Groups)
		if err != nil {
			return nil, err
		}
		return core.DescribeGroupsResp{Groups: ds}, nil
	}
	return nil, &Error{Code: core.ErrInvalid}
}
