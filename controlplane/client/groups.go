package client

import (
	"context"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

// Group calls (slice 4b). A JoinGroup or SyncGroup can block for a whole
// rebalance, so these calls use only the caller's context, with no fixed
// timeout. A business error comes back as *Error, as for every call here.

// JoinGroup joins a consumer group and waits for the join round to end.
func (c *Client) JoinGroup(ctx context.Context, request contract.GroupJoin) (contract.GroupJoinResp, error) {
	return callRPC(ctx, c, "JoinGroup", cpv1.ControlPlaneClient.JoinGroup,
		joinGroupFromContract(request), joinGroupRespToContract)
}

// SyncGroup sends the leader's assignment, or waits for it as a follower.
func (c *Client) SyncGroup(ctx context.Context, request contract.GroupSync) (contract.GroupSyncResp, error) {
	return callRPC(ctx, c, "SyncGroup", cpv1.ControlPlaneClient.SyncGroup,
		syncGroupFromContract(request), syncGroupRespToContract)
}

// Heartbeat renews a member's session.
func (c *Client) Heartbeat(ctx context.Context, request contract.GroupHeartbeat) error {
	return c.do(ctx, "Heartbeat", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.Heartbeat(ctx, heartbeatFromContract(request))
		return err
	})
}

// LeaveGroup removes members. It returns one code per member, in request order.
func (c *Client) LeaveGroup(ctx context.Context, request contract.GroupLeave) ([]contract.ErrCode, error) {
	return callRPC(ctx, c, "LeaveGroup", cpv1.ControlPlaneClient.LeaveGroup,
		leaveGroupFromContract(request), leaveGroupRespToContract)
}

// OffsetCommit stores committed offsets. It returns one code per partition.
func (c *Client) OffsetCommit(ctx context.Context, operation contract.OffsetCommit) (contract.OffsetCommitResp, error) {
	return callRPC(ctx, c, "OffsetCommit", cpv1.ControlPlaneClient.OffsetCommit,
		offsetCommitFromContract(operation), offsetCommitRespToContract)
}

// OffsetFetch reads committed offsets.
func (c *Client) OffsetFetch(ctx context.Context, operation contract.OffsetFetch) (contract.OffsetFetchResp, error) {
	return callRPC(ctx, c, "OffsetFetch", cpv1.ControlPlaneClient.OffsetFetch,
		offsetFetchFromContract(operation), offsetFetchRespToContract)
}

// DescribeGroups describes the named groups, in request order.
func (c *Client) DescribeGroups(ctx context.Context, names []string) ([]contract.GroupDescription, error) {
	return callRPC(ctx, c, "DescribeGroups", cpv1.ControlPlaneClient.DescribeGroups,
		describeGroupsFromContract(names), describeGroupsRespToContract)
}

// ListGroups lists the groups in the log, sorted by name.
func (c *Client) ListGroups(ctx context.Context) ([]contract.GroupSummary, error) {
	var out *cpv1.ListGroupsResponse
	err := c.do(ctx, "ListGroups", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.ListGroups(ctx, &cpv1.ListGroupsRequest{})
		return e
	})
	if err != nil {
		return nil, err
	}
	return listGroupsRespToContract(out), nil
}

// callGroup performs one group operation of the broker (Call). A business
// error comes back as *Error, as for every other call.
func (c *Client) callGroup(ctx context.Context, operation contract.Operation) (contract.Response, error) {
	switch operation := operation.(type) {
	case contract.GroupJoin:
		r, err := c.JoinGroup(ctx, operation)
		if err != nil {
			return nil, err
		}
		return r, nil
	case contract.GroupSync:
		r, err := c.SyncGroup(ctx, operation)
		if err != nil {
			return nil, err
		}
		return r, nil
	case contract.GroupHeartbeat:
		return nil, c.Heartbeat(ctx, operation)
	case contract.GroupLeave:
		errs, err := c.LeaveGroup(ctx, operation)
		if err != nil {
			return nil, err
		}
		return contract.GroupLeaveResp{Errs: errs}, nil
	case contract.OffsetCommit:
		return callOp(ctx, c, "OffsetCommit", cpv1.ControlPlaneClient.OffsetCommit,
			offsetCommitFromContract(operation), offsetCommitRespToContract)
	case contract.OffsetFetch:
		return callOp(ctx, c, "OffsetFetch", cpv1.ControlPlaneClient.OffsetFetch,
			offsetFetchFromContract(operation), offsetFetchRespToContract)
	case contract.ListGroups:
		gs, err := c.ListGroups(ctx)
		if err != nil {
			return nil, err
		}
		return contract.ListGroupsResp{Groups: gs}, nil
	case contract.DescribeGroups:
		ds, err := c.DescribeGroups(ctx, operation.Groups)
		if err != nil {
			return nil, err
		}
		return contract.DescribeGroupsResp{Groups: ds}, nil
	}
	return nil, &Error{Code: contract.ErrInvalid}
}
