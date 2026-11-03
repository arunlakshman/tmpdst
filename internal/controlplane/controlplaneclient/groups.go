package controlplaneclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/groups"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// Group calls (slice 4b). A JoinGroup or SyncGroup can block for a whole
// rebalance, so these calls use only the caller's context, with no fixed
// timeout. A business error comes back as *Error, as for every call here.

// JoinGroup joins a consumer group and waits for the join round to end.
func (c *Client) JoinGroup(ctx context.Context, request groups.JoinReq) (groups.JoinResp, error) {
	return callRPC(ctx, c, "JoinGroup", cpv1.ControlPlaneClient.JoinGroup,
		wireboundary.JoinGroupFromCore(request), wireboundary.JoinGroupRespToCore)
}

// SyncGroup sends the leader's assignment, or waits for it as a follower.
func (c *Client) SyncGroup(ctx context.Context, request groups.SyncReq) (groups.SyncResp, error) {
	return callRPC(ctx, c, "SyncGroup", cpv1.ControlPlaneClient.SyncGroup,
		wireboundary.SyncGroupFromCore(request), wireboundary.SyncGroupRespToCore)
}

// GroupHeartbeat renews a member's session.
func (c *Client) GroupHeartbeat(ctx context.Context, request groups.HeartbeatReq) error {
	return c.do(ctx, "GroupHeartbeat", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.GroupHeartbeat(ctx, wireboundary.HeartbeatFromCore(request))
		return err
	})
}

// LeaveGroup removes members. It returns one code per member, in request order.
func (c *Client) LeaveGroup(ctx context.Context, request groups.LeaveReq) ([]core.ErrCode, error) {
	return callRPC(ctx, c, "LeaveGroup", cpv1.ControlPlaneClient.LeaveGroup,
		wireboundary.LeaveGroupFromCore(request), wireboundary.LeaveGroupRespToCore)
}

// GroupOffsetCommit stores committed offsets. It returns one code per partition.
func (c *Client) GroupOffsetCommit(ctx context.Context, operation core.OffsetCommit) (core.OffsetCommitResp, error) {
	return callRPC(ctx, c, "GroupOffsetCommit", cpv1.ControlPlaneClient.GroupOffsetCommit,
		wireboundary.OffsetCommitFromCore(operation), wireboundary.OffsetCommitRespToCore)
}

// GroupOffsetFetch reads committed offsets.
func (c *Client) GroupOffsetFetch(ctx context.Context, operation core.OffsetFetch) (core.OffsetFetchResp, error) {
	return callRPC(ctx, c, "GroupOffsetFetch", cpv1.ControlPlaneClient.GroupOffsetFetch,
		wireboundary.OffsetFetchFromCore(operation), wireboundary.OffsetFetchRespToCore)
}

// DescribeGroups describes the named groups, in request order.
func (c *Client) DescribeGroups(ctx context.Context, names []string) ([]groups.GroupDescription, error) {
	return callRPC(ctx, c, "DescribeGroups", cpv1.ControlPlaneClient.DescribeGroups,
		wireboundary.DescribeGroupsFromCore(names), wireboundary.DescribeGroupsRespToCore)
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
	return wireboundary.ListGroupsRespToCore(out), nil
}

// callGroup performs one group operation of the broker (Call). A business
// error comes back as *Error, as for every other call.
func (c *Client) callGroup(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	switch operation := operation.(type) {
	case core.GroupJoin:
		r, err := c.JoinGroup(ctx, joinReq(operation))
		if err != nil {
			return nil, err
		}
		return joinResp(r), nil
	case core.GroupSync:
		r, err := c.SyncGroup(ctx, syncReq(operation))
		if err != nil {
			return nil, err
		}
		return core.GroupSyncResp{Assignment: r.Assignment}, nil
	case core.GroupHeartbeat:
		return nil, c.GroupHeartbeat(ctx, groups.HeartbeatReq{Group: operation.Group, MemberID: operation.MemberID, Generation: operation.Generation})
	case core.GroupLeave:
		errs, err := c.LeaveGroup(ctx, groups.LeaveReq{Group: operation.Group, Members: operation.Members})
		if err != nil {
			return nil, err
		}
		return core.GroupLeaveResp{Errs: errs}, nil
	case core.OffsetCommit:
		return c.GroupOffsetCommit(ctx, operation)
	case core.OffsetFetch:
		return c.GroupOffsetFetch(ctx, operation)
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
		return describeGroupsResp(ds), nil
	}
	return nil, &Error{Code: core.ErrInvalid}
}

func joinReq(operation core.GroupJoin) groups.JoinReq {
	r := groups.JoinReq{
		Group: operation.Group, MemberID: operation.MemberID, GroupInstanceID: operation.GroupInstanceID,
		ClientID: operation.ClientID, ClientHost: operation.ClientHost,
		SessionTimeoutMs: operation.SessionTimeoutMs, RebalanceTimeoutMs: operation.RebalanceTimeoutMs,
		ProtocolType: operation.ProtocolType,
	}
	for _, p := range operation.Protocols {
		r.Protocols = append(r.Protocols, groups.Protocol{Name: p.Name, Metadata: p.Metadata})
	}
	return r
}

func joinResp(r groups.JoinResp) core.GroupJoinResp {
	out := core.GroupJoinResp{Generation: r.Generation, Protocol: r.Protocol, Leader: r.Leader, MemberID: r.MemberID}
	for _, m := range r.Members {
		out.Members = append(out.Members, core.GroupJoinMember{MemberID: m.ID, GroupInstanceID: m.InstanceID, Metadata: m.Metadata})
	}
	return out
}

func syncReq(operation core.GroupSync) groups.SyncReq {
	r := groups.SyncReq{Group: operation.Group, MemberID: operation.MemberID, Generation: operation.Generation}
	for _, a := range operation.Assignments {
		r.Assignments = append(r.Assignments, groups.Assignment{Member: a.Member, Bytes: a.Bytes})
	}
	return r
}

func describeGroupsResp(ds []groups.GroupDescription) core.DescribeGroupsResp {
	var out core.DescribeGroupsResp
	for _, d := range ds {
		out.Groups = append(out.Groups, core.GroupDescription{Name: d.Name, DescribeGroupResp: d.DescribeGroupResp})
	}
	return out
}
