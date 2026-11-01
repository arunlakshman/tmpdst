package server_test

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "mds/internal/controlplane/api/v1"
	"mds/internal/controlplane/statemachine"
	"mds/internal/core"
)

// A standby control plane serves no group call: every one answers Unavailable,
// so the client tries the next address (same rule as BrokerHeartbeat).
func TestStandbyGroupCallsUnavailable(t *testing.T) {
	cli, _, stop := startNode(t, statemachine.Config{
		Rank: 1, Heartbeat: core.Duration(time.Second), TakeoverAfter: core.Duration(time.Hour),
	})
	defer stop()
	ctx := t.Context()
	checks := map[string]error{}
	_, checks["JoinGroup"] = cli.JoinGroup(ctx, &cpv1.JoinGroupRequest{Group: "g", SessionTimeoutMs: 1000})
	_, checks["SyncGroup"] = cli.SyncGroup(ctx, &cpv1.SyncGroupRequest{Group: "g", MemberId: "m"})
	_, checks["GroupHeartbeat"] = cli.GroupHeartbeat(ctx, &cpv1.GroupHeartbeatRequest{Group: "g", MemberId: "m"})
	_, checks["LeaveGroup"] = cli.LeaveGroup(ctx, &cpv1.LeaveGroupRequest{Group: "g", Members: []string{"m"}})
	_, checks["GroupOffsetCommit"] = cli.GroupOffsetCommit(ctx, &cpv1.GroupOffsetCommitRequest{Group: "g"})
	_, checks["GroupOffsetFetch"] = cli.GroupOffsetFetch(ctx, &cpv1.GroupOffsetFetchRequest{Group: "g"})
	_, checks["DescribeGroups"] = cli.DescribeGroups(ctx, &cpv1.DescribeGroupsRequest{Groups: []string{"g"}})
	_, checks["ListGroups"] = cli.ListGroups(ctx, &cpv1.ListGroupsRequest{})
	for name, err := range checks {
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
			t.Errorf("%s: %v, want Unavailable", name, err)
		}
	}
}
