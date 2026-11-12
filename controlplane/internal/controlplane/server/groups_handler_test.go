package server_test

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// A standby control plane serves no group call: every one answers Unavailable,
// so the client tries the next address (same rule as BrokerHeartbeat).
func TestStandbyGroupCallsUnavailable(t *testing.T) {
	cli, _, stop := startNode(t, replica.Config{
		Rank: 1, Heartbeat: runtime.Duration(time.Second), TakeoverAfter: runtime.Duration(time.Hour),
	})
	defer stop()
	ctx := t.Context()
	checks := map[string]error{}
	// A well-formed join: a malformed one is ErrInvalid on any node (rule 1 comes first).
	_, checks["JoinGroup"] = cli.JoinGroup(ctx, &cpv1.JoinGroupRequest{Group: "g", SessionTimeoutMs: 1000,
		ProtocolType: "consumer", Protocols: []*cpv1.GroupProtocol{{Name: "range"}}})
	_, checks["SyncGroup"] = cli.SyncGroup(ctx, &cpv1.SyncGroupRequest{Group: "g", MemberId: "m"})
	_, checks["Heartbeat"] = cli.Heartbeat(ctx, &cpv1.HeartbeatRequest{Group: "g", MemberId: "m"})
	_, checks["LeaveGroup"] = cli.LeaveGroup(ctx, &cpv1.LeaveGroupRequest{Group: "g", Members: []string{"m"}})
	_, checks["OffsetCommit"] = cli.OffsetCommit(ctx, &cpv1.OffsetCommitRequest{Group: "g"})
	_, checks["OffsetFetch"] = cli.OffsetFetch(ctx, &cpv1.OffsetFetchRequest{Group: "g"})
	_, checks["DescribeGroups"] = cli.DescribeGroups(ctx, &cpv1.DescribeGroupsRequest{Groups: []string{"g"}})
	_, checks["ListGroups"] = cli.ListGroups(ctx, &cpv1.ListGroupsRequest{})
	for name, err := range checks {
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
			t.Errorf("%s: %v, want Unavailable", name, err)
		}
	}
}
