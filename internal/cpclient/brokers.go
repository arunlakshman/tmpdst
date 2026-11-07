package cpclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds/api/controlplane/v1"
	"github.com/manifesterio/mds/internal/core"
)

// BrokerHeartbeat renews this broker's lease and returns the live broker set.
// A standby control plane answers ErrUnavailable, so the call moves to the next
// address.
func (c *Client) BrokerHeartbeat(ctx context.Context, self core.BrokerAddr) ([]core.BrokerAddr, error) {
	return callRPC(ctx, c, "BrokerHeartbeat", cpv1.ControlPlaneClient.BrokerHeartbeat,
		brokerHeartbeatFromCore(self), brokerHeartbeatRespToCore)
}
