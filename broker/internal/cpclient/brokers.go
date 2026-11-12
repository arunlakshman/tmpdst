package cpclient

import (
	"context"

	"github.com/manifesterio/mds/broker/internal/core"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
)

// BrokerHeartbeat renews this broker's lease and returns the live broker set.
// A standby control plane answers ErrUnavailable, so the call moves to the next
// address.
func (c *Client) BrokerHeartbeat(ctx context.Context, self core.BrokerAddr) ([]core.BrokerAddr, error) {
	return callRPC(ctx, c, "BrokerHeartbeat", cpv1.ControlPlaneClient.BrokerHeartbeat,
		brokerHeartbeatFromCore(self), brokerHeartbeatRespToCore)
}
