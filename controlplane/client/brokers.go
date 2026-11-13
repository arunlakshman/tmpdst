package client

import (
	"context"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

// BrokerHeartbeat renews this broker's lease and returns the live broker set.
// A standby control plane answers ErrUnavailable, so the call moves to the next
// address.
func (c *Client) BrokerHeartbeat(ctx context.Context, self contract.BrokerAddr) ([]contract.BrokerAddr, error) {
	return callRPC(ctx, c, "BrokerHeartbeat", cpv1.ControlPlaneClient.BrokerHeartbeat,
		brokerHeartbeatFromContract(self), brokerHeartbeatRespToContract)
}
