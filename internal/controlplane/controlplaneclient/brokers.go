package controlplaneclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// Broker membership, client side. The server side is server/brokers.go.

// BrokerHeartbeat renews this broker's lease and returns the live broker set.
// A standby control plane answers ErrUnavailable, so the call moves to the next
// address.
func (c *Client) BrokerHeartbeat(ctx context.Context, self core.BrokerAddr) ([]core.BrokerAddr, error) {
	return callRPC(ctx, c, "BrokerHeartbeat", cpv1.ControlPlaneClient.BrokerHeartbeat,
		wireboundary.BrokerHeartbeatFromCore(self), wireboundary.BrokerHeartbeatRespToCore)
}
