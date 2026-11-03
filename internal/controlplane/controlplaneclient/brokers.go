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
//
//nolint:dupl // each ControlPlaneOperation keeps its own request/response conversion
func (c *Client) BrokerHeartbeat(ctx context.Context, self core.BrokerAddr) ([]core.BrokerAddr, error) {
	var out *cpv1.BrokerHeartbeatResponse
	err := c.do(ctx, "BrokerHeartbeat", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.BrokerHeartbeat(ctx, wireboundary.BrokerHeartbeatFromCore(self))
		return e
	})
	if err != nil {
		return nil, err
	}
	return wireboundary.BrokerHeartbeatRespToCore(out), nil
}
