package cpclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
)

// GetStatus calls GetStatus on the preferred address.
func (c *Client) GetStatus(ctx context.Context) (*cpv1.GetStatusResponse, error) {
	var out *cpv1.GetStatusResponse
	err := c.do(ctx, "GetStatus", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.GetStatus(ctx, &cpv1.GetStatusRequest{})
		return e
	})
	return out, err
}
