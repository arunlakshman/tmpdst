package speccheck

import (
	cpv1 "github.com/manifesterio/mds/internal/controlplane/api/v1"
	"github.com/manifesterio/mds/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds/internal/core"
)

// ExecRequest runs a generated CommitSegment/FindSegments/DescribeTopics request
// through the oracle. Apply logic stays in this package (decision 21); only
// the message types are shared with the wire API.
func (c *ControlPlane) ExecRequest(request core.RequestID, msg any) core.ControlPlaneDone {
	switch m := msg.(type) {
	case *cpv1.CommitSegmentRequest:
		return c.Exec(core.ControlPlaneCall{Req: request, Op: wireboundary.CommitSegmentToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.LookupRequest:
		return c.Exec(core.ControlPlaneCall{Req: request, Op: wireboundary.LookupToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.DescribeTopicsRequest:
		return c.Exec(core.ControlPlaneCall{Req: request, Op: wireboundary.DescribeTopicsToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.CreateTopicRequest:
		code := c.CreateTopic(m.GetTopic(), m.GetPartitions())
		return core.ControlPlaneDone{Req: request, Err: code}
	default:
		return core.ControlPlaneDone{Req: request, Err: core.ErrInvalid}
	}
}
