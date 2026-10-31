package speccheck

import (
	cpv1 "mds/internal/controlplane/api/v1"
	"mds/internal/controlplane/apiconvert"
	"mds/internal/core"
)

// ExecRequest runs a generated CommitSegment/FindSegments/DescribeTopics request
// through the oracle. Apply logic stays in this package (decision 21); only
// the message types are shared with the wire API.
func (c *ControlPlane) ExecRequest(request core.RequestID, msg any) core.ControlPlaneDone {
	switch m := msg.(type) {
	case *cpv1.CommitSegmentRequest:
		return c.Exec(core.ControlPlaneCall{Req: request, Op: apiconvert.CommitSegmentToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.LookupRequest:
		return c.Exec(core.ControlPlaneCall{Req: request, Op: apiconvert.LookupToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.DescribeTopicsRequest:
		return c.Exec(core.ControlPlaneCall{Req: request, Op: apiconvert.DescribeTopicsToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.CreateTopicRequest:
		code := c.CreateTopic(m.GetTopic(), m.GetPartitions())
		return core.ControlPlaneDone{Req: request, Err: code}
	default:
		return core.ControlPlaneDone{Req: request, Err: core.ErrInvalid}
	}
}
