package veritas

import (
	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/apiconv"
	"manifester/internal/core"
)

// ExecRequest runs a generated CommitSegment/FindSegments/DescribeTopics request
// through the oracle. Apply logic stays in this package (decision 21); only
// the message types are shared with the wire API.
func (c *ControlPlane) ExecRequest(req core.RequestID, msg any) core.ControlPlaneDone {
	switch m := msg.(type) {
	case *cpv1.CommitSegmentRequest:
		return c.Exec(core.ControlPlaneCall{Req: req, Op: apiconv.CommitSegmentToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.LookupRequest:
		return c.Exec(core.ControlPlaneCall{Req: req, Op: apiconv.LookupToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.DescribeTopicsRequest:
		return c.Exec(core.ControlPlaneCall{Req: req, Op: apiconv.DescribeTopicsToCore(m)}).(core.ControlPlaneDone)
	case *cpv1.CreateTopicRequest:
		code := c.CreateTopic(m.GetTopic(), m.GetPartitions())
		return core.ControlPlaneDone{Req: req, Err: code}
	default:
		return core.ControlPlaneDone{Req: req, Err: core.ErrInvalid}
	}
}
