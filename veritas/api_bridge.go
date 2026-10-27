package veritas

import (
	cpv1 "manifester/controlplane/api/v1"
	"manifester/controlplane/apiconv"
	"manifester/core"
)

// ExecRequest runs a generated CommitSegment/Lookup/DescribeTopics request
// through the oracle. Apply logic stays in this package (decision 21); only
// the message types are shared with the wire API.
func (c *ControlPlane) ExecRequest(req core.ReqID, msg any) core.CPDone {
	switch m := msg.(type) {
	case *cpv1.CommitSegmentRequest:
		return c.Exec(core.CPCall{Req: req, Op: apiconv.CommitSegmentToCore(m)}).(core.CPDone)
	case *cpv1.LookupRequest:
		return c.Exec(core.CPCall{Req: req, Op: apiconv.LookupToCore(m)}).(core.CPDone)
	case *cpv1.DescribeTopicsRequest:
		return c.Exec(core.CPCall{Req: req, Op: apiconv.DescribeTopicsToCore(m)}).(core.CPDone)
	case *cpv1.CreateTopicRequest:
		code := c.CreateTopic(m.GetTopic(), m.GetPartitions())
		return core.CPDone{Req: req, Err: code}
	default:
		return core.CPDone{Req: req, Err: core.ErrInvalid}
	}
}
