package cp

import (
	"context"
	"errors"

	"manifester/core"
)

// ErrUnavailable is the infrastructure error for throttling, overload and
// exhausted optimistic retries. The HTTP handler maps it to 503.
var ErrUnavailable = errors.New("cp: unavailable")

// Store is the control-plane state machine behind the HTTP handler. It must
// behave exactly like veritas.ControlPlane (op for op) and be safe for
// concurrent use by many replicas. A business error is returned as a non-OK
// core.ErrCode with a nil error; error is infrastructure only.
type Store interface {
	Register(ctx context.Context, op core.Register) (core.RegisterResp, error)
	InitProducer(ctx context.Context) (core.InitProducerResp, error)
	CommitSegment(ctx context.Context, op core.CommitSegment) (core.CommitSegmentResp, core.ErrCode, error)
	Lookup(ctx context.Context, op core.Lookup) (core.LookupResp, core.ErrCode, error)
	DescribeTopics(ctx context.Context, op core.DescribeTopics) (core.DescribeTopicsResp, error)
	CreateTopic(ctx context.Context, name string, partitions int32) (core.ErrCode, error)
}

// ParseErrCode is the inverse of core.ErrCode.String.
func ParseErrCode(s string) (core.ErrCode, bool) {
	for c := core.OK; c <= core.ErrOutOfOrder; c++ {
		if c.String() == s {
			return c, true
		}
	}
	return 0, false
}
