package controlplane

import (
	"context"
	"errors"

	"github.com/manifesterio/mds/internal/core"
)

// ErrUnavailable is the infrastructure error for throttling, overload and
// exhausted optimistic retries. The gRPC server maps it to codes.Unavailable.
var ErrUnavailable = errors.New("cp: unavailable")

// GroupStore runs the consumer group operations of the control plane. Exec takes
// one of GroupInstall, GroupAssign, GroupRemove and OffsetCommit (writes, logged)
// or OffsetFetch, DescribeGroup and ListGroups (reads, not logged). A business
// error is the Err of the answer; the error result is infrastructure only.
type GroupStore interface {
	Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error)
}
