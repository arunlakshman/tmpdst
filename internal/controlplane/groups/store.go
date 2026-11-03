package groups

import (
	"context"

	"github.com/manifesterio/mds/internal/core"
)

// Store is where the coordinator writes group state and reads it back. The
// coordinator owns this port, and the control-plane node implements it over the
// replicated log. Exec takes one of GroupInstall, GroupAssign, GroupRemove and
// OffsetCommit (writes, logged) or OffsetFetch, DescribeGroup and ListGroups
// (reads, not logged). A business error is the Err of the answer; the error
// result is infrastructure only.
type Store interface {
	Exec(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error)
}
