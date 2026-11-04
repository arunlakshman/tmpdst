package groups

import (
	"github.com/manifesterio/mds/internal/core"
)

// Store is where the coordinator writes group state and reads it back. The
// coordinator owns this port, and the control-plane node implements it over the
// replicated log. It takes GroupInstall, GroupAssign, GroupRemove and
// OffsetCommit (writes, logged) or OffsetFetch, DescribeGroup and ListGroups
// (reads, not logged).
type Store = core.OperationRunner
