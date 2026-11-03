package storage

import (
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

// Module is the groups context in the replicated state: the four group writes
// are logged and the three group reads are not. It implements module.Module.
type Module struct{ partitions Partitions }

var _ module.Module = Module{}

// NewModule returns the module. An offset commit asks partitions how many
// partitions its topic has.
func NewModule(partitions Partitions) Module { return Module{partitions: partitions} }

// Mode reports how the module treats operation.
func (Module) Mode(operation core.ControlPlaneOperation) module.Mode {
	switch operation.(type) {
	case core.GroupInstall, core.GroupAssign, core.GroupRemove, core.OffsetCommit:
		return module.Logged
	case core.OffsetFetch, core.DescribeGroup, core.ListGroups:
		return module.ReadOnly
	}
	return module.NotMine
}

// Write applies a group write. The slot is not used: group state does not
// depend on where in the log a command sits.
func (m Module) Write(transaction kv.Transaction, _ int64, operation core.ControlPlaneOperation) core.ControlPlaneDone {
	return write(transaction, operation, m.partitions)
}

// Read answers a group read.
func (Module) Read(transaction kv.Transaction, operation core.ControlPlaneOperation) core.ControlPlaneDone {
	switch x := operation.(type) {
	case core.OffsetFetch:
		return offsetFetch(transaction, x)
	case core.DescribeGroup:
		return describeGroup(transaction, x)
	case core.ListGroups:
		return listGroups(transaction, x)
	}
	return core.ControlPlaneDone{Err: core.ErrInvalid}
}
