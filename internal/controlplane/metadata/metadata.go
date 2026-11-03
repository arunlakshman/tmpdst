package metadata

import (
	"fmt"

	"github.com/manifesterio/mds/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/controlplane/topics"
	"github.com/manifesterio/mds/internal/core"
)

// Store is the control-plane replicated state over a kv.Store. It is the kernel:
// it holds the modules (topics, groups) and sends each operation to the one that
// owns it. Writes (commands) must be applied by one goroutine in log order;
// reads may run concurrently.
type Store struct {
	keyValue kv.Store
	modules  []module.Module
}

// New returns a metadata store over kv (empty, or restored from a checkpoint).
func New(keyValue kv.Store) *Store {
	return &Store{keyValue: keyValue, modules: []module.Module{
		topics.Module{},
		storage.NewModule(topics.PartitionCount),
	}}
}

// Close closes the store.
func (a *Store) Close() error { return a.keyValue.Close() }

// mustStore panics when a store call failed. This is on purpose: the
// metadata store stops (fail-stop) instead of going on with bad state.
func mustStore(err error) {
	if err != nil {
		panic(fmt.Errorf("metadata: store failed: %w", err))
	}
}

// owner is the module that handles operation, and how it treats it.
func (a *Store) owner(operation core.ControlPlaneOperation) (module.Module, module.Mode) {
	for _, m := range a.modules {
		if mode := m.Mode(operation); mode != module.NotMine {
			return m, mode
		}
	}
	return nil, module.NotMine
}

// Mode reports how the store treats operation: Logged (a command), ReadOnly, or
// NotMine (an operation no module handles).
func (a *Store) Mode(operation core.ControlPlaneOperation) module.Mode {
	_, mode := a.owner(operation)
	return mode
}

// Write applies a Logged operation at log slot slot, in its own transaction.
// ErrInvalid if no module handles it as a command.
func (a *Store) Write(slot int64, operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	m, mode := a.owner(operation)
	if mode != module.Logged {
		return core.ControlPlaneDone{Err: core.ErrInvalid}, nil
	}
	var d core.ControlPlaneDone
	err := a.keyValue.Update(func(transaction kv.Transaction) { d = m.Write(transaction, slot, operation) })
	return d, err
}

// Read answers a ReadOnly operation in a read transaction. ErrInvalid if no
// module handles it as a read.
func (a *Store) Read(operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	m, mode := a.owner(operation)
	if mode != module.ReadOnly {
		return core.ControlPlaneDone{Err: core.ErrInvalid}, nil
	}
	var d core.ControlPlaneDone
	err := a.keyValue.View(func(transaction kv.Transaction) { d = m.Read(transaction, operation) })
	return d, err
}

// Exec runs one ControlPlaneCall like speccheck.ControlPlane.Exec. For a CommitSegment the
// call's Req is the log slot (it orders the idempotency window for Trim). A
// store failure panics.
func (a *Store) Exec(act core.IOCall) core.IOResult {
	call, ok := act.(core.ControlPlaneCall)
	if !ok {
		panic("metadata: not a ControlPlaneCall")
	}
	var d core.ControlPlaneDone
	var err error
	switch a.Mode(call.Op) {
	case module.Logged:
		d, err = a.Write(int64(call.Req), call.Op)
		if _, isCreate := call.Op.(core.CreateTopic); isCreate && d.Err == core.OK {
			d.Outcome = core.Applied
		}
	case module.ReadOnly:
		d, err = a.Read(call.Op)
	default:
		d.Err = core.ErrInvalid
	}
	mustStore(err)
	d.Req = call.Req
	return d
}

// Command is one CommitSegment at its log slot.
type Command struct {
	Slot int64
	Op   core.CommitSegment
}

// CommitBatch applies commands in order in ONE store transaction (each sees
// the writes of the ones before it) and returns their answers. It is what a
// replica uses to apply the slots one tail read delivered: the same state as
// applying them one by one, with far fewer page writes in the store.
func (a *Store) CommitBatch(cmds []Command) ([]core.ControlPlaneDone, error) {
	var out []core.ControlPlaneDone
	err := a.keyValue.Update(func(transaction kv.Transaction) {
		out = make([]core.ControlPlaneDone, len(cmds))
		for i, c := range cmds {
			out[i] = topics.Commit(transaction, c.Slot, c.Op)
		}
	})
	return out, err
}
