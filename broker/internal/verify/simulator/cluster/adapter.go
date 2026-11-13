package cluster

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// The core machines (the broker, the workloads) take one core.Event, with the
// time inside it. The dst v2 contract is Step(now, in). The adapters (core.OnContract
// for a broker, driverAdapter for a workload) change the form of the call and nothing else. The product code of core and broker is
// the same as in production.

// adaptFactory turns a core machine factory into a dst v2 factory. A nil
// machine stays nil, so the kernel reports it. It is core.ContractFactory, the
// same adapter that the production loop uses.
func adaptFactory(f core.MachineFactory) machine.Factory[core.EventData, core.Action] {
	return core.ContractFactory(f)
}

// phaseTimer is the timer id that carries a phase change to a workload. The kernel
// hands a phase change to a driver as one input of its input type, and the input
// type of a node is core.EventData, which cannot get a new case here. So the phase
// travels as a core.TimerFired with this reserved id and the phase in Gen. The
// workloads own the small timer ids (see internal/session), so there is no clash.
const phaseTimer machine.TimerID = 1 << 62

// phaseEvent is NodeSpec.OnPhase of a workload node.
func phaseEvent(p kernel.Phase) (core.EventData, bool) {
	return core.TimerFired{ID: phaseTimer, Gen: machine.TimerVersion(p)}, true
}

// bootEvent is NodeSpec.Boot of every node.
func bootEvent(self machine.NodeInstance) core.EventData { return core.NodeStarted{Self: self} }

// stopEvent is NodeSpec.StopMsg of a broker node.
func stopEvent(deadline machine.NodeTime) core.EventData { return core.StopRequest{Deadline: deadline} }

// driverAdapter runs a workload. A phase change (a TimerFired with phaseTimer)
// goes to Workload.OnPhase. Every other input is a normal Step.
type driverAdapter struct{ w Workload }

var _ machine.Machine[core.EventData, core.Action] = driverAdapter{}

func (a driverAdapter) Step(now machine.Now, in core.EventData) []core.Action {
	if t, ok := in.(core.TimerFired); ok && t.ID == phaseTimer {
		return a.w.OnPhase(kernel.Phase(t.Gen))
	}
	return a.w.Step(core.Event{At: now.Mono, Wall: now.Wall, Body: in})
}

func (a driverAdapter) StateHash() uint64 { return a.w.StateHash() }
