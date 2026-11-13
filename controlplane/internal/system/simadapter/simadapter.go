// Package simadapter runs the two machines of the control plane (the replica and the
// rebalance machine) as dst sim.Process values.
//
// The machines take the dst v2 contract (machine.Machine with typed input and output).
// The simulator host code still speaks the old contract, sim.Process: one Step takes an
// inbox value of type any and returns values of type any. This package is the bridge
// between the two. It changes the form of the values and nothing else: the same inputs reach the machine in the same order, the
// outputs come back in the same order, and no random number is drawn here.
//
// This is a temporary package. The production server does not use it any more (it runs the
// machines on dst/live). It is deleted when the simulator host code moves to the dst
// kernel (plan T1.11 step c).
package simadapter

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
)

// now makes the time that a machine sees from the time of a sim step.
func now(cx sim.Context) machine.Now {
	return machine.Now{Mono: machine.NodeTime(cx.Now()), Wall: machine.RealTime(cx.Wall())}
}

// Replica runs a replica as a sim.Process.
type Replica struct {
	id sim.ProcessID
	m  *replica.Replica
}

var _ sim.Process = (*Replica)(nil)

// NewReplica wraps m. id is the process id, the name of the node.
func NewReplica(id sim.ProcessID, m *replica.Replica) *Replica {
	return &Replica{id: id, m: m}
}

// ID implements sim.Process.
func (p *Replica) ID() sim.ProcessID { return p.id }

// StateHash implements sim.Process.
func (p *Replica) StateHash() uint64 { return p.m.StateHash() }

// Step implements sim.Process. The inbox value must be a runtime.EventData; any other
// value is ignored. The timers and the end of the incarnation become the dst control
// values (SetTimer, CancelTimer, Crash, Exit). Every other action (an IO call or a reply)
// passes as it is.
func (p *Replica) Step(cx sim.Context, inbox any) []any {
	in, ok := inbox.(runtime.EventData)
	if !ok {
		return nil
	}
	var out []any
	for _, a := range p.m.Step(now(cx), in) {
		switch a := a.(type) {
		case runtime.SetTimer:
			out = append(out, sim.SetTimer{ID: uint64(a.ID), After: sched.TimeNs(a.After), Body: runtime.TimerFired{ID: a.ID, Gen: a.Gen}})
		case runtime.CancelTimer:
			out = append(out, sim.CancelTimer{ID: uint64(a.ID)})
		case runtime.StopForRestart:
			out = append(out, sim.Crash{Cause: a.Cause})
		case runtime.ShutdownDone:
			out = append(out, sim.Exit{})
		case runtime.ObjectPut, runtime.ObjectGet, runtime.ObjectDelete, runtime.ObjectList,
			runtime.ControlPlaneCall, runtime.ControlPlaneReply,
			runtime.LogAppend, runtime.LogRead, runtime.LogGetEnd, runtime.LogDeleteOld, runtime.LogClaimEpoch:
			out = append(out, a)
		}
	}
	return out
}

// Rebalance runs a rebalance machine as a sim.Process.
type Rebalance struct {
	id sim.ProcessID
	m  *rebalance.Machine
}

var _ sim.Process = (*Rebalance)(nil)

// NewRebalance wraps m. id is the process id. The machine sends to itself with this id.
func NewRebalance(id sim.ProcessID, m *rebalance.Machine) *Rebalance {
	return &Rebalance{id: id, m: m}
}

// ID implements sim.Process.
func (p *Rebalance) ID() sim.ProcessID { return p.id }

// StateHash implements sim.Process.
func (p *Rebalance) StateHash() uint64 { return p.m.StateHash() }

// Step implements sim.Process. The inbox value must be a rebalance.Input, or a
// runtime.TimerFired (the value that a timer of the host delivers), which becomes a
// rebalance.TimerFired. Any other value is ignored. A SetTimer becomes a sim.SetTimer
// whose body is a runtime.TimerFired, a SendSelf becomes a sim.Send to this process, and
// the other outputs pass as they are.
func (p *Rebalance) Step(cx sim.Context, inbox any) []any {
	var in rebalance.Input
	switch v := inbox.(type) {
	case rebalance.Input:
		in = v
	case runtime.TimerFired:
		in = rebalance.TimerFired{ID: v.ID, Gen: v.Gen}
	default:
		return nil
	}
	var out []any
	for _, o := range p.m.Step(now(cx), in) {
		switch o := o.(type) {
		case rebalance.SetTimer:
			out = append(out, sim.SetTimer{ID: uint64(o.ID), After: sched.TimeNs(o.After), Body: runtime.TimerFired{ID: o.ID, Gen: o.Gen}})
		case rebalance.CancelTimer:
			out = append(out, sim.CancelTimer{ID: uint64(o.ID)})
		case rebalance.SendSelf:
			out = append(out, sim.Send{To: p.id, Body: self(o.Msg)})
		case rebalance.SubmitCommand, rebalance.SubmitRead, rebalance.Reply:
			out = append(out, o)
		}
	}
	return out
}

// self is the body of a send to the machine itself. A TimerFired goes out as the value
// that a timer of the host delivers, so that it looks the same as a real timer.
func self(in rebalance.Input) any {
	if t, ok := in.(rebalance.TimerFired); ok {
		return runtime.TimerFired{ID: t.ID, Gen: t.Gen}
	}
	return in
}
