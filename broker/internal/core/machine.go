package core

import "github.com/manifesterio/mds/dst/machine"

// StateMachine is the shape of every stateful component (broker, sim workload).
// Step is the only place state changes. It never blocks and never does I/O.
// An IOResult with an unknown, already-finished or wrong-kind RequestID must leave
// state unchanged.
type StateMachine interface {
	Step(Event) []Action
}

// HasStateHash lets the simulator fold state into a running trace hash after every
// step; the same seed must produce the same hash sequence (lesson 3).
type HasStateHash interface {
	StateHash() uint64
}

// Machine is a StateMachine whose state is hashed. Hashing is not optional.
type Machine interface {
	StateMachine
	HasStateHash
}

// MachineFactory builds a fresh Machine for one boot. It is called on first boot
// and after every crash, with a new BootNumber each time (lessons 4, 10).
type MachineFactory func(env Environment, self machine.NodeInstance) Machine

// A core machine takes one Event, with the time inside it. The dst contract
// (machine.Machine) is Step(now, in). OnContract and ContractFactory change the form
// of the call and nothing else. The simulator and the production loop (dst/live) both
// use them, so a broker runs the same code in both.

type contractMachine struct{ m Machine }

var _ machine.Machine[EventData, Action] = contractMachine{}

func (a contractMachine) Step(now machine.Now, in EventData) []Action {
	return a.m.Step(Event{At: now.Mono, Wall: now.Wall, Body: in})
}

func (a contractMachine) StateHash() uint64 { return a.m.StateHash() }

// OnContract runs m on the dst contract.
func OnContract(m Machine) machine.Machine[EventData, Action] { return contractMachine{m} }

// ContractFactory turns a core machine factory into a dst factory. A nil machine
// stays nil, so the caller can report it.
func ContractFactory(f MachineFactory) machine.Factory[EventData, Action] {
	return func(env machine.Env, self machine.NodeInstance) machine.Machine[EventData, Action] {
		m := f(env, self)
		if m == nil {
			return nil
		}
		return contractMachine{m}
	}
}
