// Package sim is the process runner that wires eng engines to rng/sched/trace/assert/fault.
package sim

import (
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
)

// ProcessID names a process in one run.
type ProcessID = eng.ActorID

// Process is one actor the runner steps.
//
// Step takes one inbox value and returns zero or more outbound values.
// Outbound values are either a control struct in this package (Send, SetTimer,
// CancelTimer, Crash, Restart) or a domain value an eng.Engine understands.
// There is no separate Event/Action type: those names collide with "simulated
// action" in everyday English.
type Process interface {
	ID() ProcessID
	StateHash() uint64
	Step(cx Context, inbox any) (out []any)
}

// Context is lent to Process.Step.
type Context interface {
	Now() sched.TimeNs
	// Wall is the UTC clock in ns: Config.WallStart plus Now in a simulation.
	Wall() sched.TimeNs
	Rand() *rng.Rand
	Faults() *fault.Injector
	Assert() *assert.Log
}

// Send delivers Body to To at the next turn (delay 0).
type Send struct {
	To   ProcessID
	Body any
}

// SetTimer arms a timer on the issuing process. Body is delivered when it fires.
type SetTimer struct {
	ID    uint64
	After sched.TimeNs
	Body  any
}

// CancelTimer cancels a pending timer on the issuing process.
type CancelTimer struct {
	ID uint64
}

// Crash marks a process crashed: Who, or the issuing process when Who is empty.
// Deliveries to it are ignored until Restart (or Spawn) clears the crash. Pending
// timers still fire; Step is a no-op while crashed. Crash does not remove the
// process from the runner. Cause is free text for traces and for a production
// runner (it reports why the process stopped).
type Crash struct {
	Who   ProcessID
	Cause string
}

// Exit ends the issuing process on purpose (a graceful shutdown). The runner
// treats it like Crash, and Exited reports it, so a supervisor can tell a
// shutdown from a failure and not restart the process.
type Exit struct{}

// Restart clears Crash for Who and starts a new incarnation of it. If New is
// non-nil it replaces the process object (its ID must be Who). If Boot is
// non-nil, it is delivered to Who at delay 0 after the clear. Any process may
// return Restart; Who need not be the issuer.
//
// A new incarnation never sees anything that belongs to the old one: timers the
// old one armed are dropped, and so are replies that an engine sends back to
// the old one (see "Incarnation rules" in runner.go).
type Restart struct {
	Who  ProcessID
	Boot any
	New  Process
}

// Config configures one Runner.
type Config struct {
	Seed uint64
	// WallStart is the UTC time (ns) at virtual time zero. Context.Wall is WallStart + Now.
	WallStart sched.TimeNs
	Steps     int // stop after this many Process.Step calls (0 = drain until empty / MaxPops)
	MaxPops   int // hard cap on scheduler pops; default 1_000_000
	// OnStep, if set, runs after every Process.Step and before the runner acts
	// on the outbound values. The process has its new state then, so an
	// observer can check invariants and record a trace. It must not call the Runner.
	OnStep func(StepInfo)
}

// StepInfo describes one Process.Step call, for Config.OnStep.
type StepInfo struct {
	Who   ProcessID
	At    sched.TimeNs // virtual time of the step
	Inbox any
	Out   []any // what Step returned, before the runner acts on it
}

// Failure is one assert onFail record collected during Run.
type Failure struct {
	Property string
	Detail   string
}

// Result is the fingerprint and counters of one Run.
type Result struct {
	Hash     uint64
	Steps    int // Process.Step calls
	Pops     int // scheduler pops
	Calls    uint64
	Failures []Failure
}

const defaultMaxPops = 1_000_000
