// Package machine is the contract between a sans-IO state machine and the code
// that runs it.
//
// A machine is a state machine that does no IO itself ("sans-IO"). It takes one
// input value and returns output values as plain data. Somebody else does the
// real work: the simulation kernel (package kernel) in a test, or the
// production loop in a real process. Both run the same machine.
//
// Product machine code imports only this package, never the kernel.
//
// The inputs and outputs of a machine are sealed sum types owned by the
// product: an interface with an unexported marker method, marked with a
// "//sumtype:decl" comment, so the gochecksumtype linter can check that every
// type switch over it lists every case. Every case is a Message: it can fold
// itself into the trace hash.
package machine

import (
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/trace"
)

// NodeTime is node-local monotonic time in nanoseconds. In a simulation it
// includes the fixed clock skew of the node.
type NodeTime int64

// RealTime is UTC, in nanoseconds since the Unix epoch.
type RealTime int64

// Duration is a length of time in nanoseconds.
type Duration int64

// Units of Duration.
const (
	Nanosecond  Duration = 1
	Microsecond          = 1000 * Nanosecond
	Millisecond          = 1000 * Microsecond
	Second               = 1000 * Millisecond
)

// ID is a 16-byte id drawn from the seeded random stream (never from uuid.New,
// so a simulation stays repeatable).
type ID [16]byte

// NodeID is the stable name of a node.
type NodeID string

// BootNumber is new for every boot of a node.
type BootNumber ID

// RequestID matches an IO call with its completion.
type RequestID uint64

// TimerID names a timer of one node.
type TimerID uint64

// TimerVersion lets a machine ignore a TimerFired of an old timer.
type TimerVersion uint64

// NodeInstance names one boot (one incarnation) of one node.
type NodeInstance struct {
	Node NodeID
	Inc  BootNumber
}

// Now is the time a machine sees when it takes one input. It has two clocks.
type Now struct {
	Mono NodeTime // node clock, with skew; use it for deadlines and leases
	Wall RealTime // UTC
}

// Message is every input and every output of a machine. Fold writes the value
// into the trace hash: a fixed tag number first, then the fields. It never uses
// %T or reflection, so renaming a Go type does not move a golden hash.
type Message interface {
	Fold(h *trace.Hash)
}

// Machine is a sans-IO state machine. Step is the only place where its state
// changes. Step never blocks and never does IO. In and Out are sealed sum
// types owned by the product.
//
// StateHash is a hash of the whole state. The kernel folds it into the trace
// hash after every step.
type Machine[In, Out Message] interface {
	Step(now Now, in In) []Out
	StateHash() uint64
}

// Factory builds a fresh machine for one boot. It is called on the first boot
// and after every crash, with a new boot number each time. Nothing in memory
// survives a crash, so the factory never reuses an old machine.
type Factory[In, Out Message] func(env Env, self NodeInstance) Machine[In, Out]

// Env is the only way that randomness, buggify and assertions reach a machine.
// Time is not here: it comes with each Step.
type Env interface {
	Rand
	Faults
	Assertions
}

// Rand is the seeded random stream of the run, as a machine sees it.
type Rand interface {
	Uint64() uint64
	// Intn returns a value in [0, n). It returns 0, and draws nothing, when n <= 0.
	Intn(n int) int
	// Between returns a value in [lo, hi]: both ends are included. It returns
	// lo, and draws nothing, when hi <= lo.
	Between(lo, hi int64) int64
	// Chance is true with probability p. It draws nothing when p <= 0.
	Chance(p float64) bool
	// NewID draws two Uint64 values and puts them in the id, little-endian.
	NewID() ID
}

// Faults is buggify: faults the code injects on purpose at declared sites.
type Faults interface {
	// Maybe switches the site on for the run on first use (probability 0.5),
	// then fires with probability 0.05 when it is on.
	Maybe(site fault.Site) bool
	// MaybeP is Maybe with fire probability p.
	MaybeP(site fault.Site, p float64) bool
	// Fault fires with probability p. It never fires outside the fault phase.
	Fault(site fault.Site, p float64) bool
}

// Assertions record properties. A failed safety property is reported and the
// run keeps going.
type Assertions interface {
	Always(cond bool, p assert.Always, details ...assert.Pair)
	AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, details ...assert.Pair)
	Sometimes(cond bool, p assert.Sometimes)
	Reachable(p assert.Reachable)
	Unreachable(p assert.Unreachable, details ...assert.Pair)
}

// End says how an incarnation ended.
type End uint8

// The ways an incarnation can end.
const (
	Crash End = iota // killed from outside (nemesis, attrition, stop deadline)
	Trip             // the machine asked for a restart: its memory is not trusted
	Exit             // the machine finished a graceful stop
)

// String returns the plain name of the end.
func (e End) String() string {
	switch e {
	case Crash:
		return "crash"
	case Trip:
		return "trip"
	case Exit:
		return "exit"
	}
	return "unknown"
}

// Class picks one of the two inboxes of a node. Control is always taken
// before Data.
type Class uint8

// The two inbox classes.
const (
	Control Class = iota
	Data
)

// Clock is time for code that is not a machine (run loops, adapters).
type Clock interface {
	Mono() NodeTime
	Wall() RealTime
	AfterFunc(d Duration, f func()) Timer
}

// Timer is a timer made by Clock.AfterFunc.
type Timer interface {
	// Stop stops the timer. It reports false if the timer already fired or
	// was already stopped.
	Stop() bool
}
