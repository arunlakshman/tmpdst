// Package eng defines the engine hand-out interface for dst runners.
package eng

import (
	"errors"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
)

// ID names one engine instance inside a run.
type ID string

// ActorID names a process (or other actor) the runner can deliver to.
type ActorID string

// ErrNotMine means the action is outside this engine's vocabulary.
var ErrNotMine = errors.New("eng: not mine")

// Context is what the Runner lends an Engine during Handle or OnSelf.
type Context interface {
	Now() sched.TimeNs
	Rand() *rng.Rand
	Faults() *fault.Injector
	Assert() *assert.Log
	From() ActorID
	// Deliver schedules event to actor after delay (0 = next turn at now).
	Deliver(to ActorID, after sched.TimeNs, event any)
	// Self schedules an engine-internal wake (opaque body).
	Self(after sched.TimeNs, body any)
}

// Engine handles domain actions that are not sim control actions.
type Engine interface {
	ID() ID
	StateHash() uint64
	// Handle returns ErrNotMine when act is outside this engine's vocabulary.
	Handle(cx Context, act any) error
	// OnSelf runs a body previously scheduled with Self.
	OnSelf(cx Context, body any)
}
