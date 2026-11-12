// Package realclock is the production implementation of machine.Clock. It is
// nondeterministic by design and is not in the determinism gate.
package realclock

import (
	"time"

	"github.com/manifesterio/mds/dst/machine"
)

// Clock is the process clock: Mono counts from the clock's creation using
// Go's monotonic reading, Wall is UTC.
type Clock struct{ start time.Time }

var _ machine.Clock = Clock{}

// New returns a Clock anchored at the current time.
func New() Clock { return Clock{start: time.Now()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Clock) Mono() machine.NodeTime { return machine.NodeTime(time.Since(c.start)) }

// Wall returns the current wall-clock time in nanoseconds.
func (Clock) Wall() machine.RealTime { return machine.RealTime(time.Now().UnixNano()) }

// AfterFunc calls f once after d, on its own goroutine.
func (Clock) AfterFunc(d machine.Duration, f func()) machine.Timer {
	return time.AfterFunc(time.Duration(d), f)
}
