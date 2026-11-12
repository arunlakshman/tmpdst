// Package realclock is the production implementation of core.Clock. It is
// nondeterministic by design and is not in the determinism gate.
package realclock

import (
	"time"

	"github.com/manifesterio/mds/broker/internal/core"
)

// Clock is the process clock: Mono counts from the clock's creation using
// Go's monotonic reading, Wall is UTC.
type Clock struct{ start time.Time }

var _ core.Clock = Clock{}

// New returns a Clock anchored at the current time.
func New() Clock { return Clock{start: time.Now()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Clock) Mono() core.NodeTime { return core.NodeTime(time.Since(c.start)) }

// Wall returns the current wall-clock time in nanoseconds.
func (Clock) Wall() core.RealTime { return core.RealTime(time.Now().UnixNano()) }

// AfterFunc calls f once after d, on its own goroutine.
func (Clock) AfterFunc(d core.Duration, f func()) core.Timer {
	return time.AfterFunc(time.Duration(d), f)
}
