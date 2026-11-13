// Package clock has the two implementations of machine.Clock that the broker and
// the control plane share outside the simulator:
//
//   - Real is the production clock. It only converts types over dst/realclock.
//     It is nondeterministic by design and is not in the determinism gate.
//   - Fake is the test clock. It only converts types: the timers, the lock and
//     Advance are dst/syncclock. Time moves only when the caller calls Advance.
//     Do not use it inside the simulator, which has its own single-goroutine
//     dst/clock.
package clock

import (
	"github.com/manifesterio/mds/dst/machine"
	dstreal "github.com/manifesterio/mds/dst/realclock"
	"github.com/manifesterio/mds/dst/sched"
)

// Real is the process clock.
type Real struct{ dstreal.Clock }

var _ machine.Clock = Real{}

// NewReal returns a Real clock anchored at the current time.
func NewReal() Real { return Real{dstreal.New()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Real) Mono() machine.NodeTime { return machine.NodeTime(c.Clock.Mono()) }

// Wall returns the current wall-clock time in nanoseconds.
func (c Real) Wall() machine.RealTime { return machine.RealTime(c.Clock.Wall()) }

// AfterFunc calls f once after d, on its own goroutine.
func (c Real) AfterFunc(d machine.Duration, f func()) machine.Timer {
	return c.Clock.AfterFunc(sched.TimeNs(d), f)
}
