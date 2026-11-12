// Package realclock is the production implementation of runtime.Clock. It only
// converts types over dst/realclock. It is nondeterministic by design and is
// not in the determinism gate.
package realclock

import (
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	dstreal "github.com/manifesterio/mds/dst/realclock"
	"github.com/manifesterio/mds/dst/sched"
)

// Clock is the process clock.
type Clock struct{ dstreal.Clock }

var _ runtime.Clock = Clock{}

// New returns a Clock anchored at the current time.
func New() Clock { return Clock{dstreal.New()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Clock) Mono() runtime.NodeTime { return runtime.NodeTime(c.Clock.Mono()) }

// Wall returns the current wall-clock time in nanoseconds.
func (c Clock) Wall() runtime.RealTime { return runtime.RealTime(c.Clock.Wall()) }

// AfterFunc calls f once after d, on its own goroutine.
func (c Clock) AfterFunc(d runtime.Duration, f func()) runtime.Timer {
	return c.Clock.AfterFunc(sched.TimeNs(d), f)
}
