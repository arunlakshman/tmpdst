// Package fakeclock is the test implementation of runtime.Clock. It only
// converts types: the timers, the lock and Advance are dst/syncclock. Time
// moves only when the caller calls Advance. Do not use it inside the simulator
// (internal/verify/simulator), which has its own single-goroutine dst/clock.
package fakeclock

import (
	"time"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/syncclock"
)

// Clock is a runtime.Clock that moves only on Advance.
type Clock struct{ *syncclock.Sync }

var _ runtime.Clock = Clock{}

// New returns a Clock whose Wall time starts at wall. Mono starts at one
// second, so that zero can mean "not set" in the code under test.
func New(wall time.Time) Clock {
	return Clock{syncclock.NewSync(sched.TimeNs(wall.UnixNano()), sched.TimeNs(time.Second))}
}

// Mono returns the node-local time.
func (c Clock) Mono() runtime.NodeTime { return runtime.NodeTime(c.Sync.Mono()) }

// Wall returns the UTC time.
func (c Clock) Wall() runtime.RealTime { return runtime.RealTime(c.Sync.Wall()) }

// AfterFunc schedules f for d after now. It runs inside a later Advance.
func (c Clock) AfterFunc(d runtime.Duration, f func()) runtime.Timer {
	return c.Sync.AfterFunc(sched.TimeNs(d), f)
}

// Advance moves time forward by d and fires every timer that is due.
func (c Clock) Advance(d time.Duration) { c.Sync.Advance(sched.TimeNs(d)) }
