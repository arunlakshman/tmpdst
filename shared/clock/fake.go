package clock

import (
	"time"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/syncclock"
)

// Fake is a machine.Clock that moves only on Advance.
type Fake struct{ *syncclock.Sync }

var _ machine.Clock = Fake{}

// NewFake returns a Fake clock whose Wall time starts at wall. Mono starts at one
// second, so that zero can mean "not set" in the code under test.
func NewFake(wall time.Time) Fake {
	return Fake{syncclock.NewSync(sched.TimeNs(wall.UnixNano()), sched.TimeNs(time.Second))}
}

// Mono returns the node-local time.
func (c Fake) Mono() machine.NodeTime { return machine.NodeTime(c.Sync.Mono()) }

// Wall returns the UTC time.
func (c Fake) Wall() machine.RealTime { return machine.RealTime(c.Sync.Wall()) }

// AfterFunc schedules f for d after now. It runs inside a later Advance.
func (c Fake) AfterFunc(d machine.Duration, f func()) machine.Timer {
	return c.Sync.AfterFunc(sched.TimeNs(d), f)
}

// Advance moves time forward by d and fires every timer that is due.
func (c Fake) Advance(d time.Duration) { c.Sync.Advance(sched.TimeNs(d)) }
