// Package fakeclock is the test and simulation implementation of machine.Clock.
// Time moves only when the caller calls Advance. It is not in the determinism
// gate, because it uses a mutex; the timers run on the caller's goroutine, in
// time order.
package fakeclock

import (
	"sync"
	"time"

	"github.com/manifesterio/mds/dst/machine"
)

// Clock is a machine.Clock that moves only on Advance.
type Clock struct {
	mu     sync.Mutex
	mono   machine.NodeTime
	wall   machine.RealTime
	timers []*timer
}

type timer struct {
	clock   *Clock
	at      machine.NodeTime
	f       func()
	stopped bool
	fired   bool
}

var _ machine.Clock = (*Clock)(nil)

// New returns a Clock whose Wall time starts at wall. Mono starts at one
// second, so that zero can mean "not set" in the code under test.
func New(wall time.Time) *Clock {
	return &Clock{mono: machine.NodeTime(time.Second), wall: machine.RealTime(wall.UnixNano())}
}

// Mono returns the node-local time.
func (c *Clock) Mono() machine.NodeTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// Wall returns the UTC time.
func (c *Clock) Wall() machine.RealTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

// AfterFunc schedules f for d after now. It runs inside a later Advance.
func (c *Clock) AfterFunc(d machine.Duration, f func()) machine.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &timer{clock: c, at: c.mono + machine.NodeTime(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *timer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

// Advance moves time forward by d and fires every timer that is due, in time
// order. A timer may start new timers; they fire in the same call if they are due.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.mono + machine.NodeTime(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var next *timer
		for _, t := range c.timers {
			if t.stopped || t.fired || t.at > target {
				continue
			}
			if next == nil || t.at < next.at {
				next = t
			}
		}
		if next == nil {
			c.wall += machine.RealTime(target - c.mono)
			c.mono = target
			c.mu.Unlock()
			return
		}
		next.fired = true
		if next.at > c.mono {
			c.wall += machine.RealTime(next.at - c.mono)
			c.mono = next.at
		}
		c.mu.Unlock()
		next.f()
	}
}

// Pending counts the timers that were not stopped and did not fire yet.
func (c *Clock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped && !t.fired {
			n++
		}
	}
	return n
}
