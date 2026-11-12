// Package clock wraps a sched.Scheduler with a wall-time offset for
// standalone deterministic tests.
//
// AfterFunc schedules on the same event heap as the scheduler, so timer
// callbacks share sim time with every other After event. Not safe for
// concurrent use; keep one Clock on one goroutine so it can sit in
// deterministic gates later.
package clock

import "github.com/manifesterio/mds/dst/sched"

// Clock is a single-threaded virtual clock.
// Wall is wallStart + Mono.
type Clock struct {
	s         *sched.Scheduler
	wallStart sched.TimeNs
}

// New returns a Clock over s. Wall reports wallStart+Mono.
func New(s *sched.Scheduler, wallStart sched.TimeNs) *Clock {
	return &Clock{s: s, wallStart: wallStart}
}

// Mono is the scheduler's virtual monotonic time.
func (c *Clock) Mono() sched.TimeNs { return c.s.Mono() }

// Wall is wallStart + Mono.
func (c *Clock) Wall() sched.TimeNs { return c.wallStart + c.s.Mono() }

// Timer is a one-shot timer scheduled via AfterFunc.
type Timer struct {
	stopped bool
	fired   bool
}

// Stop cancels the timer. It returns true if the timer was stopped before
// firing. A second Stop, or Stop after fire, returns false. The event may
// still leave the heap; the callback is a no-op when stopped.
func (tm *Timer) Stop() bool {
	if tm.fired || tm.stopped {
		return false
	}
	tm.stopped = true
	return true
}

// AfterFunc schedules f to run after delay on the scheduler heap.
// Delay semantics match sched.Scheduler.After (negative delay is 0).
func (c *Clock) AfterFunc(d sched.TimeNs, f func()) *Timer {
	tm := new(Timer)
	c.s.AfterFIFO(d, "clock", func() {
		if tm.stopped {
			return
		}
		tm.fired = true
		f()
	})
	return tm
}

// Advance moves Mono to now+d and runs every heap event due by that time.
// Later timers stay pending. When the queue is empty or the next event is
// past the target, Mono still jumps to the target via sched.Jump.
func (c *Clock) Advance(d sched.TimeNs) {
	if d < 0 {
		d = 0
	}
	target := c.s.Mono() + d
	for {
		at, ok := c.s.Peek()
		if !ok || at > target {
			c.s.Jump(target)
			return
		}
		if !c.s.Next() {
			c.s.Jump(target)
			return
		}
	}
}
