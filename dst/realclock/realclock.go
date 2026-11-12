// Package realclock is the process clock for production code that takes a
// clock the same way tests take clock.Sync. It reads real time, so it must
// never run inside a deterministic simulator run.
package realclock

import (
	"time"

	"github.com/manifesterio/mds/dst/sched"
)

// Clock is the process clock: Mono counts from the clock's creation using
// Go's monotonic reading, Wall is UTC.
type Clock struct{ start time.Time }

// New returns a Clock anchored at the current time.
func New() Clock { return Clock{start: time.Now()} }

// Mono is the monotonic time elapsed since the clock was created.
func (c Clock) Mono() sched.TimeNs { return sched.TimeNs(time.Since(c.start)) }

// Wall is the current UTC time as Unix nanoseconds.
func (Clock) Wall() sched.TimeNs { return sched.TimeNs(time.Now().UnixNano()) }

// Timer is a pending AfterFunc call.
type Timer struct{ t *time.Timer }

// Stop cancels the call if it has not started. It reports whether it did.
func (t Timer) Stop() bool { return t.t.Stop() }

// AfterFunc calls f once, on its own goroutine, after d.
func (Clock) AfterFunc(d sched.TimeNs, f func()) Timer {
	return Timer{t: time.AfterFunc(time.Duration(d), f)}
}
