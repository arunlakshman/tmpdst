// Package syncclock is a goroutine-safe virtual clock for tests of concurrent
// code. It is not in the determinism gate; package clock is.
package syncclock

import (
	"sync"

	"github.com/manifesterio/mds/dst/clock"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
)

// Sync is a goroutine-safe virtual clock for code under test that starts timers
// from several goroutines (a run loop, a coordinator). Time moves only when a
// caller calls Advance. It owns a private scheduler, so it cannot share an
// event heap with a simulator run.
//
// Do not use Sync inside a deterministic simulator run: its callers race, so
// the order in which timers are queued is not reproducible. Inside a run use
// clock.Clock over the run's own sched.Scheduler on one goroutine.
type Sync struct {
	mu      sync.Mutex
	s       *sched.Scheduler
	inner   *clock.Clock
	pending int
}

// SyncTimer is a one-shot timer started by Sync.AfterFunc.
type SyncTimer struct {
	c     *Sync
	inner *clock.Timer
}

// NewSync returns a Sync whose Mono starts at monoStart and whose Wall reads
// wall at that moment. Use a nonzero monoStart if zero means "not set" in the
// code under test.
func NewSync(wall, monoStart sched.TimeNs) *Sync {
	s := sched.New(rng.New(1))
	s.Jump(monoStart)
	return &Sync{s: s, inner: clock.New(s, wall-monoStart)}
}

// Mono is the virtual monotonic time.
func (c *Sync) Mono() sched.TimeNs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Mono()
}

// Wall is the virtual UTC time.
func (c *Sync) Wall() sched.TimeNs {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.Wall()
}

// AfterFunc schedules f for d after now. It runs inside a later Advance, on the
// goroutine that calls Advance, without the lock held.
func (c *Sync) AfterFunc(d sched.TimeNs, f func()) *SyncTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending++
	tm := &SyncTimer{c: c}
	tm.inner = c.inner.AfterFunc(d, func() {
		c.mu.Lock()
		c.pending--
		c.mu.Unlock()
		f()
	})
	return tm
}

// Stop cancels the timer. It reports whether it stopped it before it fired.
func (t *SyncTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if !t.inner.Stop() {
		return false
	}
	t.c.pending--
	return true
}

// Advance moves time forward by d and fires every timer that is due, in time
// order. A timer may start new timers; they fire in the same call if they are
// due. Callbacks run unlocked, so they may call AfterFunc, Stop and Mono.
func (c *Sync) Advance(d sched.TimeNs) {
	c.mu.Lock()
	target := c.s.Mono() + max(d, 0)
	for {
		at, ok := c.s.Peek()
		if !ok || at > target {
			c.s.Jump(target)
			c.mu.Unlock()
			return
		}
		run, ok := c.s.Take()
		if !ok {
			c.s.Jump(target)
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		run()
		c.mu.Lock()
	}
}

// Pending counts the timers that were not stopped and did not fire yet.
func (c *Sync) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending
}
