// Package sched is the virtual monotonic clock and the (at, tie, seq) event
// heap of one run.
package sched

import "github.com/manifesterio/mds/dst/rng"

// TimeNs is virtual monotonic nanoseconds since run start.
type TimeNs int64

type event struct {
	at    TimeNs
	tie   uint64
	seq   uint64
	label string
	run   func()
}

type eventHeap []*event

func (h eventHeap) less(i, j int) bool {
	a, b := h[i], h[j]
	if a.at != b.at {
		return a.at < b.at
	}
	if a.tie != b.tie {
		return a.tie < b.tie
	}
	return a.seq < b.seq
}

func (h *eventHeap) push(e *event) {
	*h = append(*h, e)
	q := *h
	for i := len(q) - 1; i > 0; {
		parent := (i - 1) / 2
		if !q.less(i, parent) {
			break
		}
		q[i], q[parent] = q[parent], q[i]
		i = parent
	}
}

func (h *eventHeap) pop() *event {
	q := *h
	top := q[0]
	last := len(q) - 1
	q[0], q[last] = q[last], nil
	q = q[:last]
	*h = q
	for i := 0; ; {
		small, l, r := i, 2*i+1, 2*i+2
		if l < len(q) && q.less(l, small) {
			small = l
		}
		if r < len(q) && q.less(r, small) {
			small = r
		}
		if small == i {
			break
		}
		q[i], q[small] = q[small], q[i]
		i = small
	}
	return top
}

// Scheduler owns the virtual clock and the event queue.
// Invariant: Mono moves inside Next/Take when an event is taken, or via Jump.
// Invariant: same-time events order by seeded tie, then by enqueue seq (total order).
// Not safe for concurrent use.
type Scheduler struct {
	rand  *rng.Rand
	now   TimeNs
	seq   uint64
	steps int
	queue eventHeap
}

// New returns a scheduler that draws ties from r.
func New(r *rng.Rand) *Scheduler { return &Scheduler{rand: r} }

// Mono is the virtual monotonic time.
func (s *Scheduler) Mono() TimeNs { return s.now }

// Steps is the number of events taken so far.
func (s *Scheduler) Steps() int { return s.steps }

// Len is the number of queued events.
func (s *Scheduler) Len() int { return len(s.queue) }

// Peek reports the soonest event time. ok is false when the queue is empty.
func (s *Scheduler) Peek() (TimeNs, bool) {
	if len(s.queue) == 0 {
		return 0, false
	}
	return s.queue[0].at, true
}

// Jump sets Mono to t when t is greater than Mono. Used by clock.Advance to
// move time across empty spans without taking an event.
func (s *Scheduler) Jump(t TimeNs) {
	if t > s.now {
		s.now = t
	}
}

// After queues run at Mono+delay (delay<0 counts as 0). label is for traces only.
// Same-time events order by a seeded tie, then by enqueue seq.
func (s *Scheduler) After(delay TimeNs, label string, run func()) {
	s.after(delay, s.rand.Uint64(), label, run)
}

// AfterFIFO queues like After, but same-time events keep enqueue order (tie=0).
// Virtual clocks use this so Advance matches a simple timer list.
func (s *Scheduler) AfterFIFO(delay TimeNs, label string, run func()) {
	s.after(delay, 0, label, run)
}

func (s *Scheduler) after(delay TimeNs, tie uint64, label string, run func()) {
	if delay < 0 {
		delay = 0
	}
	s.seq++
	s.queue.push(&event{
		at:    s.now + delay,
		tie:   tie,
		seq:   s.seq,
		label: label,
		run:   run,
	})
}

// Next pops the soonest event, sets Mono to max(Mono, at), runs it, returns false if empty.
func (s *Scheduler) Next() bool {
	run, ok := s.Take()
	if !ok {
		return false
	}
	run()
	return true
}

// Take pops the soonest event, sets Mono to max(Mono, at), and returns its
// callback without running it. ok is false when the queue is empty. Callers
// that hold a lock around the heap use Take so the callback can run unlocked.
func (s *Scheduler) Take() (func(), bool) {
	if len(s.queue) == 0 {
		return nil, false
	}
	e := s.queue.pop()
	s.now = max(s.now, e.at)
	s.steps++
	return e.run, true
}
