package logcp_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"manifester/cp/logcp"
	"manifester/veritas/veritastest"
)

// The seeded scheduler of X2 (cp/logsm/sim_test.go), extended so the REAL
// service runs under it: every Env.Wait and every log call parks the actor,
// goroutines the service starts (Env.Go) are actors too, and exactly one actor
// runs at a time, chosen by the seeded stream. The same seed gives the same
// interleaving, faults and trace hash. One step is one millisecond of Env time.

type park struct{ ch chan struct{} }

type sched struct {
	rng     *veritastest.Rand
	mu      sync.Mutex
	cv      *sync.Cond
	running int
	parked  []*park
	step    int64
	limit   int64
	trace   uint64
	hung    bool  // the run was abandoned past twice the step limit
	scale   int64 // steps per millisecond of Env time
}

func newSched(seed uint64, limit int64) *sched {
	s := &sched{rng: veritastest.NewRand(seed), limit: limit, trace: 14695981039346656037, scale: 1}
	s.cv = sync.NewCond(&s.mu)
	return s
}

// spawn starts an actor from outside the scheduler and waits until it first
// parks or ends, so the park order is deterministic.
func (s *sched) spawn(f func()) {
	s.mu.Lock()
	s.running++
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			s.running--
			s.cv.Broadcast()
			s.mu.Unlock()
		}()
		f()
	}()
	s.mu.Lock()
	for s.running > 0 {
		s.cv.Wait()
	}
	s.mu.Unlock()
}

// goFrom starts an actor from a running actor: it is parked at once (in
// order) and runs when the scheduler picks it.
func (s *sched) goFrom(f func()) {
	p := &park{ch: make(chan struct{})}
	s.mu.Lock()
	s.parked = append(s.parked, p)
	s.mu.Unlock()
	go func() {
		<-p.ch
		defer func() {
			s.mu.Lock()
			s.running--
			s.cv.Broadcast()
			s.mu.Unlock()
		}()
		f()
	}()
}

// yield parks the calling actor until the scheduler resumes it.
func (s *sched) yield() {
	p := &park{ch: make(chan struct{})}
	s.mu.Lock()
	s.parked = append(s.parked, p)
	s.running--
	s.cv.Broadcast()
	s.mu.Unlock()
	<-p.ch
}

func (s *sched) run() {
	for {
		s.mu.Lock()
		for s.running > 0 {
			s.cv.Wait()
		}
		if len(s.parked) == 0 || s.step > 2*s.limit {
			s.hung = len(s.parked) > 0
			s.mu.Unlock()
			return
		}
		i := s.rng.Intn(len(s.parked))
		p := s.parked[i]
		s.parked = slices.Delete(s.parked, i, i+1)
		s.running++
		s.step++
		s.trace = (s.trace ^ uint64(i+1)) * 1099511628211
		s.mu.Unlock()
		close(p.ch)
	}
}

func (s *sched) chance(percent int) bool { return s.rng.Intn(100) < percent }

// env is the service's view of the scheduler.
func (s *sched) env() logcp.Env {
	return logcp.Env{
		Now: func() int64 { return s.step * int64(time.Millisecond) / s.scale },
		Go:  s.goFrom,
		Wait: func(ctx context.Context, ready <-chan struct{}, d time.Duration) error {
			deadline := s.step + max(1, int64(d)*s.scale/int64(time.Millisecond))
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				if ready != nil {
					select {
					case <-ready:
						return nil
					default:
					}
				}
				if d > 0 && s.step >= deadline {
					return nil
				}
				s.yield()
				if s.step > s.limit {
					return errLimit
				}
			}
		},
	}
}

var errLimit = errors.New("step limit")
