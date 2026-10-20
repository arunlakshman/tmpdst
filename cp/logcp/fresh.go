package logcp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"manifester/cp"
)

// A fresh read must see every commit acknowledged before it began, through
// any node. It therefore needs a tail read that STARTED after the read did.
// Reads share tail checks: while a batch is open (waiting out TailBatch since
// the previous check began) every arriving read joins it; when it starts, it
// closes to new joiners and later reads open the next. A check costs about
// 2 ms (docs/x1-x3-x4-results.md), so reads pay at most TailBatch plus one
// check, and the checks are at most 1/TailBatch per second per node.
type fresher struct {
	mu        sync.Mutex
	next      *freshBatch // the open batch, not yet started
	lastStart int64
}

type freshBatch struct {
	done chan struct{}
	tail int64
	err  error
}

// freshTries bounds the CheckTail retries of one batch.
const freshTries = 4

// fresh returns once this node has applied every slot that existed when the
// call began.
func (s *Service) fresh(ctx context.Context) error {
	t0 := time.Now()
	if err := s.Blocked(); err != nil {
		s.m.reject("blocked")
		return ErrBlocked
	}
	b := s.fr.join(s)
	for {
		select {
		case <-b.done:
			if b.err != nil {
				return b.err
			}
			err := s.waitApplied(ctx, b.tail)
			s.m.fresh(time.Since(t0))
			return err
		default:
		}
		if err := s.env.Wait(ctx, b.done, 0); err != nil {
			return err
		}
	}
}

func (f *fresher) join(s *Service) *freshBatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.next == nil {
		b := &freshBatch{done: make(chan struct{})}
		f.next = b
		s.env.Go(func() { s.runBatch(b) })
	}
	return f.next
}

// runBatch waits out the batch window, closes the batch to new reads, reads
// the tail and publishes it.
func (s *Service) runBatch(b *freshBatch) {
	ctx := s.ctx
	f := &s.fr
	f.mu.Lock()
	wait := time.Duration(f.lastStart + int64(s.cfg.TailBatch) - s.env.Now())
	f.mu.Unlock()
	if wait > 0 {
		_ = s.env.sleep(ctx, wait)
	}
	f.mu.Lock()
	f.next, f.lastStart = nil, s.env.Now()
	f.mu.Unlock()
	b.tail, b.err = s.checkTail(ctx)
	close(b.done)
}

func (s *Service) checkTail(ctx context.Context) (int64, error) {
	var err error
	for try := 0; try < freshTries; try++ {
		var tail int64
		if tail, err = s.log.CheckTail(ctx); err == nil {
			s.m.tailCheck(true)
			return tail, nil
		}
		s.m.tailCheck(false)
		if s.env.sleep(ctx, time.Duration(try+1)*2*time.Millisecond) != nil {
			break
		}
	}
	return 0, fmt.Errorf("%w: tail check: %v", cp.ErrUnavailable, err)
}

// waitApplied blocks until slots below tail are applied, nudging the tailing
// loop to read now.
func (s *Service) waitApplied(ctx context.Context, tail int64) error {
	for {
		applied, adv, blocked := s.rep.position()
		switch {
		case blocked != nil:
			return ErrBlocked
		case applied >= tail:
			return nil
		case s.stopped():
			return ErrStopped
		}
		s.rep.kickPull()
		if err := s.env.Wait(ctx, adv, s.cfg.TailPoll*4); err != nil {
			return err
		}
	}
}
