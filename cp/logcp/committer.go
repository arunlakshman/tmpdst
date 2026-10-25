package logcp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"manifester/core"
	"manifester/cp"
	"manifester/cp/logsm"
)

// Errors that the HTTP handler turns into 503 (they wrap cp.ErrUnavailable).
var (
	// ErrNotActive: this node is a standby; commands go to the active node.
	ErrNotActive = fmt.Errorf("%w: not the active control-plane node", cp.ErrUnavailable)
	// ErrOverloaded: the commit queue is full; retry later (with backoff).
	ErrOverloaded = fmt.Errorf("%w: commit queue full", cp.ErrUnavailable)
	// ErrBlocked: the node cannot apply the log (see Healthy).
	ErrBlocked = fmt.Errorf("%w: cannot apply the log", cp.ErrUnavailable)
	// ErrStopped: the service is shutting down.
	ErrStopped = fmt.Errorf("%w: stopping", cp.ErrUnavailable)
)

// stopPoll is how often a waiting caller checks that the service still runs.
const stopPoll = 20 * time.Millisecond

// pending is one command waiting for its slot and its answer.
type pending struct {
	ctx  context.Context
	cmd  logsm.Entry
	raw  []byte
	enq  int64
	fin  chan struct{}
	once sync.Once
	res  core.CPDone
	err  error
}

func (p *pending) finish(res core.CPDone, err error) {
	p.once.Do(func() { p.res, p.err = res, err; close(p.fin) })
}

// queue holds the commands waiting for a slot, in arrival order.
type queue struct {
	mu     sync.Mutex
	items  []*pending
	bytes  int
	wake   chan struct{}
	nextAt int64 // earliest time the next slot append may start (slot-rate budget)
}

func newQueue() *queue { return &queue{wake: make(chan struct{}, 1)} }

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// push adds p unless the queue is over its bounds.
func (q *queue) push(p *pending, maxN, maxBytes int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= maxN || q.bytes+len(p.raw) > maxBytes {
		return false
	}
	q.items = append(q.items, p)
	q.bytes += len(p.raw)
	q.signal()
	return true
}

func (q *queue) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// take removes the commands of the next slot: those whose caller is still
// waiting, up to maxCmds and the entry size bound.
func (q *queue) take(maxCmds int) (out []*pending, size int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	i := 0
	for ; i < len(q.items) && len(out) < maxCmds; i++ {
		p := q.items[i]
		q.bytes -= len(p.raw)
		if p.ctx.Err() != nil {
			continue // the caller gave up
		}
		if size+len(p.raw)+8 > MaxEntryBytes-itemOverhead-512 && len(out) > 0 {
			q.bytes += len(p.raw)
			break
		}
		size += len(p.raw) + 8
		out = append(out, p)
	}
	q.items = append(q.items[:0:0], q.items[i:]...)
	if len(q.items) > 0 {
		q.signal() // another flusher can take the rest
	}
	return out, size
}

// reserve claims the next slot-rate token and returns how long to wait for it.
func (q *queue) reserve(now, interval int64) time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	at := max(now, q.nextAt)
	q.nextAt = at + interval
	return time.Duration(at - now)
}

// submit queues cmd and waits for the answer derived from the applied slot.
func (s *Service) submit(ctx context.Context, cmd logsm.Entry) (core.CPDone, error) {
	if err := s.admit(); err != nil {
		return core.CPDone{}, err
	}
	raw := logsm.Encode(cmd)
	if len(raw)+8+itemOverhead+512 > MaxEntryBytes {
		s.m.reject("too_large")
		return core.CPDone{Err: core.ErrTooLarge}, nil
	}
	p := &pending{ctx: ctx, cmd: cmd, raw: raw, enq: s.env.Now(), fin: make(chan struct{})}
	if !s.q.push(p, s.cfg.QueueMax, s.cfg.QueueBytesMax) {
		s.m.reject("overloaded")
		return core.CPDone{}, ErrOverloaded
	}
	s.m.queue(s.q.depth())
	for {
		select {
		case <-p.fin:
			return p.res, p.err
		default:
		}
		if s.stopped() { // the loops are gone: nobody will answer
			return core.CPDone{}, ErrStopped
		}
		if err := s.env.Wait(ctx, p.fin, stopPoll); err != nil {
			return core.CPDone{}, err
		}
	}
}

// admit refuses work this node cannot do now.
func (s *Service) admit() error {
	switch {
	case s.stopped():
		return ErrStopped
	case s.Blocked() != nil:
		s.m.reject("blocked")
		return ErrBlocked
	case !s.isActive():
		s.m.reject("not_active")
		return ErrNotActive
	}
	return nil
}

// flusher is one of K appenders: it takes the commands queued while it was
// busy as the next slot's entry, so the group grows with the load.
func (s *Service) flusher(ctx context.Context) {
	for ctx.Err() == nil {
		ps := s.nextGroup(ctx)
		if len(ps) == 0 {
			continue
		}
		s.appendGroup(ctx, s.newEntry(ps), ps)
	}
}

// nextGroup waits for work and for a slot-rate token, then takes the group.
func (s *Service) nextGroup(ctx context.Context) []*pending {
	if s.q.depth() == 0 {
		if s.env.Wait(ctx, s.q.wake, s.cfg.Heartbeat) != nil {
			return nil
		}
		if s.q.depth() == 0 {
			return nil
		}
	}
	interval := int64(float64(time.Second) / s.cfg.MaxSlotRate)
	if d := s.q.reserve(s.env.Now(), interval); d > 0 {
		if s.env.sleep(ctx, d) != nil {
			return nil
		}
	}
	ps, _ := s.q.take(s.cfg.MaxCmdsPerSlot)
	s.m.queue(s.q.depth())
	now := s.env.Now()
	for _, p := range ps {
		s.m.queued(time.Duration(now - p.enq))
	}
	return ps
}

// newEntry builds the entry for ps (none: a no-op).
func (s *Service) newEntry(ps []*pending) Entry {
	e := Entry{Writer: s.cfg.Name, Rank: s.cfg.Rank, ID: s.rep.newID(), TS: s.env.Now() / 1e6}
	for _, p := range ps {
		e.Cmds = append(e.Cmds, p.cmd)
		e.raw = append(e.raw, p.raw)
	}
	return e
}

// appendGroup appends e at the first slot it wins. A slot lost to another
// writer (or a hole filler) is not an error: no offset was computed here, so
// the same entry just goes to a later slot. Its answers arrive when the
// replica applies the slot (replica.deliverLocked).
func (s *Service) appendGroup(ctx context.Context, e Entry, ps []*pending) {
	s.rep.register(e.ID, ps)
	t0 := time.Now()
	size := len(MarshalEntry(e, Version))
	for {
		n := s.rep.alloc()
		cur, won, err := s.writeSlot(ctx, n, e)
		if err != nil {
			s.rep.unregister(e.ID)
			s.rep.blockIfTrimmed(err)
			failAll(ps, err)
			return
		}
		s.rep.observe(n, cur)
		if won || cur.ID == e.ID {
			s.m.attempt("won", time.Since(t0), len(e.Cmds), size)
			return
		}
		s.m.attempt("lost", time.Since(t0), 0, 0)
		s.rep.pull(ctx)
	}
}

func failAll(ps []*pending, err error) {
	if errors.Is(err, ErrTrimmed) {
		err = ErrBlocked
	}
	if errors.Is(err, ErrEntryTooLarge) {
		for _, p := range ps {
			p.finish(core.CPDone{Err: core.ErrTooLarge}, nil)
		}
		return
	}
	for _, p := range ps {
		p.finish(core.CPDone{}, fmt.Errorf("%w: %w", ErrStopped, err))
	}
}

// writeSlot appends e at slot n and retries an unknown outcome at the same
// slot: the append is conditional, so it either lands, or fails and shows
// what is there, which is ours iff the ID matches.
func (s *Service) writeSlot(ctx context.Context, n int64, e Entry) (cur Entry, won bool, err error) {
	backoff := time.Millisecond
	for {
		cur, won, err = s.log.Append(ctx, n, e)
		if err == nil || errors.Is(err, ErrEntryTooLarge) || errors.Is(err, ErrTrimmed) {
			return cur, won, err
		}
		s.m.attempt("error", 0, 0, 0)
		if err := s.env.sleep(ctx, backoff); err != nil {
			return Entry{}, false, err
		}
		backoff = min(backoff*2, 200*time.Millisecond)
	}
}
