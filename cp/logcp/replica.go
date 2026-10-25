package logcp

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// replica is the node's position in the log: the entries read but not yet
// applied, the slots allocated to local appends, the callers waiting for an
// answer and. The state machine itself is the applier; all
// of the replica's state is a function of the log prefix it has applied
// (plus the clock readings that only feed the advisory role logic).
type replica struct {
	s *Service

	applyMu sync.Mutex // one apply batch at a time; also held to start a snapshot

	mu      sync.Mutex
	applied int64           // every slot below is applied
	high    int64           // 1 + highest slot seen or allocated: the next to allocate
	tail    int64           // 1 + highest slot known to exist
	buf     map[int64]Entry // read but not yet applicable (above a hole)
	holeAt  int64           // when the slot at applied was first found empty, 0 = not waiting
	waiters map[string][]*pending
	adv     chan struct{} // closed (and replaced) whenever applied moves
	kick    chan struct{} // wakes the tailing loop
	pulling bool          // a tail read is in flight
	blocked error         // set when the node cannot go on (an unreadable slot, a failed store); permanent
	stuck   bool          // apply itself failed: nothing more is applied
	seq     uint64

	lastPullOK  int64
	lastOtherAt int64 // when a slot of another writer was last applied
	lastOwnAt   int64 // when a slot of this node was last applied
	promoSlot   int64 // slots from here on are written after this node became active
	lowerSeen   bool  // a writer of a lower rank wrote at or after promoSlot
}

func newReplica(s *Service) *replica {
	return &replica{s: s, buf: map[int64]Entry{}, waiters: map[string][]*pending{},
		adv: make(chan struct{}), kick: make(chan struct{}, 1)}
}

// startAt positions an empty replica after a restored checkpoint.
func (r *replica) startAt(slot int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied, r.high, r.tail = slot, slot, slot
}

func (r *replica) appliedNow() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}

// position returns where the replica is and a channel that closes when that changes.
func (r *replica) position() (applied int64, adv <-chan struct{}, blocked error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied, r.adv, r.blocked
}

func (r *replica) newID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	return fmt.Sprintf("%s/%s/%d", r.s.cfg.Name, r.s.cfg.Incarnation, r.seq)
}

// alloc reserves the next slot for a local append.
func (r *replica) alloc() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := max(r.high, r.applied)
	r.high = n + 1
	return n
}

func (r *replica) register(id string, ps []*pending) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waiters[id] = ps
}

func (r *replica) unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.waiters, id)
}

// observe records that slot n holds e and applies whatever became contiguous.
func (r *replica) observe(n int64, e Entry) {
	r.mu.Lock()
	r.addLocked(n, e)
	r.mu.Unlock()
	r.drain()
}

func (r *replica) addLocked(n int64, e Entry) {
	r.high = max(r.high, n+1)
	r.tail = max(r.tail, n+1)
	if n >= r.applied {
		r.buf[n] = e
	}
}

func (r *replica) kickPull() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// pull reads the log from the first unapplied slot and applies what it can.
// Concurrent callers share one read.
func (r *replica) pull(ctx context.Context) {
	r.mu.Lock()
	if r.pulling || r.blocked != nil {
		r.mu.Unlock()
		return
	}
	r.pulling = true
	from := r.applied
	r.mu.Unlock()
	slots, err := r.s.log.Read(ctx, from, r.s.cfg.ReadMax)
	r.mu.Lock()
	r.pulling = false
	for _, sl := range slots {
		r.addLocked(sl.N, sl.E)
	}
	r.noteReadLocked(err)
	r.mu.Unlock()
	r.s.m.pull(err == nil)
	r.drain()
}

// noteReadLocked records the outcome of a tail read. A slot this build cannot
// read stops the replica for good: skipping it would fork the state.
func (r *replica) noteReadLocked(err error) {
	var unsup *UnsupportedError
	switch {
	case err == nil:
		r.lastPullOK = r.s.env.Now()
	case errors.As(err, &unsup) || errors.Is(err, errCorrupt) || errors.Is(err, ErrTrimmed):
		r.blockLocked(err)
	}
}

func (r *replica) blockLocked(err error) {
	if r.blocked == nil {
		r.blocked = err
		r.s.m.block(true)
		r.s.cfg.Logger.Error("log: cannot apply, stopped", "event", "log_blocked", "applied", r.applied, "err", err.Error())
		close(r.adv)
		r.adv = make(chan struct{})
	}
}

// holeTick fills the slot at applied with a no-op once it has been empty for
// HoleAfter while later slots wait behind it.
func (r *replica) holeTick(ctx context.Context) {
	r.mu.Lock()
	if len(r.buf) == 0 {
		r.holeAt = 0
		r.mu.Unlock()
		return
	}
	now := r.s.env.Now()
	if r.holeAt == 0 {
		r.holeAt = now + 1 // +1: a clock at 0 still means "set"
	}
	due := now+1-r.holeAt >= int64(r.s.cfg.HoleAfter)
	slot := r.applied
	r.mu.Unlock()
	if due {
		r.fill(ctx, slot)
	}
}

// fill writes a no-op into the empty slot. Whoever wins the slot, the entry
// that results is observed: the no-op, or the late original command.
func (r *replica) fill(ctx context.Context, slot int64) {
	if r.s.trimmedPast(ctx, slot) {
		return
	}
	noop := r.s.newEntry(nil)
	cur, _, err := r.s.log.Append(ctx, slot, noop)
	if err != nil {
		r.blockIfTrimmed(err)
		return
	}
	if cur.ID == noop.ID {
		r.s.m.holeFill()
	}
	r.observe(slot, cur)
}

func (r *replica) blockIfTrimmed(err error) {
	if errors.Is(err, ErrTrimmed) {
		r.mu.Lock()
		r.blockLocked(err)
		r.mu.Unlock()
	}
}

// trimmedPast decides whether the empty slot may be filled. Below the newest
// checkpoint a live hole cannot exist (the node that wrote the checkpoint
// applied that far), so an empty slot there is either one this node has not
// read yet or one removed by the table's TTL; a no-op written into the latter
// would replace a command the other nodes applied. So below the checkpoint the
// slot is read instead: if it exists it is applied, if it is gone the node
// stops (trimming leaves tombstones, which stop it the same way). It returns
// true when it has dealt with the slot and the caller must not fill it.
func (s *Service) trimmedPast(ctx context.Context, slot int64) bool {
	if !s.canCheckpoint() {
		return false
	}
	it, err := s.getPointer(ctx, ckptPK)
	if err != nil {
		return true // cannot tell; try again at the next tick
	}
	if it == nil || numAttr(it, "ck_slot") <= slot {
		return false
	}
	got, err := s.log.Read(ctx, slot, 1)
	if err == nil && len(got) > 0 && got[0].N == slot {
		s.rep.observe(slot, got[0].E)
		return true
	}
	s.rep.mu.Lock()
	defer s.rep.mu.Unlock()
	if err != nil {
		s.rep.noteReadLocked(err)
	} else {
		s.rep.blockLocked(ErrTrimmed)
	}
	return true
}
