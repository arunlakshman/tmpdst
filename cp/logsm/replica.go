package logsm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"manifester/core"
	"manifester/veritas"
)

// Machine is the state machine a Replica applies the log to. The default is
// veritas.ControlPlane; the production applier (cp/applier) and the lockstep
// tests plug in here. Exec gets the log slot as the Req of a CommitSegment.
type Machine interface {
	Exec(a core.IOAction) core.Completion
	CreateTopic(name string, partitions int32) core.ErrCode
	StateHash() uint64
}

// Config configures a Replica. Log is required.
type Config struct {
	Log Log
	// Machine is the state machine; nil means a fresh veritas.ControlPlane.
	// Checkpoint needs the veritas default.
	Machine Machine
	// Writer names this replica in entry IDs; it must differ between
	// replicas sharing a log.
	Writer string
	// Now is the clock for hole timeouts and tail pacing, in nanoseconds. The
	// default is the wall clock; a test passes a logical clock.
	Now func() int64
	// Wait pauses between retries and returns ctx's error once it is done.
	// The default sleeps one millisecond.
	Wait func(ctx context.Context) error
	// HoleAfter is how long the next slot may stay empty, while later slots
	// exist, before this replica writes a no-op into it. Default 250 ms.
	HoleAfter int64
	// TailPoll is the minimum time between two tail reads of this replica
	// (a read forced by a Lookup ignores it). Default 5 ms.
	TailPoll int64
	// ReadMax is the number of slots one tail read asks for. Default 500.
	ReadMax int
}

// Stats are counters, for tests and the model.
type Stats struct {
	Applied   int64 // slots applied (including no-ops)
	Pulls     int64 // tail reads
	ReadErrs  int64
	HoleFills int64 // no-ops this replica wrote
	Lost      int64 // appends lost to another writer (committer)
	Retries   int64 // unknown-outcome appends retried at the same slot
}

// Replica is one control-plane replica: it applies the log in slot order to a
// veritas.ControlPlane. All of its state is a function of the log prefix it
// has applied.
type Replica struct {
	cfg Config

	mu      sync.Mutex
	cp      Machine
	applied core.LogSlot           // every slot below is applied
	high    core.LogSlot           // 1 + highest slot seen or allocated: the next to allocate
	buf     map[core.LogSlot]Entry // read but not yet applicable (above a hole)
	syncing bool                   // a tail read is in flight (the "bus stand")
	lastPul int64
	holeAt  int64 // when the slot at applied was first found empty, 0 = not waiting
	watch   map[core.LogSlot]bool
	done    map[core.LogSlot]core.CPDone // results of watched slots
	seq     uint64
	st      Stats
}

// NewReplica returns a replica at the empty state.
func NewReplica(cfg Config) *Replica {
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().UnixNano() }
	}
	if cfg.Wait == nil {
		cfg.Wait = sleepWait
	}
	cfg.HoleAfter = defaultNs(cfg.HoleAfter, int64(250*time.Millisecond))
	cfg.TailPoll = defaultNs(cfg.TailPoll, int64(5*time.Millisecond))
	if cfg.ReadMax <= 0 {
		cfg.ReadMax = 500
	}
	m := cfg.Machine
	if m == nil {
		m = veritas.NewControlPlane()
	}
	return &Replica{cfg: cfg, cp: m, buf: map[core.LogSlot]Entry{},
		watch: map[core.LogSlot]bool{}, done: map[core.LogSlot]core.CPDone{}, lastPul: -1 << 62}
}

func defaultNs(v, d int64) int64 {
	if v <= 0 {
		return d
	}
	return v
}

func sleepWait(ctx context.Context) error {
	t := time.NewTimer(time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Applied is the number of slots applied: slots 0..Applied()-1.
func (r *Replica) Applied() core.LogSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}

// StateHash is the veritas state hash after the applied prefix.
func (r *Replica) StateHash() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cp.StateHash()
}

// Stats returns a copy of the counters.
func (r *Replica) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.st
}

// newID returns a fresh entry ID.
func (r *Replica) newID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	return fmt.Sprintf("%s/%d", r.cfg.Writer, r.seq)
}

// alloc reserves the next slot for a local append and watches its result.
func (r *Replica) alloc() core.LogSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := max(r.high, r.applied)
	r.high = n + 1
	r.watch[n] = true
	return n
}

// unwatch drops the watch on slot n (the append lost it or gave up).
func (r *Replica) unwatch(n core.LogSlot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.watch, n)
	delete(r.done, n)
}

// observe records that slot n holds e and applies whatever has become
// contiguous. The caller holds mu.
func (r *Replica) observeLocked(n core.LogSlot, e Entry) {
	r.high = max(r.high, n+1)
	if n >= r.applied {
		r.buf[n] = e
	}
	for {
		next, ok := r.buf[r.applied]
		if !ok {
			return
		}
		r.applyLocked(r.applied, next)
		delete(r.buf, r.applied)
		r.applied++
		r.holeAt = 0
	}
}

func (r *Replica) observe(n core.LogSlot, e Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observeLocked(n, e)
}

// applyLocked is the one place the state changes: slot n's command goes
// through the veritas state machine. The result is kept only if a local
// committer is waiting for it.
func (r *Replica) applyLocked(n core.LogSlot, e Entry) {
	var res core.CPDone
	switch e.Kind {
	case KindCommit:
		res = r.cp.Exec(core.CPCall{Req: core.ReqID(n), Op: e.Commit}).(core.CPDone)
	case KindCreateTopic:
		res = core.CPDone{Req: core.ReqID(n), Err: r.cp.CreateTopic(e.Topic, e.Parts)}
	}
	r.st.Applied++
	if r.watch[n] {
		r.done[n] = res
	}
}

// pull reads the log from the first unapplied slot and applies what it can.
// Concurrent callers share one read: if one is in flight, or the last one was
// less than TailPoll ago (and !force), pull does nothing.
// pullPaced reads the tail unless a pull is already in flight or TailPoll has
// not yet elapsed since the last pull.
func (r *Replica) pullPaced(ctx context.Context) {
	r.mu.Lock()
	now := r.cfg.Now()
	if r.syncing || now-r.lastPul < r.cfg.TailPoll {
		r.mu.Unlock()
		return
	}
	r.pullLocked(ctx, now)
}

// pullNow reads the tail immediately unless a pull is already in flight.
func (r *Replica) pullNow(ctx context.Context) {
	r.mu.Lock()
	if r.syncing {
		r.mu.Unlock()
		return
	}
	r.pullLocked(ctx, r.cfg.Now())
}

// pullLocked runs the Read under r.mu held on entry; it releases and reacquires.
func (r *Replica) pullLocked(ctx context.Context, now int64) {
	r.syncing, r.lastPul = true, now
	from := r.applied
	r.mu.Unlock()
	slots, err := r.cfg.Log.Read(ctx, from, r.cfg.ReadMax)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.syncing = false
	r.st.Pulls++
	if err != nil {
		r.st.ReadErrs++
		return
	}
	for _, s := range slots {
		r.observeLocked(s.N, s.E)
	}
}

// holeDue reports whether the slot at applied has been empty for HoleAfter
// (starting the timer on first sight), and which slot it is.
func (r *Replica) holeDue() (slot core.LogSlot, due bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.cfg.Now()
	if r.holeAt == 0 {
		r.holeAt = now + 1 // +1: a clock at 0 still means "set"
	}
	return r.applied, now+1-r.holeAt >= r.cfg.HoleAfter
}

// fill writes a no-op into the empty slot. Whoever wins the slot, the entry
// that results is observed: the no-op, or the late original command.
func (r *Replica) fill(ctx context.Context, slot core.LogSlot) {
	cur, _, err := r.cfg.Log.Append(ctx, slot, Entry{Kind: KindNoop, ID: r.newID()})
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		return
	}
	if cur.Kind == KindNoop {
		r.st.HoleFills++
	}
	r.observeLocked(slot, cur)
}

// SyncTo applies the log through slot target-1: it reads the tail (paced),
// waits for slots still in flight, and fills a hole that stays empty past
// HoleAfter. It returns ctx's error only.
func (r *Replica) SyncTo(ctx context.Context, target core.LogSlot) error {
	return r.syncTo(ctx, target, r.pullPaced)
}

// SyncToNow is SyncTo with an immediate first pull (not paced by TailPoll).
func (r *Replica) SyncToNow(ctx context.Context, target core.LogSlot) error {
	return r.syncTo(ctx, target, r.pullNow)
}

func (r *Replica) syncTo(ctx context.Context, target core.LogSlot, firstPull func(context.Context)) error {
	pull := firstPull
	for {
		if r.Applied() >= target {
			return nil
		}
		pull(ctx)
		pull = r.pullPaced
		if r.Applied() >= target {
			return nil
		}
		if slot, due := r.holeDue(); due {
			r.fill(ctx, slot)
			continue
		}
		if err := r.cfg.Wait(ctx); err != nil {
			return err
		}
	}
}

// Await returns the result of the local append at slot n once n is applied.
func (r *Replica) Await(ctx context.Context, n core.LogSlot) (core.CPDone, error) {
	if err := r.SyncTo(ctx, n+1); err != nil {
		r.unwatch(n)
		return core.CPDone{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	res := r.done[n]
	delete(r.done, n)
	delete(r.watch, n)
	return res, nil
}

// Read serves a read-only op (Lookup, DescribeTopics) fresh: one consistent
// tail read, then apply through it, then answer from memory.
func (r *Replica) Read(ctx context.Context, op core.CPOp) (core.CPDone, error) {
	tail, err := r.cfg.Log.CheckTail(ctx)
	for err != nil {
		if err = r.cfg.Wait(ctx); err != nil {
			return core.CPDone{}, err
		}
		tail, err = r.cfg.Log.CheckTail(ctx)
	}
	if err := r.SyncToNow(ctx, tail); err != nil {
		return core.CPDone{}, err
	}
	return r.Local(op), nil
}

// Local answers a read-only op from the applied prefix, however stale.
func (r *Replica) Local(op core.CPOp) core.CPDone {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cp.Exec(core.CPCall{Op: op}).(core.CPDone)
}

// Tick is one step of tailing: a paced tail read, and a hole fill if slots
// are stuck behind an empty one for too long.
func (r *Replica) Tick(ctx context.Context) {
	r.pullPaced(ctx)
	if r.pending() {
		if slot, due := r.holeDue(); due {
			r.fill(ctx, slot)
		}
	}
}

// Run tails the log until ctx is done, so the replica stays close to the tail
// and learns where the next free slot is.
func (r *Replica) Run(ctx context.Context) {
	for ctx.Err() == nil {
		r.Tick(ctx)
		if r.cfg.Wait(ctx) != nil {
			return
		}
	}
}

// pending reports whether slots beyond a hole are waiting to be applied.
func (r *Replica) pending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		r.holeAt = 0
		return false
	}
	return true
}
