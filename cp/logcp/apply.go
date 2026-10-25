package logcp

import (
	"errors"
	"time"

	"manifester/core"
	"manifester/cp/applier"
	"manifester/cp/logsm"
)

// maxBatchSlots bounds the slots applied in one store transaction group.
const maxBatchSlots = 256

// drain applies every contiguous buffered slot. Batches never straddle a trim
// boundary (see applyBatch), so the state after any slot is a function of the
// log prefix alone, however the slots arrived.
func (r *replica) drain() {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	for {
		b := r.takeBatch()
		if len(b) == 0 {
			return
		}
		if err := r.applyBatch(b); err != nil {
			r.mu.Lock()
			r.stuck = true
			r.blockLocked(err)
			r.mu.Unlock()
			return
		}
	}
}

// takeBatch removes the next run of contiguous slots from buf. A run starts at
// applied and ends before the next multiple of TrimStride, so a boundary is
// always the first slot of a batch.
func (r *replica) takeBatch() []Slot {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stuck {
		return nil
	}
	var b []Slot
	for n := r.applied; len(b) < maxBatchSlots; n++ {
		if len(b) > 0 && n%r.s.cfg.TrimStride == 0 {
			break
		}
		e, ok := r.buf[n]
		if !ok {
			break
		}
		b = append(b, Slot{N: n, E: e})
		delete(r.buf, n)
	}
	return b
}

// applyBatch applies the commands of the slots in b, in order, to the applier
// (consecutive commits share one store transaction) and then completes the
// slots. At a slot that is a multiple of TrimStride the idempotency window is
// first cut to WindowSlots: the rule depends only on the slot number, so every
// replica forgets the same keys at the same point of the log.
func (r *replica) applyBatch(b []Slot) error {
	t0 := time.Now()
	if start := b[0].N; start%r.s.cfg.TrimStride == 0 && start >= r.s.cfg.WindowSlots {
		if _, err := r.s.app.Trim(start - r.s.cfg.WindowSlots); err != nil {
			return err
		}
	}
	res := make([][]core.CPDone, len(b))
	var run runBuf
	for i, sl := range b {
		res[i] = make([]core.CPDone, len(sl.E.Cmds))
		for j, c := range sl.E.Cmds {
			if err := r.applyCmd(&run, res, i, j, sl.N, c); err != nil {
				return err
			}
		}
	}
	if err := run.flush(r.s.app, res); err != nil {
		return err
	}
	r.s.m.applyBatch(time.Since(t0))
	r.complete(b, res)
	return nil
}

// runBuf collects consecutive commits so they are applied in one transaction.
type runBuf struct {
	cmds  []applier.Command
	where [][2]int
}

func (b *runBuf) flush(a *applier.Applier, res [][]core.CPDone) error {
	if len(b.cmds) == 0 {
		return nil
	}
	out, err := a.CommitBatch(b.cmds)
	if err != nil {
		return err
	}
	for k, d := range out {
		res[b.where[k][0]][b.where[k][1]] = d
	}
	b.cmds, b.where = b.cmds[:0], b.where[:0]
	return nil
}

func (r *replica) applyCmd(run *runBuf, res [][]core.CPDone, i, j int, slot int64, c logsm.Entry) error {
	switch c.Kind {
	case logsm.KindCommit:
		run.cmds = append(run.cmds, applier.Command{Slot: slot, Op: c.Commit})
		run.where = append(run.where, [2]int{i, j})
	case logsm.KindCreateTopic:
		if err := run.flush(r.s.app, res); err != nil {
			return err
		}
		res[i][j] = core.CPDone{Err: r.s.app.CreateTopic(c.Topic, c.Parts)}
	default:
		return errors.New("logcp: command of unknown kind in a slot")
	}
	return nil
}

// complete publishes an applied batch: answers to the waiting callers, the
// role observations and the new position.
func (r *replica) complete(b []Slot, res [][]core.CPDone) {
	now := r.s.env.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, sl := range b {
		r.deliverLocked(sl.E.ID, res[i])
		r.observeWriterLocked(sl, now)
	}
	r.applied = b[len(b)-1].N + 1
	r.holeAt = 0
	for n := range r.buf {
		if n < r.applied {
			delete(r.buf, n)
		}
	}
	r.s.m.progress(r.applied, max(r.tail, r.high)-r.applied, !r.s.isActive())
	close(r.adv)
	r.adv = make(chan struct{})
}

// deliverLocked answers the callers whose entry this is.
func (r *replica) deliverLocked(id string, res []core.CPDone) {
	ps, ok := r.waiters[id]
	if !ok {
		return
	}
	delete(r.waiters, id)
	for k, p := range ps {
		p.finish(res[k], nil)
	}
}

func (r *replica) observeWriterLocked(sl Slot, now int64) {
	if sl.E.Writer == r.s.cfg.Name {
		r.lastOwnAt = now
		return
	}
	r.lastOtherAt = now
	if sl.N >= r.promoSlot && sl.E.Rank < r.s.cfg.Rank {
		r.lowerSeen = true
	}
}
