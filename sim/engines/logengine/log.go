package logengine

import (
	"slices"
	"strconv"

	"manifester/core"
	"manifester/sim"
)

type fate uint8

const (
	fateOK   fate = iota // applied, answer arrives
	fateFail             // not applied
	fateLost             // applied, answer never arrives
)

type outcome uint8

const (
	outOurs outcome = iota // the slot holds this entry
	outLost                // the slot is taken by something else
)

func (e *Engine) lane(ctx sim.EngineCtx, rank int, id uint64) sim.Lane {
	return ctx.Lane("logengine/put/" + strconv.Itoa(rank+1) + "/" + strconv.Itoa(int(id%8)))
}

// canWrite: the replica is up, active and not stalled.
func (r *replica) canWrite(now sim.Time) bool { return r.live(now) && r.active }

// flush starts entries for the queued commands while fewer than K appends are
// in flight, and a heartbeat no-op when an active node is idle.
func (e *Engine) flush(ctx sim.EngineCtx, r *replica) {
	if !r.canWrite(e.now) {
		return
	}
	for len(r.ops) < e.p.K && len(r.q) > 0 {
		n := min(e.p.GroupMax, len(r.q))
		e.startEntry(ctx, r, r.q[:n])
		r.q = r.q[n:]
	}
	if len(r.q) == 0 && len(r.ops) == 0 && e.now-r.lastAppend >= sim.Time(e.p.Heartbeat) {
		e.startEntry(ctx, r, nil)
	}
	ctx.Assert().Sometimes(len(r.ops) >= 2, "logcp-sim: several appends in flight")
}

// startEntry appends qs as one entry at the next slot this writer knows of.
func (e *Engine) startEntry(ctx sim.EngineCtx, r *replica, qs []qcmd) {
	e.nextEnt++
	ent := &entry{id: e.nextEnt}
	if len(r.ops) == 0 && r.applied+1 >= r.next {
		r.proj = nil
	}
	for i, q := range qs {
		c := q.c
		if e.p.Bug == BugWriterOffsets {
			c.bases = r.project(c)
		}
		ent.cmds = append(ent.cmds, c)
		r.waits = append(r.waits, waiter{eid: ent.id, idx: i, req: q.req})
	}
	e.nextOp++
	op := &appendOp{id: e.nextOp, rank: r.rank, inc: r.inc, slot: max(r.next, r.applied+1), ent: ent}
	r.next = op.slot + 1
	r.ops = append(r.ops, op.id)
	r.lastAppend = e.now
	e.ops = append(e.ops, op)
	e.schedLand(ctx, op, 0)
}

// project is the BugWriterOffsets mutant: the writer assumes every command it
// has in flight lands first and in order, and numbers the batches itself.
func (r *replica) project(c cmd) []core.Offset {
	bases := make([]core.Offset, len(c.batches))
	for i, b := range c.batches {
		j := slices.IndexFunc(r.proj, func(p projEnd) bool { return p.tp == b.TP })
		if j < 0 {
			r.proj = append(r.proj, projEnd{tp: b.TP, end: r.st.end(b.TP)})
			j = len(r.proj) - 1
		}
		bases[i] = r.proj[j].end
		r.proj[j].end += core.Offset(b.Records)
	}
	return bases
}

func (e *Engine) schedLand(ctx sim.EngineCtx, op *appendOp, after core.Duration) {
	at := e.now + sim.Time(after) + sim.Time(e.f.ApplyLatency.Sample(ctx.Rand()))
	ctx.Self(at, e.lane(ctx, op.rank, op.id), landEv{op: op.id})
}

func (e *Engine) op(id uint64) *appendOp {
	i := slices.IndexFunc(e.ops, func(o *appendOp) bool { return o.id == id })
	if i < 0 {
		return nil
	}
	return e.ops[i]
}

func (e *Engine) removeOp(op *appendOp) {
	e.ops = slices.DeleteFunc(e.ops, func(o *appendOp) bool { return o.id == op.id })
	if op.filler {
		return
	}
	r := e.reps[op.rank]
	if r.inc == op.inc {
		r.ops = slices.DeleteFunc(r.ops, func(id uint64) bool { return id == op.id })
	}
}

// drawFate draws what happens to one Put attempt.
func (e *Engine) drawFate(ctx sim.EngineCtx) fate {
	r := ctx.Rand()
	failed, lost := r.Chance(e.prob(e.p.PutFail)), r.Chance(e.prob(e.p.PutLost))
	switch {
	case failed:
		return fateFail
	case lost:
		return fateLost
	}
	return fateOK
}

// onLand is a Put arriving at the store.
func (e *Engine) onLand(ctx sim.EngineCtx, id uint64) {
	op := e.op(id)
	if op == nil {
		return
	}
	if op.filler {
		e.landFiller(ctx, op)
		return
	}
	r := e.reps[op.rank]
	if op.attempt > 0 && e.writerGone(ctx, r, op) {
		return
	}
	f := e.drawFate(ctx)
	out := outLost
	if f != fateFail {
		out = e.write(ctx, op)
	}
	e.afterPut(ctx, op, f, out)
}

// writerGone handles a repeated Put whose writer crashed (the op is forgotten)
// or is stalled (the repeat waits for it to wake).
func (e *Engine) writerGone(ctx sim.EngineCtx, r *replica, op *appendOp) bool {
	if r.inc != op.inc {
		e.removeOp(op)
		return true
	}
	if r.live(e.now) {
		return false
	}
	ctx.Self(r.stallUntil, e.lane(ctx, op.rank, op.id), landEv{op: op.id})
	return true
}

// afterPut schedules what the writer does next with the answer of a Put.
func (e *Engine) afterPut(ctx sim.EngineCtx, op *appendOp, f fate, out outcome) {
	if f != fateOK { // no answer: the writer times out and repeats the same slot
		op.attempt++
		ctx.Assert().Reachable("logcp-sim: Put not answered, same slot repeated")
		ctx.Self(e.now+sim.Time(e.p.PutRetry), e.lane(ctx, op.rank, op.id), landEv{op: op.id})
		return
	}
	at := e.now + sim.Time(e.f.RespondLatency.Sample(ctx.Rand()))
	op.attempt++
	ctx.Self(at, e.lane(ctx, op.rank, op.id), answerEv{op: op.id, out: out})
}

// onAnswer is the writer reading the answer of a Put.
func (e *Engine) onAnswer(ctx sim.EngineCtx, id uint64, out outcome) {
	op := e.op(id)
	if op == nil {
		return
	}
	r := e.reps[op.rank]
	switch {
	case r.inc != op.inc:
		e.removeOp(op)
	case !r.live(e.now):
		ctx.Self(r.stallUntil, e.lane(ctx, op.rank, op.id), answerEv{op: id, out: out})
	case out == outOurs:
		e.removeOp(op)
		e.flush(ctx, r)
	default:
		e.reappend(ctx, r, op)
	}
}

// reappend: the slot was taken; the same entry goes to a later slot.
func (e *Engine) reappend(ctx sim.EngineCtx, r *replica, op *appendOp) {
	ctx.Assert().Reachable("logcp-sim: lost slot retried at a later slot")
	op.slot = max(r.next, r.applied+1, op.slot+1)
	r.next = op.slot + 1
	op.attempt = 0
	e.schedLand(ctx, op, 0)
}

// write is the if-absent Put into op.slot.
func (e *Engine) write(ctx sim.EngineCtx, op *appendOp) outcome {
	e.extend(op.slot)
	sl := &e.slots[op.slot]
	switch {
	case sl.kind == slotEmpty:
		ctx.Assert().Always(op.slot >= e.trimmed, "logcp-sim: a Put never wins a slot below the trim floor", core.Detail{Key: "slot", Value: op.slot})
		*sl = slotRec{kind: slotEntry, writer: op.rank, id: op.ent.id, cmds: op.ent.cmds}
		e.landed(ctx, op.slot)
		return outOurs
	case sl.kind == slotEntry && sl.id == op.ent.id:
		return outOurs
	}
	return outLost
}

func (e *Engine) extend(slot int) {
	for slot >= len(e.slots) {
		e.slots = append(e.slots, slotRec{})
	}
}

func (e *Engine) landed(ctx sim.EngineCtx, slot int) {
	e.hi = max(e.hi, slot)
	e.advanceRef(ctx)
}

// landFiller is a hole filler's if-absent no-op Put.
func (e *Engine) landFiller(ctx sim.EngineCtx, op *appendOp) {
	e.removeOp(op)
	if r := e.reps[op.rank]; r.inc == op.inc && r.fillSlot == op.slot {
		r.fillSlot = -1
	}
	e.extend(op.slot)
	if e.slots[op.slot].kind != slotEmpty {
		return
	}
	ctx.Assert().Reachable("logcp-sim: hole filled by a no-op")
	e.slots[op.slot] = slotRec{kind: slotEntry, writer: op.rank, id: op.ent.id}
	e.landed(ctx, op.slot)
}

// startFill writes a no-op into the hole at slot on behalf of r.
func (e *Engine) startFill(ctx sim.EngineCtx, r *replica, slot int) {
	e.nextEnt++
	e.nextOp++
	op := &appendOp{id: e.nextOp, rank: r.rank, inc: r.inc, slot: slot, ent: &entry{id: e.nextEnt}, filler: true}
	r.fillSlot = slot
	e.ops = append(e.ops, op)
	e.schedLand(ctx, op, 0)
}
