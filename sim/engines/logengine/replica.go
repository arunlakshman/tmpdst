package logengine

import (
	"slices"
	"strings"

	"manifester/core"
	"manifester/sim"
)

// applyTo applies slot s of the log to st: every command in order, then the
// window rule at a stride boundary (a function of the slot number only).
func (e *Engine) applyTo(st *state, s int) []result {
	sl := e.slots[s]
	res := make([]result, len(sl.cmds))
	for i, c := range sl.cmds {
		res[i] = st.applyCmd(s, sl.writer, c)
		st.fold(s, c.key, res[i])
	}
	forgot := 0
	if e.p.TrimStride > 0 && s%e.p.TrimStride == 0 {
		forgot = st.forget(s, e.p.WindowSlots)
	}
	st.foldSlot(s, len(sl.cmds), forgot)
	return res
}

// advanceRef replays newly contiguous landed slots on the reference.
func (e *Engine) advanceRef(ctx sim.EngineCtx) {
	for {
		s := e.ref.applied + 1
		if s > e.hi || e.slots[s].kind != slotEntry {
			return
		}
		res := e.applyTo(e.ref.st, s)
		e.ref.applied = s
		e.refDig = append(e.refDig, e.ref.st.digest)
		e.gen++
		e.checkRef(ctx, s, res)
	}
}

// checkRef is the ground truth for what the replicas answer: a replayed key
// returns its original ranges, and replays hit the paths the sim must reach.
func (e *Engine) checkRef(ctx sim.EngineCtx, s int, res []result) {
	a := ctx.Assert()
	for i, r := range res {
		key := e.slots[s].cmds[i].key
		j, seen := slices.BinarySearchFunc(e.shadow, key, func(k shadowKey, key string) int { return strings.Compare(k.key, key) })
		switch {
		case r.err != core.OK:
		case seen:
			a.Always(rangesEqual(e.shadow[j].ranges, r.ranges), "logcp-sim: a key committed twice gets its original ranges", core.Detail{Key: "key", Value: key})
		default:
			e.shadow = slices.Insert(e.shadow, j, shadowKey{key: key, ranges: r.ranges})
		}
		e.reachReplay(ctx, s, r)
	}
	if e.p.TrimStride > 0 && s%e.p.TrimStride == 0 {
		e.shadow = slices.DeleteFunc(e.shadow, func(k shadowKey) bool { _, ok := e.ref.st.keyAt(k.key); return !ok })
	}
}

func rangesEqual(a, b []core.BatchRange) bool { return slices.Equal(a, b) }

func (e *Engine) reachReplay(ctx sim.EngineCtx, s int, r result) {
	if !r.replay {
		return
	}
	a := ctx.Assert()
	a.Reachable("logcp-sim: replayed key answered from the window")
	if r.rec.first < e.trimmed {
		a.Reachable("logcp-sim: replay of a key whose slot was trimmed")
	}
	if r.rec.writer != e.slots[s].writer {
		a.Reachable("logcp-sim: idempotent replay after takeover")
	}
}

// applyReplica applies slot s on r, answers the commands r was waiting for and
// checks r against the reference.
func (e *Engine) applyReplica(ctx sim.EngineCtx, r *replica, s int) {
	res := e.applyTo(r.st, s)
	r.applied = s
	sl := e.slots[s]
	if sl.writer != r.rank {
		r.lastOther = e.now
	}
	if r.active && sl.writer < r.rank {
		r.sawLower = true
	}
	e.answerWaiters(ctx, r, sl, res)
	e.checkDigest(ctx, r)
}

// checkDigest: a replica that applied slot n has exactly the state the log
// determines (all replicas' query digests equal at a quiesced log).
func (e *Engine) checkDigest(ctx sim.EngineCtx, r *replica) {
	if r.applied < 0 {
		return
	}
	ok := r.applied < len(e.refDig) && r.st.digest == e.refDig[r.applied]
	ctx.Assert().Always(ok, "logcp-sim: a replica's state equals the replay of the log at its slot",
		core.Detail{Key: "rank", Value: r.rank}, core.Detail{Key: "slot", Value: r.applied})
}

func (e *Engine) answerWaiters(ctx sim.EngineCtx, r *replica, sl slotRec, res []result) {
	for i, x := range res {
		j := slices.IndexFunc(r.waits, func(w waiter) bool { return w.eid == sl.id && w.idx == i })
		if j < 0 {
			continue
		}
		w := r.waits[j]
		r.waits = slices.Delete(r.waits, j, j+1)
		if rq := e.req(w.req); rq != nil {
			e.finish(ctx, rq, completion(rq.call.Req, x))
		}
	}
}

func completion(req core.ReqID, x result) core.CPDone {
	if x.err != core.OK {
		return core.CPDone{Req: req, Err: x.err}
	}
	return core.CPDone{Req: req, Resp: core.CommitSegmentResp{Assigned: x.ranges}, Outcome: core.Applied}
}

// sync is one tail read plus applying every contiguous landed slot.
func (e *Engine) sync(ctx sim.EngineCtx, r *replica) {
	if ctx.Rand().Chance(e.prob(e.p.ReadFail)) {
		return
	}
	for r.applied < e.hi {
		s := r.applied + 1
		switch e.slots[s].kind {
		case slotEntry:
			e.applyReplica(ctx, r, s)
		case slotTomb:
			e.restore(ctx, r)
			return
		default:
			if !e.hole(ctx, r, s) {
				return
			}
		}
	}
}

// hole handles an empty slot below the tail. It reports whether r may go on.
func (e *Engine) hole(ctx sim.EngineCtx, r *replica, s int) bool {
	ctx.Assert().Always(s >= e.trimmed, "logcp-sim: no empty slot below the trim floor", core.Detail{Key: "slot", Value: s})
	if e.p.Bug == BugHoleSkip {
		r.applied = s
		return true
	}
	if r.holeSlot != s {
		r.holeSlot, r.holeSince = s, e.now
		return false
	}
	if e.now-r.holeSince >= sim.Time(e.p.HoleAfter) && r.fillSlot != s {
		e.startFill(ctx, r, s)
	}
	return false
}

// restore rebuilds r from the latest checkpoint (the log is replayed from there).
func (e *Engine) restore(ctx sim.EngineCtx, r *replica) {
	r.st, r.applied = e.fresh.clone(), -1
	if n := len(e.snaps); n > 0 {
		r.st, r.applied = e.snaps[n-1].st.clone(), e.snaps[n-1].slot
	}
	r.holeSlot, r.fillSlot = -1, -1
	ctx.Assert().Always(r.applied+1 >= e.trimmed, "logcp-sim: a checkpoint is never below the trim floor", core.Detail{Key: "slot", Value: r.applied})
	r.next = max(r.next, r.applied+1)
	ctx.Assert().Reachable("logcp-sim: replica restored from a checkpoint")
}

// checkpoint snapshots the active replica every CkptEvery slots and trims the
// log below the snapshot, leaving tombstones (or, in the mutant, nothing).
func (e *Engine) checkpoint(ctx sim.EngineCtx, r *replica) {
	if !r.active || e.p.CkptEvery <= 0 {
		return
	}
	last := -1
	if n := len(e.snaps); n > 0 {
		last = e.snaps[n-1].slot
	}
	if r.applied-last < e.p.CkptEvery {
		return
	}
	e.snaps = append(e.snaps, snapshot{slot: r.applied, st: r.st.clone()})
	if len(e.snaps) > 2 {
		e.snaps = e.snaps[1:]
	}
	e.trimTo(ctx, r.applied-e.p.KeepSlots)
}

func (e *Engine) trimTo(ctx sim.EngineCtx, floor int) {
	kind := slotTomb
	if e.p.Bug == BugTrimDelete {
		kind = slotEmpty
	}
	for ; e.trimmed < floor && e.trimmed < len(e.slots); e.trimmed++ {
		e.slots[e.trimmed] = slotRec{kind: kind}
	}
	ctx.Assert().Reachable("logcp-sim: log trimmed below a checkpoint")
}

// roles: the standby becomes active when it saw no slot of another writer for
// TakeoverAfter; an active standby steps down when a lower rank wrote after it.
func (e *Engine) roles(ctx sim.EngineCtx, r *replica) {
	switch {
	case r.rank == 0:
	case r.active && r.sawLower:
		r.active, r.sawLower = false, false
		e.failQueued(ctx, r)
		ctx.Assert().Reachable("logcp-sim: failback, the standby stepped down")
	case !r.active && e.now-r.lastOther >= sim.Time(e.p.TakeoverAfter):
		r.active, r.sawLower = true, false
		ctx.Assert().Reachable("logcp-sim: standby took over")
	}
}

// failQueued answers the commands a node never started with a 503.
func (e *Engine) failQueued(ctx sim.EngineCtx, r *replica) {
	for _, q := range r.q {
		if rq := e.req(q.req); rq != nil {
			e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Err: core.ErrUnavailable})
		}
	}
	r.q = nil
}

// crash wipes r's memory; its Puts already on the wire still land.
func (e *Engine) crash(ctx sim.EngineCtx, r *replica) {
	r.up, r.inc = false, r.inc+1
	r.downUntil = e.now + sim.Time(between(ctx.Rand(), e.p.DownDur))
	r.q, r.ops, r.waits, r.proj, r.st, r.applied = nil, nil, nil, nil, &state{}, -1
	r.active = r.rank == 0
	ctx.Fault("logcp_crash", core.Field{Key: "rank", Value: core.I64(r.rank)})
	ctx.Assert().Reachable("logcp-sim: replica crashed")
}

func (e *Engine) restart(ctx sim.EngineCtx, r *replica) {
	r.up, r.lastOther, r.lastAppend = true, e.now, e.now
	e.restore(ctx, r)
}

func between(r sim.CountedRand, d [2]core.Duration) core.Duration {
	if d[1] <= d[0] {
		return d[0]
	}
	return core.Duration(r.Between(int64(d[0]), int64(d[1])))
}

// faults may stall or crash one replica (never while the other is impaired).
func (e *Engine) faults(ctx sim.EngineCtx) {
	if e.recovery {
		return
	}
	for i, r := range e.reps {
		other := e.reps[1-i]
		if !r.live(e.now) || !other.live(e.now) {
			continue
		}
		if ctx.Rand().Chance(e.p.StallProb) {
			r.stallUntil = e.now + sim.Time(between(ctx.Rand(), e.p.StallDur))
			ctx.Fault("logcp_stall", core.Field{Key: "rank", Value: core.I64(r.rank)})
			ctx.Assert().Reachable("logcp-sim: replica stalled")
		} else if ctx.Rand().Chance(e.p.CrashProb) {
			e.crash(ctx, r)
		}
	}
}

// onTick is the engine's heartbeat: faults, then each replica in rank order.
func (e *Engine) onTick(ctx sim.EngineCtx) {
	e.ticks++
	e.faults(ctx)
	for _, r := range e.reps {
		e.tickReplica(ctx, r)
	}
	ctx.Self(e.now+sim.Time(e.p.Tick), ctx.Lane("logengine/tick"), tickEv{})
}

func (e *Engine) tickReplica(ctx sim.EngineCtx, r *replica) {
	if !r.up {
		if e.now >= r.downUntil {
			e.restart(ctx, r)
		}
		return
	}
	if !r.live(e.now) {
		return
	}
	e.roles(ctx, r)
	e.flush(ctx, r)
	if r.active || e.ticks%2 == 0 { // a standby polls the tail at half the rate
		e.sync(ctx, r)
	}
	e.windowMutant(ctx, r)
	e.checkpoint(ctx, r)
	e.serveReads(ctx, r)
	e.checkDigest(ctx, r)
}

// windowMutant (BugPerReplicaWindow): the replica forgets keys on its own
// pace instead of at the stride boundaries of the log.
func (e *Engine) windowMutant(_ sim.EngineCtx, r *replica) {
	if e.p.Bug != BugPerReplicaWindow || r.applied < 0 {
		return
	}
	if n := r.st.forget(r.applied, e.p.WindowSlots); n > 0 {
		r.st.mix(0xF0F0)
		r.st.mix(uint64(n))
	}
}
