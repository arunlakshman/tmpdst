package logengine

import (
	"slices"

	"manifester/core"
	"manifester/sim"
)

// Internal entries the engine schedules for itself.
type (
	tickEv   struct{}
	landEv   struct{ op uint64 }
	answerEv struct {
		op  uint64
		out outcome
	}
	deadlineEv struct{ req uint64 }
)

type fault uint8

const (
	fNone fault = iota
	fUnavailable
	fTimeout // the caller gives up; the call may or may not reach the service
	fLost    // the call is served, its answer is dropped
)

func cause(outage bool) string {
	if outage {
		return "outage"
	}
	return "coin"
}

func (e *Engine) prob(p float64) float64 {
	if e.recovery {
		return 0
	}
	return p
}

// plan draws the boundary faults of one call, in the order of the other
// engines: outage, 503, timeout, lost.
func (e *Engine) plan(ctx sim.EngineCtx) fault {
	if e.recovery {
		return fNone
	}
	r, now, p := ctx.Rand(), ctx.Now(), e.f
	if now >= e.until && r.Chance(p.Outage.Prob) {
		d := p.Outage.Dur[0]
		if p.Outage.Dur[1] > d {
			d = core.Duration(r.Between(int64(p.Outage.Dur[0]), int64(p.Outage.Dur[1])))
		}
		e.until = now + sim.Time(d)
		ctx.Fault("cp_outage", core.Field{Key: "dur", Value: core.I64(d)})
		ctx.Assert().Reachable("cp outage started")
	}
	out := now < e.until
	is503, isTimeout, isLost := r.Chance(p.Unavailable503), r.Chance(p.Timeout), r.Chance(p.LostResponse)
	switch {
	case out || is503:
		ctx.Fault("cp_503", core.Field{Key: "cause", Value: core.Str(cause(out))})
		ctx.Assert().Reachable("cp 503 injected")
		return fUnavailable
	case isTimeout:
		ctx.Fault("cp_timeout")
		ctx.Assert().Reachable("cp timeout injected")
		return fTimeout
	case isLost:
		ctx.Fault("cp_lost_response")
		ctx.Assert().Reachable("cp lost response injected")
		return fLost
	}
	return fNone
}

// Execute starts a, a core.CPCall.
func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.Action) {
	e.enter(ctx)
	call := a.(core.CPCall)
	k := e.plan(ctx)
	if k == fUnavailable {
		e.deliver(ctx, from, core.CPDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	e.nextReq++
	rq := &request{id: e.nextReq, from: from, call: call, muted: k != fNone, rank: -1}
	rq.deadline = ctx.Now() + sim.Time(max(e.f.CallerTimeout, 500*core.Millisecond))
	ctx.Self(rq.deadline, ctx.Lane("logengine/deadline"), deadlineEv{req: rq.id})
	e.reqs = append(e.reqs, rq)
	if k == fTimeout && ctx.Rand().Chance(0.5) {
		return // never reached the service; the caller times out
	}
	e.admit(ctx, rq)
}

// enter records the time and starts the tick chain on first use.
func (e *Engine) enter(ctx sim.EngineCtx) {
	e.now = ctx.Now()
	if !e.started {
		e.started = true
		for _, r := range e.reps {
			r.lastOther, r.lastAppend = e.now, e.now
		}
		ctx.Self(e.now+sim.Time(e.p.Tick), ctx.Lane("logengine/tick"), tickEv{})
	}
}

// admit routes a request to a replica.
func (e *Engine) admit(ctx sim.EngineCtx, rq *request) {
	switch op := rq.call.Op.(type) {
	case core.CommitSegment:
		e.admitCommit(ctx, rq, op)
	case core.Lookup:
		if op.From < 0 {
			e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Err: core.ErrInvalid})
			return
		}
		e.admitRead(ctx, rq)
	case core.DescribeTopics:
		e.admitRead(ctx, rq)
	default:
		e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Err: core.ErrInvalid})
	}
}

func (e *Engine) admitCommit(ctx sim.EngineCtx, rq *request, op core.CommitSegment) {
	i := e.routeWrite() // shape errors are decided by apply, after the key check (the spec's order)
	if i < 0 {
		e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Err: core.ErrUnavailable})
		return
	}
	r := e.reps[i]
	r.q = append(r.q, qcmd{c: cmd{key: op.SegmentKey, batches: slices.Clone(op.Batches)}, req: rq.id})
	e.flush(ctx, r)
}

// routeWrite is the load balancer for commits: the lowest-ranked active replica
// that is up and not stalled; -1 when every node answers 503.
func (e *Engine) routeWrite() int {
	for i, r := range e.reps {
		if r.live(e.now) && r.active {
			return i
		}
	}
	return -1
}

func (r *replica) live(now sim.Time) bool { return r.up && now >= r.stallUntil }

func (e *Engine) admitRead(ctx sim.EngineCtx, rq *request) {
	var cands []int
	for i, r := range e.reps {
		if r.live(e.now) {
			cands = append(cands, i)
		}
	}
	if len(cands) == 0 {
		e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Err: core.ErrUnavailable})
		return
	}
	rq.rank, rq.tail = cands[int(rq.id)%len(cands)], e.hi
	e.serveReads(ctx, e.reps[rq.rank])
}

// serveReads answers the reads of r that have reached their tail.
func (e *Engine) serveReads(ctx sim.EngineCtx, r *replica) {
	for _, rq := range slices.Clone(e.reqs) {
		if rq.rank != r.rank || rq.done || r.applied < rq.tail || !r.live(e.now) {
			continue
		}
		switch op := rq.call.Op.(type) {
		case core.Lookup:
			e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Resp: r.st.lookup(op)})
		case core.DescribeTopics:
			e.finish(ctx, rq, core.CPDone{Req: rq.call.Req, Resp: r.st.describe(op)})
		}
	}
}

func (e *Engine) req(id uint64) *request {
	i, ok := slices.BinarySearchFunc(e.reqs, id, func(r *request, id uint64) int { return int(r.id) - int(id) })
	if !ok {
		return nil
	}
	return e.reqs[i]
}

// finish answers rq once: delivered after the respond latency unless the
// answer is dropped (timeout, lost response) or the caller died.
func (e *Engine) finish(ctx sim.EngineCtx, rq *request, c core.CPDone) {
	if rq.done {
		return
	}
	if rq.muted { // the caller hears a timeout at the deadline
		rq.rank = -1
		return
	}
	rq.done = true
	e.drop(rq.id)
	e.deliver(ctx, rq.from, c)
}

func (e *Engine) drop(id uint64) {
	e.reqs = slices.DeleteFunc(e.reqs, func(r *request) bool { return r.id == id })
}

func (e *Engine) deliver(ctx sim.EngineCtx, to core.NodeRef, c core.CPDone) {
	if cur, up := ctx.(sim.NodeCurrent).Current(to.Node); !up || cur != to {
		return
	}
	at := ctx.Now() + sim.Time(e.f.RespondLatency.Sample(ctx.Rand()))
	ctx.Deliver(to, at, ctx.Lane("logengine/"+string(to.Node)), c)
}

// onDeadline gives up on a call that was not answered in time.
func (e *Engine) onDeadline(ctx sim.EngineCtx, id uint64) {
	rq := e.req(id)
	if rq == nil || rq.done {
		return
	}
	rq.done = true
	e.drop(id)
	if cur, up := ctx.(sim.NodeCurrent).Current(rq.from.Node); !up || cur != rq.from {
		return
	}
	ctx.Deliver(rq.from, ctx.Now(), ctx.Lane("logengine/"+string(rq.from.Node)),
		core.CPDone{Req: rq.call.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied})
}

// OnInternal consumes an entry the engine scheduled for itself.
func (e *Engine) OnInternal(ctx sim.EngineCtx, body any) {
	e.now = ctx.Now()
	switch b := body.(type) {
	case tickEv:
		e.onTick(ctx)
	case landEv:
		e.onLand(ctx, b.op)
	case answerEv:
		e.onAnswer(ctx, b.op, b.out)
	case deadlineEv:
		e.onDeadline(ctx, b.req)
	}
}
