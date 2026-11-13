// Package faulty turns a fault-free spec into a simulated service.
//
// A spec is a small program with two methods: Apply runs one op and gives the
// right answer, and StateHash hashes its whole state. The spec never fails and
// never waits. Faulty puts timing and faults around it, so that the code under
// test meets a service that is slow, that refuses calls, that times out and that
// loses answers, as a real outside system does.
//
// The faults are exactly these, and nothing else:
//
//   - Slow reply: the apply time and the respond time are drawn from a delay that
//     is mostly fast and has a rare slow tail (Faults.ApplyLatency, RespondLatency).
//   - Error, nothing applied (the "503"): the call is refused, the spec never sees
//     it, and the caller gets Unavailable (Faults.Unavailable503).
//   - Timeout: the caller gets TimedOut after CallerTimeout. The op is applied or
//     not, by a fair coin drawn when it lands (Faults.Timeout).
//   - Applied, but the reply is lost: the op is applied, and the caller gets
//     TimedOut after CallerTimeout (Faults.LostResponse).
//   - Outage window: for a time, every call is refused as in the "503" case
//     (Faults.Outage).
//   - Optional, set in Shape: a limit that refuses some ops before the spec sees
//     them (Shape.Limit), and a bit flip in the answer of some ops
//     (Shape.Corrupt).
//
// All coins and delays are drawn when the call is issued, in a fixed order (see
// Service.Handle), so one seed always gives the same run. After EnterRecovery no
// new fault is drawn. A call that was planned before stays as planned.
//
// This package has no product words. The product gives it the spec, the fault
// numbers, and a Shape that says how to read a request and how to build an answer.
package faulty

import (
	"slices"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// Spec is the fault-free program that gives the right answer. It has the same
// two methods as a check/spectest.Spec, so one spec serves both.
type Spec[Op, Res any] interface {
	Apply(op Op) Res
	StateHash() uint64
}

// Failure says how a call failed. The product turns it into its own error value.
type Failure uint8

const (
	// Unavailable means that the service refused the call and applied nothing.
	Unavailable Failure = iota + 1
	// TimedOut means that the caller gave up. The op may or may not have been applied.
	TimedOut
)

// Outage is a window in which every call is refused. A window starts by a coin
// with chance Prob when a call is issued and no window is open. It lasts a time
// drawn from Dur (the end is not included). Windows never overlap.
type Outage struct {
	Prob float64
	Dur  [2]machine.Duration
}

// Faults are the numbers of the fault kinds. A chance of 0 turns a kind off.
type Faults struct {
	ApplyLatency   kernel.FastWithRareSlow // from issue to the effect
	RespondLatency kernel.FastWithRareSlow // from the effect to the reply
	Unavailable503 float64                 // chance that a call is refused, nothing applied
	Timeout        float64                 // chance of a timeout; the op may or may not be applied
	LostResponse   float64                 // chance that the op is applied and the reply is lost
	Outage         Outage
	CallerTimeout  machine.Duration // when the caller sees TimedOut
}

// Asserts are the assertions of the fault kinds. The product declares them at
// package level (the assertion catalog needs that), with names it chooses.
type Asserts struct {
	Outage      assert.Reachable // an outage window started
	Unavailable assert.Reachable // a call was refused
	Timeout     assert.Reachable // a timeout was injected
	Lost        assert.Reachable // a reply was lost
	Applied     assert.Sometimes // a timed-out op was applied
	NotApplied  assert.Sometimes // a timed-out op was not applied
}

// Corruption flips a bit in the answer of some ops, once in a while. The spec
// state stays whole: only the copy that goes to the caller changes.
type Corruption[Op, Res any] struct {
	Prob    float64          // chance, drawn for each op that Targets accepts
	Targets func(op Op) bool // which ops can be corrupted (reads, for example)
	// Flip flips the bit with number bits (taken modulo the size of the data) in
	// res. It reports the bit it flipped, or false when res has nothing to flip.
	// The answer of Apply must be a copy, so Flip may change it in place.
	Flip    func(res Res, bits uint64) (out Res, bit uint64, ok bool)
	Kind    string           // the name of the fault fact, for example "obj_get_corrupt"
	Reached assert.Reachable // recorded when a bit was flipped
}

// Shape tells Faulty how to read a request and how to build an answer. It is
// the only place where the product types meet the spec types.
type Shape[Req, In machine.Message, Op, Res any] struct {
	// Name is the service name. It is also the name in fault facts.
	Name string
	// Prefix starts the names of the fault facts: Prefix+"_outage",
	// Prefix+"_503", Prefix+"_timeout" and Prefix+"_lost_response".
	Prefix  string
	Asserts Asserts
	// Op returns the spec op inside a request.
	Op func(req Req) Op
	// RequestID returns the id the caller gave the request.
	RequestID func(req Req) machine.RequestID
	// Mutating reports whether op may change the spec state. A read needs no
	// landing when its answer is dropped.
	Mutating func(op Op) bool
	// Changed reports whether the answer res says that the spec state changed.
	Changed func(res Res) bool
	// Reply builds the normal answer.
	Reply func(req Req, res Res) In
	// Fail builds the answer for a failure.
	Fail func(req Req, f Failure) In
	// Limit is optional. It runs when an op lands, before the spec. When it
	// returns true, the answer is the returned one and the spec never sees op.
	Limit func(op Op) (Res, bool)
	// Corrupt is optional.
	Corrupt *Corruption[Op, Res]
}

// How an issued call ends.
type kind uint8

const (
	replyNormal      kind = iota // apply at T1, reply at T2
	replyUnavailable             // refused, nothing applied
	replyTimeout                 // the caller sees TimedOut; applied or not, drawn at landing
	replyLost                    // applied at T1; the caller sees TimedOut
)

// plan is one call in flight.
type plan[Req, In machine.Message] struct {
	id     uint64
	from   kernel.Ref[In]
	req    Req
	kind   kind
	t1, t2 kernel.Time
	flag   bool   // the corruption coin came up
	bits   uint64 // which bit, before the modulo
	lane   kernel.Lane
}

// Service is the spec with faults around it. It is a kernel.Service.
type Service[Req, In machine.Message, Op, Res any] struct {
	shape  Shape[Req, In, Op, Res]
	faults Faults
	spec   Spec[Op, Res]
	lanes  *kernel.Lanes[machine.NodeID] // one lane for each calling node

	nextID   uint64
	plans    []plan[Req, In] // in issue order
	until    kernel.Time     // the end of the outage window
	recovery bool
	gen      uint64
	// specHash caches spec.StateHash(). It is valid only while specOK. Every path
	// that can change the spec clears specOK (landing a mutating op, Bump).
	specHash uint64
	specOK   bool
}

var _ kernel.Service[machine.Message, machine.Message] = (*Service[machine.Message, machine.Message, int, int])(nil)

// New returns a service around spec. The world gives it the lane table.
func New[Req, In machine.Message, Op, Res any](w *kernel.World, spec Spec[Op, Res], f Faults, s Shape[Req, In, Op, Res]) *Service[Req, In, Op, Res] {
	return &Service[Req, In, Op, Res]{shape: s, faults: f, spec: spec, lanes: kernel.NewLanes[machine.NodeID](w)}
}

// Gen counts the changes that were applied. Bump adds a change made outside a
// call, for example a setup step on the spec.
func (s *Service[Req, In, Op, Res]) Gen() uint64 { return s.gen }

// Bump records a change of the spec made outside a call. It also clears the
// cached spec hash.
func (s *Service[Req, In, Op, Res]) Bump() { s.gen++; s.specOK = false }

// specStateHash is the spec hash, computed again only after a change.
func (s *Service[Req, In, Op, Res]) specStateHash() uint64 {
	if !s.specOK {
		s.specHash, s.specOK = s.spec.StateHash(), true
	}
	return s.specHash
}

// SpecStateHash is the cached spec hash. A test can compare it with the spec's
// own StateHash: a stale cache shows as a difference.
func (s *Service[Req, In, Op, Res]) SpecStateHash() uint64 { return s.specStateHash() }

// Name returns the service name.
func (s *Service[Req, In, Op, Res]) Name() string { return s.shape.Name }

// EnterRecovery stops new faults for the calls issued after it.
func (s *Service[Req, In, Op, Res]) EnterRecovery() { s.recovery = true }

// OnCrash does nothing. An effect that was applied stands, and a landing that
// was scheduled stays scheduled. The kernel drops the delivery to an incarnation
// that ended, and landing skips the corruption step for a caller that is gone.
func (s *Service[Req, In, Op, Res]) OnCrash(*kernel.ServiceContext[In], machine.NodeInstance) {}

// between returns a value in [lo, hi). It draws once when hi > lo, even when
// there is only one value, and never when hi <= lo. This is the rule of the
// outage length in every run so far; rng.Between is different at that edge.
func between(r *rng.Rand, lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(r.Intn(int(hi-lo)))
}

// draw draws the coins of the boundary faults: the outage window, then the
// three coins "503", timeout and lost, always all three and in this order. It
// records the fault that wins. A refusal beats a timeout, and a timeout beats a
// lost reply.
func (s *Service[Req, In, Op, Res]) draw(cx *kernel.ServiceContext[In], request machine.Field) kind {
	r, now, f, pre, a := cx.Rand(), cx.Now(), s.faults, s.shape.Prefix, s.shape.Asserts
	if now >= s.until && r.Chance(f.Outage.Prob) {
		d := f.Outage.Dur[0]
		if f.Outage.Dur[1] > d {
			d = machine.Duration(between(r, int64(f.Outage.Dur[0]), int64(f.Outage.Dur[1])))
		}
		s.until = now + kernel.Time(d)
		cx.Fault(pre+"_outage", machine.Field{Key: "dur", Value: machine.Int64Value(d)})
		cx.Assert().Reachable(a.Outage)
	}
	inOutage := now < s.until
	is503, isTimeout, isLost := r.Chance(f.Unavailable503), r.Chance(f.Timeout), r.Chance(f.LostResponse)
	switch {
	case inOutage || is503:
		cause := "coin"
		if inOutage {
			cause = "outage"
		}
		cx.Fault(pre+"_503", request, machine.Field{Key: "cause", Value: machine.StringValue(cause)})
		cx.Assert().Reachable(a.Unavailable)
		return replyUnavailable
	case isTimeout:
		cx.Fault(pre+"_timeout", request)
		cx.Assert().Reachable(a.Timeout)
		return replyTimeout
	case isLost:
		cx.Fault(pre+"_lost_response", request)
		cx.Assert().Reachable(a.Lost)
		return replyLost
	}
	return replyNormal
}

// Handle plans one call. It implements kernel.Service. It draws, in this order:
// the outage coin (and the outage length), the three coins "503", timeout and
// lost, the apply delay, the respond delay, and the corruption coin (and the
// bit). After EnterRecovery, only the two delays are drawn.
func (s *Service[Req, In, Op, Res]) Handle(cx *kernel.ServiceContext[In], from kernel.Ref[In], req Req) {
	r, now, f := cx.Rand(), cx.Now(), s.faults
	op := s.shape.Op(req)
	request := machine.Field{Key: "req", Value: machine.Uint64Value(uint64(s.shape.RequestID(req)))}
	k := replyNormal
	if !s.recovery {
		k = s.draw(cx, request)
	}
	t1 := now + kernel.Time(f.ApplyLatency.Sample(r))
	t2 := t1 + kernel.Time(f.RespondLatency.Sample(r))
	var flag bool
	var bits uint64
	if c := s.shape.Corrupt; !s.recovery && c != nil && c.Targets(op) && r.Chance(c.Prob) {
		flag, bits = true, r.Uint64()
	}

	s.nextID++
	pl := plan[Req, In]{id: s.nextID, from: from, req: req, kind: k, t1: t1, t2: t2, flag: flag, bits: bits, lane: s.lanes.Get(from.Addr().ID())}
	switch k {
	case replyUnavailable:
		cx.Deliver(from, t2, pl.lane, s.shape.Fail(req, Unavailable))
		return
	case replyTimeout, replyLost:
		cx.Deliver(from, now+kernel.Time(f.CallerTimeout), pl.lane, s.shape.Fail(req, TimedOut))
		if !s.shape.Mutating(op) {
			return // nothing to apply, and the caller already has its answer
		}
	}
	id := pl.id
	cx.Later(t1, pl.lane, func(cx *kernel.ServiceContext[In]) { s.land(cx, id) })
	s.plans = append(s.plans, pl)
}

// land lands one planned call: it runs the op on the spec and sends the reply.
func (s *Service[Req, In, Op, Res]) land(cx *kernel.ServiceContext[In], id uint64) {
	i := -1
	for j := range s.plans {
		if s.plans[j].id == id {
			i = j
		}
	}
	if i < 0 {
		return
	}
	pl := s.plans[i]
	s.plans = slices.Delete(s.plans, i, i+1)
	apply := true
	if pl.kind == replyTimeout {
		apply = cx.Rand().Chance(0.5)
		cx.Assert().Sometimes(apply, s.shape.Asserts.Applied)
		cx.Assert().Sometimes(!apply, s.shape.Asserts.NotApplied)
	}
	if !apply {
		return
	}
	op := s.shape.Op(pl.req)
	var res Res
	refused := false
	if s.shape.Limit != nil {
		res, refused = s.shape.Limit(op)
	}
	if !refused {
		res = s.spec.Apply(op)
	}
	if s.shape.Mutating(op) {
		s.specOK = false // whatever Changed says, the cache must not outlive a mutating call
	}
	if s.shape.Changed(res) {
		s.gen++
		s.specOK = false // a change that Mutating did not name must not leave a stale hash
	}
	if pl.kind != replyNormal {
		return
	}
	if cur, up := cx.Current(pl.from.Addr()); !up || cur != pl.from {
		return // the caller died; the effect stands and nobody is left to tell
	}
	if c := s.shape.Corrupt; c != nil && pl.flag {
		if out, bit, ok := c.Flip(res, pl.bits); ok {
			res = out
			cx.Fault(c.Kind, machine.Field{Key: "req", Value: machine.Uint64Value(uint64(s.shape.RequestID(pl.req)))}, machine.Field{Key: "bit", Value: machine.Uint64Value(bit)})
			cx.Assert().Reachable(c.Reached)
		}
	}
	cx.Deliver(pl.from, max(pl.t2, cx.Now()), pl.lane, s.shape.Reply(pl.req, res))
}

// StateHash folds the spec hash, the outage window, the recovery flag and the
// calls in flight, in issue order.
func (s *Service[Req, In, Op, Res]) StateHash() uint64 {
	h := trace.New()
	mix := h.Uint64
	mix(s.specStateHash())
	mix(uint64(s.until))
	h.Bool(s.recovery)
	mix(uint64(len(s.plans)))
	for _, p := range s.plans {
		mix(p.id)
		who := p.from.Instance()
		for i := range len(who.Node) {
			mix(uint64(who.Node[i]))
		}
		for _, b := range who.Inc {
			mix(uint64(b))
		}
		mix(uint64(s.shape.RequestID(p.req)))
		mix(uint64(p.kind))
		mix(uint64(p.t1))
		mix(uint64(p.t2))
		mix(p.bits)
	}
	return h.Sum()
}
