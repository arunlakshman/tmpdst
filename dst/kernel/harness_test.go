package kernel_test

import (
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// The test machine speaks two small sums: tin (inputs) and tout (outputs).
// tout is a sealed sum: the route below switches over every case of it, and
// the gochecksumtype linter checks that it stays exhaustive. tin is not marked
// as a sealed sum, so that a test script may handle only the inputs it cares
// about.

type tin interface {
	machine.Message
	isTin()
}

type (
	started struct{ self machine.NodeInstance } // the boot input
	stopReq struct{ deadline machine.NodeTime } // the graceful stop request
	tick    struct{ gen uint64 }                // a timer
	note    struct{ s string }                  // a Data input
	urgent  struct{ s string }                  // a Control input
	putDone struct {                            // the echo service answer
		req  uint64
		lost bool
	}
)

func (started) isTin() {}
func (stopReq) isTin() {}
func (tick) isTin()    {}
func (note) isTin()    {}
func (urgent) isTin()  {}
func (putDone) isTin() {}

func (m started) Fold(h *trace.Hash) {
	h.Uint64(1)
	h.String(string(m.self.Node))
	h.Bytes(m.self.Inc[:])
}
func (m stopReq) Fold(h *trace.Hash) { h.Uint64(2); h.Int64(int64(m.deadline)) }
func (m tick) Fold(h *trace.Hash)    { h.Uint64(3); h.Uint64(m.gen) }
func (m note) Fold(h *trace.Hash)    { h.Uint64(4); h.String(m.s) }
func (m urgent) Fold(h *trace.Hash)  { h.Uint64(5); h.String(m.s) }
func (m putDone) Fold(h *trace.Hash) { h.Uint64(6); h.Uint64(m.req); h.Bool(m.lost) }

//sumtype:decl
type tout interface {
	machine.Message
	isTout()
}

type (
	setTimer struct {
		id    machine.TimerID
		after machine.Duration
		msg   tin
	}
	cancelTimer struct{ id machine.TimerID }
	recordFact  struct{ f machine.Fact }
	endOut      struct {
		how   machine.End
		cause string
	}
	sendTo struct {
		to  string
		msg tin
	}
	sendSelf struct{ msg tin }
	put      struct{ req uint64 } // a call to the echo service
)

func (setTimer) isTout()    {}
func (cancelTimer) isTout() {}
func (recordFact) isTout()  {}
func (endOut) isTout()      {}
func (sendTo) isTout()      {}
func (sendSelf) isTout()    {}
func (put) isTout()         {}

func (m setTimer) Fold(h *trace.Hash) {
	h.Uint64(11)
	h.Uint64(uint64(m.id))
	h.Int64(int64(m.after))
	m.msg.Fold(h)
}
func (m cancelTimer) Fold(h *trace.Hash) { h.Uint64(12); h.Uint64(uint64(m.id)) }
func (m recordFact) Fold(h *trace.Hash)  { h.Uint64(13); m.f.Fold(h) }
func (m endOut) Fold(h *trace.Hash)      { h.Uint64(14); h.Uint64(uint64(m.how)); h.String(m.cause) }
func (m sendTo) Fold(h *trace.Hash)      { h.Uint64(15); h.String(m.to); m.msg.Fold(h) }
func (m sendSelf) Fold(h *trace.Hash)    { h.Uint64(16); m.msg.Fold(h) }
func (m put) Fold(h *trace.Hash)         { h.Uint64(17); h.Uint64(m.req) }

// echoReq is the request sum of the test services: put, and idle (a request
// that does nothing; no test sends it).
//
//sumtype:decl
type echoReq interface {
	machine.Message
	isEchoReq()
}

type idle struct{}

func (put) isEchoReq()          {}
func (idle) isEchoReq()         {}
func (idle) Fold(h *trace.Hash) { h.Uint64(18) }

// scriptM is a machine whose Step is a function.
type scriptM struct {
	fn   func(m *scriptM, now machine.Now, in tin) []tout
	env  machine.Env
	self machine.NodeInstance
	n    int // Steps so far
	h    uint64
	gen  uint64
}

func (m *scriptM) Step(now machine.Now, in tin) []tout {
	m.n++
	m.h = m.h*31 + uint64(m.n)
	return m.fn(m, now, in)
}

func (m *scriptM) StateHash() uint64 { return m.h }

type script = func(m *scriptM, now machine.Now, in tin) []tout

func fact(name string, v int64) recordFact {
	return recordFact{machine.Fact{Name: name, Fields: []machine.Field{{Key: "v", Value: machine.Int64Value(v)}}}}
}

// ping emits a "started" fact on boot and a "ping" fact every 10 ms.
func ping(m *scriptM, _ machine.Now, in tin) []tout {
	switch b := in.(type) {
	case started:
		m.gen++
		return []tout{fact("started", 0), setTimer{id: 1, after: 10 * machine.Millisecond, msg: tick{m.gen}}}
	case tick:
		if b.gen != m.gen {
			return nil
		}
		m.gen++
		return []tout{fact("ping", int64(m.n)), setTimer{id: 1, after: 10 * machine.Millisecond, msg: tick{m.gen}}}
	}
	return nil
}

// putter sends a put every 5 ms and emits "put_done" on each answer. It
// finishes a graceful stop at once.
func putter(m *scriptM, _ machine.Now, in tin) []tout {
	switch b := in.(type) {
	case started:
		m.gen++
		return []tout{fact("started", 0), setTimer{id: 1, after: 5 * machine.Millisecond, msg: tick{m.gen}}}
	case tick:
		if b.gen != m.gen {
			return nil
		}
		m.gen++
		return []tout{put{req: uint64(m.n)}, setTimer{id: 1, after: 5 * machine.Millisecond, msg: tick{m.gen}}}
	case putDone:
		lost := int64(0)
		if b.lost {
			lost = 1
		}
		return []tout{fact("put_done", lost)}
	case stopReq:
		return []tout{endOut{how: machine.Exit, cause: "stop"}}
	}
	return nil
}

// echo is a test service: it applies a put after a latency, may lose the
// answer, and answers 1 ms later.
type echo struct {
	lat       kernel.FastWithRareSlow
	lostProb  float64
	applied   uint64
	lost      uint64
	crashes   uint64
	recovered bool
}

func (e *echo) Name() string      { return "echo" }
func (e *echo) StateHash() uint64 { return e.applied*1000003 + e.lost*7919 + e.crashes }
func (e *echo) EnterRecovery()    { e.recovered = true }
func (e *echo) OnCrash(*kernel.ServiceContext[tin], machine.NodeInstance) {
	e.crashes++
}

func (e *echo) Handle(cx *kernel.ServiceContext[tin], from kernel.Ref[tin], req echoReq) {
	switch r := req.(type) {
	case put:
		lost := !e.recovered && cx.Rand().Chance(e.lostProb)
		if lost {
			cx.Fault("echo_response_lost")
		}
		at := cx.Now() + kernel.Time(e.lat.Sample(cx.Rand()))
		cx.Later(at, 1, func(cx *kernel.ServiceContext[tin]) {
			e.applied++
			if lost {
				e.lost++
			}
			cx.Deliver(from, cx.Now()+kernel.Time(machine.Millisecond), 2, putDone{req: r.req, lost: lost})
		})
	case idle:
	}
}

// harness builds a World with test nodes and one echo service.
type harness struct {
	w    *kernel.World
	addr map[string]kernel.Addr[tin]
	echo kernel.ServiceAddr[echoReq, tin]
	svc  *echo
}

func newHarness(cfg kernel.Config) *harness {
	h := &harness{w: kernel.New(cfg), addr: map[string]kernel.Addr[tin]{}, svc: &echo{}}
	h.echo = kernel.AddService[echoReq, tin](h.w, h.svc)
	return h
}

func (h *harness) route(r *kernel.Router[tin], out tout) {
	switch x := out.(type) {
	case setTimer:
		r.SetTimer(x.id, x.after, x.msg)
	case cancelTimer:
		r.CancelTimer(x.id)
	case recordFact:
		r.Fact(x.f)
	case endOut:
		r.End(x.how, x.cause)
	case sendTo:
		kernel.Send(r, h.addr[x.to], x.msg)
	case sendSelf:
		r.SendSelf(x.msg)
	case put:
		kernel.Call(r, h.echo, echoReq(x))
	}
}

// spec is the default node spec of a test node: boot input, stop request,
// urgent/started/stopReq in the Control inbox.
func (h *harness) spec(name string, fn script) kernel.NodeSpec[tin, tout] {
	return kernel.NodeSpec[tin, tout]{
		Name: machine.NodeID(name),
		New: func(env machine.Env, self machine.NodeInstance) machine.Machine[tin, tout] {
			return &scriptM{fn: fn, env: env, self: self}
		},
		Route:   h.route,
		Boot:    func(self machine.NodeInstance) tin { return started{self} },
		StopMsg: func(d machine.NodeTime) tin { return stopReq{d} },
		Class: func(in tin) machine.Class {
			switch in.(type) {
			case started, stopReq, urgent:
				return machine.Control
			case tick, note, putDone:
				return machine.Data
			}
			return machine.Data
		},
	}
}

// spawn adds a node with the default spec, changed by edit (may be nil).
func (h *harness) spawn(name string, fn script, edit func(s *kernel.NodeSpec[tin, tout])) kernel.Addr[tin] {
	s := h.spec(name, fn)
	if edit != nil {
		edit(&s)
	}
	a := kernel.Spawn(h.w, s)
	h.addr[name] = a
	return a
}

// run runs to the end and returns the result.
func (h *harness) run() kernel.Result {
	h.w.RunUntil(nil)
	return h.w.Finish()
}

// restartAfter is a NodeSpec.Restart that always restarts after a delay in
// [lo, hi].
func restartAfter(lo, hi machine.Duration) func(machine.End, *rng.Rand) (machine.Duration, bool) {
	return func(_ machine.End, r *rng.Rand) (machine.Duration, bool) {
		return machine.Duration(r.Between(int64(lo), int64(hi))), true
	}
}

func stat(res kernel.Result, name string) uint64 {
	for _, c := range res.Stats {
		if c.Name == name {
			return c.N
		}
	}
	panic("no stat " + name)
}

func hasFailure(res kernel.Result, k kernel.FailKind, property string) bool {
	for _, f := range res.Failures {
		if f.Kind == k && (property == "" || f.Property == property) {
			return true
		}
	}
	return false
}

// fnRule is a Rule made of two functions.
type fnRule struct {
	observe func(v kernel.View)
	atEnd   func(v kernel.View)
}

func (f *fnRule) Name() string { return "fn" }
func (f *fnRule) Observe(v kernel.View) {
	if f.observe != nil {
		f.observe(v)
	}
}
func (f *fnRule) AtEnd(v kernel.View) {
	if f.atEnd != nil {
		f.atEnd(v)
	}
}

func ms(n int64) kernel.Time { return kernel.Time(n) * kernel.Time(machine.Millisecond) }
