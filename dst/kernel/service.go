package kernel

import (
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// Service simulates one outside system (an object store, a network, a shared
// log). Req is the sum of the requests it takes. In is the input type of the
// nodes it talks to: it answers with In values, and may also send In values to
// nodes that did not call it (a network does).
//
// Every call into a service clears its cached state hash. A panic in a service
// halts the run with FailPanic.
type Service[Req, In machine.Message] interface {
	Name() string
	StateHash() uint64
	// Handle starts one request at issue time. Draw every fault coin and every
	// delay here, so that draws happen in issue order. Never complete anything
	// at once: every answer is a scheduled Deliver.
	Handle(cx *ServiceContext[In], from Ref[In], req Req)
	// OnCrash says that an incarnation ended. Effects already applied stand.
	// Entries the service queued for itself (Later) are not dropped, so a write
	// already on the wire can still land after the caller died.
	OnCrash(cx *ServiceContext[In], who machine.NodeInstance)
	// EnterRecovery: no new faults for requests issued after this call. Plans
	// already made stand.
	EnterRecovery()
}

// ServiceAddr names a service of one World. Only AddService makes one. The
// zero ServiceAddr, or one of another World, is a protocol failure.
type ServiceAddr[Req, In machine.Message] struct{ h *serviceHost[Req, In] }

// ServiceContext is what a service uses to act on the world.
type ServiceContext[In machine.Message] struct {
	w *World
	b *serviceBase
}

// serviceBase is the part of a service that does not depend on its types.
type serviceBase struct {
	name string
	hashCache
}

// serviceHost is one service with its context.
type serviceHost[Req, In machine.Message] struct {
	serviceBase
	w   *World
	svc Service[Req, In]
	cx  *ServiceContext[In]
}

// AddService adds a service to the world. A nil service or a name used twice
// is a protocol failure and gives the zero ServiceAddr.
func AddService[Req, In machine.Message](w *World, s Service[Req, In]) ServiceAddr[Req, In] {
	if s == nil {
		w.Fail(FailProtocol, "add_service", "a nil Service was added")
		return ServiceAddr[Req, In]{}
	}
	name := s.Name()
	if w.svcNames[name] {
		w.Fail(FailProtocol, "add_service", "the service name "+name+" is used twice")
		return ServiceAddr[Req, In]{}
	}
	h := &serviceHost[Req, In]{serviceBase: serviceBase{name: name}, w: w, svc: s}
	h.cx = &ServiceContext[In]{w: w, b: &h.serviceBase}
	w.svcNames[name] = true
	w.services = append(w.services, h)
	return ServiceAddr[Req, In]{h: h}
}

// checkService reports whether s is a service of w. If not, it is a protocol
// failure.
func checkService[Req, In machine.Message](w *World, s ServiceAddr[Req, In]) bool {
	switch {
	case s.h == nil:
		w.Fail(FailProtocol, "zero address", "a zero ServiceAddr was used")
		return false
	case s.h.w != w:
		w.Fail(FailProtocol, "foreign address", "a ServiceAddr of another World was used")
		return false
	}
	return true
}

func (h *serviceHost[Req, In]) base() *serviceBase { return &h.serviceBase }

func (h *serviceHost[Req, In]) stateHash() uint64 { return h.svc.StateHash() }

func (h *serviceHost[Req, In]) onCrash(who machine.NodeInstance) { h.svc.OnCrash(h.cx, who) }

func (h *serviceHost[Req, In]) enterRecovery() { h.svc.EnterRecovery() }

func (h *serviceHost[Req, In]) handle(from Ref[In], req Req) {
	h.ok = false
	if !h.w.guard("Handle "+h.name, func() { h.svc.Handle(h.cx, from, req) }) {
		h.w.halted = true
	}
}

// Now is the virtual time.
func (c *ServiceContext[In]) Now() Time { return c.w.q.now }

// Rand is the one random stream of the run.
func (c *ServiceContext[In]) Rand() *rng.Rand { return c.w.rand }

// Faults is the buggify injector of the run.
func (c *ServiceContext[In]) Faults() *fault.Injector { return c.w.faults }

// Assert records assertions in the assertion log of the run.
func (c *ServiceContext[In]) Assert() machine.Assertions { return env{c.w} }

// Deliver queues msg for one incarnation at time at on lane. If `to` is not up
// now, msg is dropped at once and nothing is drawn (it returns 0). If `to`
// ends before at, msg is dropped then.
func (c *ServiceContext[In]) Deliver(to Ref[In], at Time, lane Lane, msg In) EntryID {
	if !checkAddr(c.w, to.a) || !to.a.n.up || to.a.n.inc != to.inc {
		return 0
	}
	return deliver(to.a.n, at, lane, msg, to.inc, true)
}

// Later runs fn as a later step of this service, at time at on lane. The
// closure keeps its types, so it needs no opaque body and no type switch. A
// later step is not dropped when a node crashes.
func (c *ServiceContext[In]) Later(at Time, lane Lane, fn func(cx *ServiceContext[In])) EntryID {
	w := c.w
	e := &entry{at: at, lane: lane, kind: kindServiceStep, node: -1, service: c.b.name}
	e.run = func() {
		w.counters[countServiceSteps]++
		w.logMsg("service "+c.b.name+" later", nil)
		c.b.ok = false
		if !w.guard("Later "+c.b.name, func() { fn(c) }) {
			w.halted = true
			return
		}
		w.attr++
	}
	return w.q.add(e)
}

// Cancel retires a pending entry (a Deliver or a Later).
func (c *ServiceContext[In]) Cancel(id EntryID) { c.w.q.cancel(id) }

// Current is the incarnation of a node that is up now (a network uses it to
// connect).
func (c *ServiceContext[In]) Current(a Addr[In]) (Ref[In], bool) { return CurrentRef(c.w, a) }

// Fault records an injected fault: a "sim_fault" fact with the kind and the
// service name, then the extra fields.
func (c *ServiceContext[In]) Fault(kind string, fields ...machine.Field) {
	c.w.counters[countFaults]++
	all := append([]machine.Field{
		{Key: "kind", Value: machine.StringValue(kind)},
		{Key: "service", Value: machine.StringValue(c.b.name)},
	}, fields...)
	c.w.kernelFact("sim_fault", all...)
}
