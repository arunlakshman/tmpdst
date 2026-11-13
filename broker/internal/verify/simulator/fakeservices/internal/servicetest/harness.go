// Package servicetest is a small test world for the fake services. It is a real
// kernel.World with a few caller nodes. A test makes a caller issue a request, runs
// the world, and reads what the services delivered to the callers.
//
// It replaces the hand-written queue of the old tree (fakeservicetest.Ctx): the
// services now need a kernel, because a kernel.ServiceContext cannot be built by
// hand. The package does not import "testing", so that it can be used by the
// determinism gate; tests give a TB.
package servicetest

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// TB is the part of testing.TB that the harness needs.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
	Cleanup(fn func())
}

// Addr names a caller node.
type Addr = kernel.Addr[core.EventData]

// Router is the router of a caller node.
type Router = kernel.Router[core.EventData]

// Delivery is one event that a service delivered to a caller node.
type Delivery struct {
	At   kernel.Time
	To   machine.NodeID
	Body core.EventData
}

// cmdGen marks a TimerFired that the harness itself injected, to make a caller
// issue a request. No service sends a TimerFired.
const cmdGen machine.TimerVersion = 0xC0FFEE

// Harness is a kernel world with caller nodes.
type Harness struct {
	t TB
	// W is the world. Add services to it with kernel.AddService.
	W *kernel.World
	// Route handles one request of a caller. The test sets it after it added the
	// services. Its body calls kernel.Call for the service of the request.
	Route func(r *Router, out core.Action)
	// Delivered lists every event that reached a caller, in order.
	Delivered []Delivery
	// Seed is the seed of the world.
	Seed uint64

	samples []string // for Transcript
	names   []string
	addrs   map[string]Addr
	cmds    map[machine.TimerID]core.Action
	nextCmd machine.TimerID
}

// New makes a world for seed with one caller node for each name, and boots the
// callers. When the test ends, the harness finishes the world and reports any
// failure of the run (a panic, a broken Always).
func New(t TB, seed uint64, names ...string) *Harness {
	t.Helper()
	h := &Harness{
		t: t, W: kernel.New(kernel.Config{Seed: seed}), Seed: seed,
		names: names, addrs: map[string]Addr{}, cmds: map[machine.TimerID]core.Action{},
	}
	for _, n := range names {
		h.addrs[n] = kernel.Spawn(h.W, kernel.NodeSpec[core.EventData, core.Action]{
			Name: machine.NodeID(n),
			New: func(_ machine.Env, self machine.NodeInstance) machine.Machine[core.EventData, core.Action] {
				return &caller{h: h, self: self}
			},
			Route: h.route,
		})
	}
	h.W.Start()
	t.Cleanup(func() {
		for _, f := range h.W.Finish().Failures {
			t.Errorf("run failure: %s: %s", f.Property, f.Detail)
		}
	})
	return h
}

func (h *Harness) route(r *Router, out core.Action) {
	if h.Route == nil {
		h.t.Fatalf("servicetest: no Route set for %T", out)
		return
	}
	h.Route(r, out)
}

// caller is the machine of a caller node. It issues the request of a command and
// records every other event.
type caller struct {
	h    *Harness
	self machine.NodeInstance
}

// Step implements machine.Machine.
func (c *caller) Step(_ machine.Now, in core.EventData) []core.Action {
	if t, ok := in.(core.TimerFired); ok && t.Gen == cmdGen {
		act := c.h.cmds[t.ID]
		delete(c.h.cmds, t.ID)
		if act == nil {
			return nil
		}
		return []core.Action{act}
	}
	c.h.Delivered = append(c.h.Delivered, Delivery{At: c.h.W.Now(), To: c.self.Node, Body: in})
	return nil
}

// StateHash implements machine.Machine. A caller has no state that matters.
func (c *caller) StateHash() uint64 { return 0 }

// Addr returns the address of a caller.
func (h *Harness) Addr(name string) Addr { return h.addrs[name] }

// Ref returns the live incarnation of a caller.
func (h *Harness) Ref(name string) machine.NodeInstance {
	ref, _ := h.W.Current(h.addrs[name])
	return ref
}

// Now is the virtual time.
func (h *Harness) Now() kernel.Time { return h.W.Now() }

// Do makes caller `from` issue a at the current time. It is a command to the
// caller, so it runs when the world runs.
func (h *Harness) Do(from string, a core.Action) { h.DoAfter(0, from, a) }

// DoAfter makes caller `from` issue a after delay d.
func (h *Harness) DoAfter(d machine.Duration, from string, a core.Action) {
	h.nextCmd++
	id := h.nextCmd
	h.cmds[id] = a
	kernel.Inject[core.EventData](h.W, h.addrs[from], d, core.TimerFired{ID: id, Gen: cmdGen})
}

// Issue makes caller `from` issue a now, and takes queue entries until it did.
// The service has drawn its random values for a when Issue returns, as if the
// service had been called directly at the current time. Entries of the same
// instant that come first in the queue run before it.
func (h *Harness) Issue(from string, a core.Action) {
	h.t.Helper()
	h.Do(from, a)
	for len(h.cmds) > 0 {
		if !h.Step() {
			h.t.Fatalf("servicetest: the command of %s was never taken (the world halted?)", from)
		}
	}
}

// Run takes queue entries until the queue is empty.
func (h *Harness) Run() {
	h.t.Helper()
	h.W.RunUntil(nil)
}

// RunTo takes queue entries up to time at, and leaves the clock at at. Entries at
// exactly that time that come after the stop mark stay in the queue.
func (h *Harness) RunTo(at kernel.Time) {
	h.t.Helper()
	reached := false
	h.W.At(at, kernel.NoLane, func() { reached = true })
	h.W.RunUntil(func() bool { return reached })
}

// RunFor is RunTo(Now()+d).
func (h *Harness) RunFor(d machine.Duration) { h.RunTo(h.Now() + kernel.Time(d)) }

// Step takes exactly one queue entry. It reports false when the queue is empty.
func (h *Harness) Step() bool {
	start := h.W.Steps()
	h.W.RunUntil(func() bool { return h.W.Steps() > start })
	return h.W.Steps() > start
}

// Crash ends the live incarnation of a caller. The services are told.
func (h *Harness) Crash(name string) { h.W.Crash(h.addrs[name], "test") }

// Boot starts a new incarnation of a caller that is down.
func (h *Harness) Boot(name string) { h.W.Boot(h.addrs[name]) }

// Recover moves the world to PhaseRecovering. The kernel then calls EnterRecovery
// on every service: no new faults for requests issued after it.
func (h *Harness) Recover() { h.W.SetPhase(kernel.PhaseRecovering) }

// Faults lists the kinds of the faults that services injected, in order.
func (h *Harness) Faults() []string {
	var kinds []string
	for _, f := range h.W.Timeline().Snapshot("sim_fault") {
		if k, ok := f.Fact.Field("kind").(machine.StringValue); ok {
			kinds = append(kinds, string(k))
		}
	}
	return kinds
}

// Hit reports whether the assertion with that name passed at least once (a
// Reachable that was reached, a Sometimes that was true).
func (h *Harness) Hit(name string) bool {
	c, ok := h.W.Assert().Coverage()[name]
	return ok && c.Passes > 0
}

// Find returns the completion that was delivered for request (nil if none).
func (h *Harness) Find(request machine.RequestID) core.IOResult {
	for _, d := range h.Delivered {
		if r, ok := d.Body.(core.IOResult); ok && r.RequestID() == request {
			return r
		}
	}
	return nil
}

// Trace renders the deliveries as text, for equality checks.
func (h *Harness) Trace() string {
	var s strings.Builder
	for _, d := range h.Delivered {
		fmt.Fprintf(&s, "%d %s %#v\n", d.At, d.To, d.Body)
	}
	return s.String()
}
