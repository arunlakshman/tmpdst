// Package kvchaos runs the key-value store of package kv with faults, on the
// dst v2 kernel:
//
//   - the store may drop a write (the put is "maybe applied"), so the client
//     confirms each put with a get and retries when the value is not there;
//   - on about half of the seeds, host code crashes the server node and boots
//     it again later (a supervisor). Requests and answers of the dead
//     incarnation are lost, so the client also has a watchdog timer.
//
// The supervisor is host code: a few host tasks (World.After) that call
// World.Crash and World.Boot.
package kvchaos

import (
	"maps"
	"slices"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

const writeDropRate = 0.25

var (
	putSticks     = assert.NewAlways("kvchaos v2: put sticks")
	writeDropped  = assert.NewSometimes("kvchaos v2: write dropped")
	putUncertain  = assert.NewSometimes("kvchaos v2: put uncertain")
	goalConfirmed = assert.NewReachable("kvchaos v2: goal confirmed")

	nodeCrash = fault.NewSite("kvchaos v2: node crash")
)

// Result is one run, plus two facts about it.
type Result struct {
	kernel.Result
	Crashed bool // the supervisor crashed and restarted the server node
	Done    bool // the client confirmed every goal
}

// Run runs one seed.
func Run(seed uint64) Result {
	w := kernel.New(kernel.Config{Seed: seed})
	store := kernel.AddService[storeReq, serverIn](w, &store{data: map[string]string{}})
	var server kernel.Addr[serverIn]
	var client kernel.Addr[clientIn]
	var c *clientMachine
	server = kernel.Spawn(w, kernel.NodeSpec[serverIn, serverOut]{
		Name: "n1",
		New: func(machine.Env, machine.NodeInstance) machine.Machine[serverIn, serverOut] {
			return serverMachine{}
		},
		Route: func(r *kernel.Router[serverIn], out serverOut) {
			switch x := out.(type) {
			case storePut:
				kernel.Call(r, store, storeReq(x))
			case storeGet:
				kernel.Call(r, store, storeReq(x))
			case reply:
				kernel.Send(r, client, clientIn(getResponse(x)))
			}
		},
	})
	client = kernel.Spawn(w, kernel.NodeSpec[clientIn, clientOut]{
		Name: "c1",
		New: func(env machine.Env, _ machine.NodeInstance) machine.Machine[clientIn, clientOut] {
			c = &clientMachine{env: env, goals: []goal{{"a", "1"}, {"a", "2"}, {"b", "9"}}}
			return c
		},
		Route: func(r *kernel.Router[clientIn], out clientOut) {
			switch x := out.(type) {
			case sendPut:
				kernel.Send(r, server, serverIn(putRequest(x)))
			case sendGet:
				kernel.Send(r, server, serverIn(getRequest(x)))
			case arm:
				r.SetTimer(x.ID, x.After, x.Msg)
			}
		},
		Boot: func(machine.NodeInstance) clientIn { return begin{} },
	})
	crashed := supervise(w, server)
	w.RunUntil(nil)
	return Result{Result: w.Finish(), Crashed: *crashed, Done: c != nil && c.gi >= len(c.goals)}
}

// supervise adds the supervisor: on about half of the seeds it crashes the
// server node 5..40 ms after the start and boots it again 5..20 ms later. The
// result tells whether it did.
func supervise(w *kernel.World, server kernel.Addr[serverIn]) *bool {
	planned := new(bool)
	w.After(0, func() {
		// Switched on for about half of the seeds; fires every time when on.
		if !w.Env().MaybeP(nodeCrash, 1.0) {
			return
		}
		*planned = true
		killAt := machine.Duration(w.Rand().Between(5, 40)) * machine.Millisecond
		w.After(killAt, func() {
			w.Crash(server, "supervisor")
			restartAt := machine.Duration(w.Rand().Between(5, 20)) * machine.Millisecond
			w.After(restartAt, func() { w.Boot(server) })
		})
	})
	return planned
}

// ---- client ----

// clientIn is every input of the client.
//
//sumtype:decl
type clientIn interface {
	machine.Message
	Tag() uint64
	isClientIn()
}

type (
	begin    struct{} // the boot input
	retry    struct{} // the watchdog, or the gap before the next try
	getProbe struct { // time to read back the value of a goal
		key, want string
		gen       int
	}
	getResponse struct{ Key, Value, Want string }
)

func (begin) isClientIn()       {}
func (retry) isClientIn()       {}
func (getProbe) isClientIn()    {}
func (getResponse) isClientIn() {}

// clientOut is every output of the client.
//
//sumtype:decl
type clientOut interface {
	machine.Message
	Tag() uint64
	isClientOut()
}

type (
	sendPut struct{ Key, Value string }
	sendGet struct{ Key, Want string }
	arm     struct { // set a timer of the client
		ID    machine.TimerID
		After machine.Duration
		Msg   clientIn
	}
)

func (sendPut) isClientOut() {}
func (sendGet) isClientOut() {}
func (arm) isClientOut()     {}

type goal struct{ key, value string }

// clientMachine writes each goal, reads it back, and writes again until the
// read shows the value.
type clientMachine struct {
	env      machine.Env
	goals    []goal
	gi       int // the goal being worked on
	attempts int
}

func (c *clientMachine) StateHash() uint64 { return uint64(c.gi*1000 + c.attempts) }

func (c *clientMachine) Step(_ machine.Now, in clientIn) []clientOut {
	switch e := in.(type) {
	case begin, retry:
		return c.issue()
	case getProbe:
		if e.gen != c.gi {
			return nil
		}
		return []clientOut{sendGet{Key: e.key, Want: e.want}}
	case getResponse:
		return c.onResponse(e)
	}
	return nil
}

func (c *clientMachine) onResponse(e getResponse) []clientOut {
	if c.gi >= len(c.goals) {
		return nil
	}
	g := c.goals[c.gi]
	if e.Key != g.key || e.Want != g.value {
		return nil
	}
	if e.Value == e.Want {
		c.gi++
		c.env.Reachable(goalConfirmed)
		return c.issue()
	}
	c.env.Sometimes(true, putUncertain)
	gap := machine.Duration(c.env.Between(1, 10)) * machine.Millisecond
	return []clientOut{arm{ID: machine.TimerID(1000 + c.gi), After: gap, Msg: retry{}}}
}

func (c *clientMachine) issue() []clientOut {
	if c.gi >= len(c.goals) {
		return nil
	}
	g := c.goals[c.gi]
	c.attempts++
	probeAt := machine.Duration(c.env.Between(1, 5)) * machine.Millisecond
	// Watchdog: if the get was lost (the node crashed), write again.
	watchAt := probeAt + machine.Duration(c.env.Between(10, 30))*machine.Millisecond
	return []clientOut{
		sendPut{Key: g.key, Value: g.value},
		arm{ID: machine.TimerID(c.gi + 1), After: probeAt, Msg: getProbe{key: g.key, want: g.value, gen: c.gi}},
		arm{ID: machine.TimerID(5000 + c.attempts), After: watchAt, Msg: retry{}},
	}
}

// ---- server ----

// serverIn is every input of the server node.
//
//sumtype:decl
type serverIn interface {
	machine.Message
	Tag() uint64
	isServerIn()
}

type (
	putRequest struct{ Key, Value string }
	getRequest struct{ Key, Want string }
	getDone    struct{ Key, Value, Want string } // the store answer
)

func (putRequest) isServerIn() {}
func (getRequest) isServerIn() {}
func (getDone) isServerIn()    {}

// serverOut is every output of the server node.
//
//sumtype:decl
type serverOut interface {
	machine.Message
	Tag() uint64
	isServerOut()
}

type reply struct{ Key, Value, Want string } // forward a store answer to the client

func (storePut) isServerOut() {}
func (storeGet) isServerOut() {}
func (reply) isServerOut()    {}

// serverMachine has no state: it turns requests into store calls and store
// answers into replies.
type serverMachine struct{}

func (serverMachine) StateHash() uint64 { return 0 }

func (serverMachine) Step(_ machine.Now, in serverIn) []serverOut {
	switch e := in.(type) {
	case putRequest:
		return []serverOut{storePut(e)}
	case getRequest:
		return []serverOut{storeGet(e)}
	case getDone:
		return []serverOut{reply(e)}
	}
	return nil
}

// ---- store service ----

// storeReq is every request of the store service.
//
//sumtype:decl
type storeReq interface {
	machine.Message
	Tag() uint64
	isStoreReq()
}

type (
	storePut struct{ Key, Value string }
	storeGet struct{ Key, Want string }
)

func (storePut) isStoreReq() {}
func (storeGet) isStoreReq() {}

// store is the key-value service. It may drop a write.
type store struct{ data map[string]string }

func (s *store) Name() string                                                   { return "kv" }
func (s *store) EnterRecovery()                                                 {}
func (s *store) OnCrash(*kernel.ServiceContext[serverIn], machine.NodeInstance) {}

func (s *store) StateHash() uint64 {
	h := trace.New()
	for _, k := range slices.Sorted(maps.Keys(s.data)) {
		h.String(k)
		h.String(s.data[k])
	}
	return h.Sum()
}

func (s *store) Handle(cx *kernel.ServiceContext[serverIn], from kernel.Ref[serverIn], req storeReq) {
	switch a := req.(type) {
	case storePut:
		// A dropped write leaves the old value. The client finds out with a get.
		if cx.Rand().Chance(writeDropRate) {
			cx.Fault("write_dropped")
			cx.Assert().Sometimes(true, writeDropped)
			return
		}
		s.data[a.Key] = a.Value
		cx.Assert().Always(s.data[a.Key] == a.Value, putSticks, assert.Pair{Key: "k", Value: a.Key})
	case storeGet:
		delay := machine.Duration(cx.Rand().Between(1, 5)) * machine.Millisecond
		cx.Deliver(from, cx.Now()+kernel.Time(delay), kernel.NoLane, serverIn(getDone{Key: a.Key, Value: s.data[a.Key], Want: a.Want}))
	}
}
