// Package kv shows how to run a tiny key-value store on the dst v2 kernel.
//
// There are two nodes and one service:
//
//   - the client node sends puts and gets to the server node, with random
//     gaps between them, and checks every read;
//   - the server node turns each request into a call to the store;
//   - the store service keeps the data and answers each get after a random
//     delay. The server forwards the answer to the client.
//
// Each node has its own input and output sum types. Each route function is one
// exhaustive type switch over the outputs of its node.
package kv

import (
	"maps"
	"slices"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

var (
	putSticks   = assert.NewAlways("kv v2: put sticks")
	readMatches = assert.NewAlways("kv v2: read matches")
)

// Run runs one seed: one client, one server node and one store.
func Run(seed uint64) kernel.Result { return RunWith(kernel.Config{Seed: seed}) }

// RunWith is Run with a kernel config. A campaign uses it to switch on the
// step log or HaltOnFirst. These do not change the hash of a passing seed.
func RunWith(cfg kernel.Config) kernel.Result {
	w := kernel.New(cfg)
	store := kernel.AddService[storeReq, serverIn](w, &store{data: map[string]string{}})
	var server kernel.Addr[serverIn]
	var client kernel.Addr[clientIn]
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
			return &clientMachine{env: env, script: []op{putOp("a", "1"), putOp("a", "2"), getOp("a", "2")}}
		},
		Route: func(r *kernel.Router[clientIn], out clientOut) {
			switch x := out.(type) {
			case sendPut:
				kernel.Send(r, server, serverIn(putRequest(x)))
			case sendGet:
				kernel.Send(r, server, serverIn(getRequest(x)))
			case waitNext:
				r.SetTimer(x.ID, x.After, nextOp{})
			}
		},
		Boot: func(machine.NodeInstance) clientIn { return begin{} },
	})
	w.RunUntil(nil)
	return w.Finish()
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
	begin       struct{} // the boot input
	nextOp      struct{} // the timer between two operations
	getResponse struct{ Key, Value, Want string }
)

func (begin) isClientIn()       {}
func (nextOp) isClientIn()      {}
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
	sendPut  struct{ Key, Value string }
	sendGet  struct{ Key, Want string }
	waitNext struct { // arm the timer for the next operation
		ID    machine.TimerID
		After machine.Duration
	}
)

func (sendPut) isClientOut()  {}
func (sendGet) isClientOut()  {}
func (waitNext) isClientOut() {}

type opKind uint8

const (
	kindPut opKind = iota
	kindGet
)

type op struct {
	kind       opKind
	key, value string
	want       string
}

func putOp(key, value string) op { return op{kind: kindPut, key: key, value: value} }
func getOp(key, want string) op  { return op{kind: kindGet, key: key, want: want} }

// clientMachine runs its script, one operation after the other.
type clientMachine struct {
	env    machine.Env
	script []op
	pc     int
}

func (c *clientMachine) StateHash() uint64 { return uint64(c.pc) }

func (c *clientMachine) Step(_ machine.Now, in clientIn) []clientOut {
	switch e := in.(type) {
	case begin, nextOp:
		return c.issue()
	case getResponse:
		c.env.Always(e.Value == e.Want, readMatches,
			assert.Pair{Key: "k", Value: e.Key},
			assert.Pair{Key: "got", Value: e.Value},
			assert.Pair{Key: "want", Value: e.Want},
		)
	}
	return nil
}

func (c *clientMachine) issue() []clientOut {
	if c.pc >= len(c.script) {
		return nil
	}
	cur := c.script[c.pc]
	c.pc++
	var out []clientOut
	switch cur.kind {
	case kindPut:
		out = append(out, sendPut{Key: cur.key, Value: cur.value})
	case kindGet:
		out = append(out, sendGet{Key: cur.key, Want: cur.want})
	}
	if c.pc < len(c.script) {
		gap := machine.Duration(c.env.Between(1, 20)) * machine.Millisecond
		out = append(out, waitNext{ID: machine.TimerID(c.pc), After: gap})
	}
	return out
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

// storeReq is every request of the store service. Its cases are also outputs
// of the server node, so the route passes them on as they are.
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

// store is the key-value service.
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
		s.data[a.Key] = a.Value
		cx.Assert().Always(s.data[a.Key] == a.Value, putSticks, assert.Pair{Key: "k", Value: a.Key})
	case storeGet:
		delay := machine.Duration(cx.Rand().Between(1, 5)) * machine.Millisecond
		cx.Deliver(from, cx.Now()+kernel.Time(delay), kernel.NoLane, serverIn(getDone{Key: a.Key, Value: s.data[a.Key], Want: a.Want}))
	}
}
