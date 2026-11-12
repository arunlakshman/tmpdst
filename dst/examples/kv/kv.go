// Package kv shows how to implement eng.Engine and sim.Process for a tiny store.
package kv

import (
	"maps"
	"slices"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

// Result is the fingerprint of one run.
type Result struct {
	Hash     uint64
	Calls    uint64
	Steps    int
	Failures []sim.Failure
}

// Run drives one client, one node, and one store under a seeded Runner.
func Run(seed uint64) (Result, error) {
	r := sim.New(sim.Config{Seed: seed})
	store := newStore("kv")
	node := newNode("n1")
	client := newClient("c1", "n1", []op{
		putOp("a", "1"),
		putOp("a", "2"),
		getOp("a", "2"),
	})
	r.Register(store)
	r.Spawn(node)
	r.Spawn(client)
	r.Inject(client.id, 0, begin{})
	res, err := r.Run()
	return Result{
		Hash:     res.Hash,
		Calls:    res.Calls,
		Steps:    res.Steps,
		Failures: res.Failures,
	}, err
}

// ---- messages ----

type begin struct{}
type nextOp struct{}

type putRequest struct{ Key, Value string }

type getRequest struct {
	Key, Want string
	ReplyTo   eng.ActorID
}

type getResponse struct {
	Key, Value, Want string
}

// ---- engine actions ----

type putAction struct{ Key, Value string }
type getAction struct {
	Key, Want string
	ReplyTo   eng.ActorID
}

// ---- store (eng.Engine) ----

type store struct {
	id   eng.ID
	data map[string]string
}

func newStore(id eng.ID) *store {
	return &store{id: id, data: map[string]string{}}
}

func (s *store) ID() eng.ID              { return s.id }
func (s *store) OnSelf(eng.Context, any) {}

func (s *store) StateHash() uint64 {
	h := trace.New()
	for _, k := range slices.Sorted(maps.Keys(s.data)) {
		h.String(k)
		h.String(s.data[k])
	}
	return h.Sum()
}

func (s *store) Handle(cx eng.Context, act any) error {
	switch a := act.(type) {
	case putAction:
		// Happy-path campaign keeps writes reliable. Wire cx.Faults().Fault here
		// when you add a chaos script that tracks MaybeApplied outcomes.
		s.data[a.Key] = a.Value
		cx.Assert().Always(s.data[a.Key] == a.Value, "put sticks", assert.Pair{Key: "k", Value: a.Key})
		return nil
	case getAction:
		delay := sched.TimeNs(cx.Rand().Between(1, 5) * 1_000_000)
		cx.Deliver(a.ReplyTo, delay, getResponse{
			Key: a.Key, Value: s.data[a.Key], Want: a.Want,
		})
		return nil
	default:
		return eng.ErrNotMine
	}
}

// ---- node (sim.Process) ----

type node struct {
	id sim.ProcessID
}

func newNode(id sim.ProcessID) *node { return &node{id: id} }

func (n *node) ID() sim.ProcessID { return n.id }
func (n *node) StateHash() uint64 { return 0 }

func (n *node) Step(_ sim.Context, inbox any) []any {
	switch e := inbox.(type) {
	case putRequest:
		return []any{putAction(e)}
	case getRequest:
		return []any{getAction(e)}
	default:
		return nil
	}
}

// ---- client workload ----

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

type client struct {
	id, node sim.ProcessID
	script   []op
	pc       int
}

func newClient(id, nodeID sim.ProcessID, script []op) *client {
	return &client{id: id, node: nodeID, script: script}
}

func (c *client) ID() sim.ProcessID { return c.id }
func (c *client) StateHash() uint64 { return 0 }

func (c *client) Step(cx sim.Context, inbox any) []any {
	switch e := inbox.(type) {
	case begin, nextOp:
		return c.issue(cx)
	case getResponse:
		cx.Assert().Always(e.Value == e.Want, "read matches",
			assert.Pair{Key: "k", Value: e.Key},
			assert.Pair{Key: "got", Value: e.Value},
			assert.Pair{Key: "want", Value: e.Want},
		)
		return nil
	default:
		return nil
	}
}

func (c *client) issue(cx sim.Context) []any {
	if c.pc >= len(c.script) {
		return nil
	}
	cur := c.script[c.pc]
	c.pc++

	var out []any
	switch cur.kind {
	case kindPut:
		out = append(out, sim.Send{To: c.node, Body: putRequest{Key: cur.key, Value: cur.value}})
	case kindGet:
		out = append(out, sim.Send{To: c.node, Body: getRequest{
			Key: cur.key, Want: cur.want, ReplyTo: eng.ActorID(c.id),
		}})
	}
	if c.pc < len(c.script) {
		gap := sched.TimeNs(cx.Rand().Between(1, 20) * 1_000_000)
		out = append(out, sim.SetTimer{ID: uint64(c.pc), After: gap, Body: nextOp{}})
	}
	return out
}
