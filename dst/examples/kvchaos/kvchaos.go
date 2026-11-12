// Package kvchaos exercises MaybeApplied puts, write drops, and Crash+Restart.
package kvchaos

import (
	"maps"
	"slices"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

const writeDropRate = 0.25

// Result is the fingerprint of one run.
type Result struct {
	Hash     uint64
	Calls    uint64
	Steps    int
	Failures []sim.Failure
	Crashed  bool // node Crash+Restart was scheduled this seed
	Done     bool // client confirmed every goal
}

// Run drives a client that treats puts as MaybeApplied, a store that may drop
// writes, and a supervisor that Crash+Restarts the node on some seeds.
func Run(seed uint64) (Result, error) {
	r := sim.New(sim.Config{Seed: seed})
	store := newStore("kv")
	node := newNode("n1")
	client := newClient("c1", "n1", []goal{
		{key: "a", value: "1"},
		{key: "a", value: "2"},
		{key: "b", value: "9"},
	})
	sup := newSupervisor("sup", "n1")
	r.Register(store)
	r.Spawn(node)
	r.Spawn(client)
	r.Spawn(sup)
	r.Inject(client.id, 0, begin{})
	r.Inject(sup.id, 0, begin{})
	res, err := r.Run()
	return Result{
		Hash:     res.Hash,
		Calls:    res.Calls,
		Steps:    res.Steps,
		Failures: res.Failures,
		Crashed:  sup.planned,
		Done:     client.gi >= len(client.goals),
	}, err
}

type begin struct{}
type retry struct{}
type killReq struct{}
type bootMsg struct{}

type putRequest struct{ Key, Value string }

type getRequest struct {
	Key, Want string
	ReplyTo   eng.ActorID
}

type getResponse struct {
	Key, Value, Want string
}

type getProbe struct {
	key, want string
	gen       int
}

type putAction struct{ Key, Value string }
type getAction struct {
	Key, Want string
	ReplyTo   eng.ActorID
}

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
		// MaybeApplied: a dropped write leaves the prior value. Client confirms via get.
		if cx.Faults().Fault("kv.write", writeDropRate) {
			cx.Assert().Sometimes(true, "write dropped")
			return nil
		}
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
	case killReq:
		return []any{sim.Crash{}}
	case bootMsg:
		return nil
	default:
		return nil
	}
}

type supervisor struct {
	id, node sim.ProcessID
	planned  bool
}

func newSupervisor(id, nodeID sim.ProcessID) *supervisor {
	return &supervisor{id: id, node: nodeID}
}

func (s *supervisor) ID() sim.ProcessID { return s.id }
func (s *supervisor) StateHash() uint64 {
	if s.planned {
		return 1
	}
	return 0
}

func (s *supervisor) Step(cx sim.Context, inbox any) []any {
	switch inbox.(type) {
	case begin:
		// Latch ~half of seeds; fire always when active -> Crash+Restart on those seeds.
		if cx.Faults().MaybeP("node-crash", 1.0) {
			s.planned = true
			killAt := sched.TimeNs(cx.Rand().Between(5, 40) * 1_000_000)
			return []any{sim.SetTimer{ID: 1, After: killAt, Body: killReq{}}}
		}
		return nil
	case killReq:
		restartAt := sched.TimeNs(cx.Rand().Between(5, 20) * 1_000_000)
		return []any{
			sim.Send{To: s.node, Body: killReq{}},
			sim.SetTimer{ID: 2, After: restartAt, Body: bootMsg{}},
		}
	case bootMsg:
		return []any{sim.Restart{Who: s.node, Boot: bootMsg{}}}
	default:
		return nil
	}
}

type goal struct {
	key, value string
}

type client struct {
	id, node sim.ProcessID
	goals    []goal
	gi       int
	attempts int
}

func newClient(id, nodeID sim.ProcessID, goals []goal) *client {
	return &client{id: id, node: nodeID, goals: goals}
}

func (c *client) ID() sim.ProcessID { return c.id }
func (c *client) StateHash() uint64 { return uint64(c.gi*1000 + c.attempts) }

func (c *client) Step(cx sim.Context, inbox any) []any {
	switch e := inbox.(type) {
	case begin, retry:
		return c.issue(cx)
	case getProbe:
		if e.gen != c.gi {
			return nil
		}
		return []any{sim.Send{To: c.node, Body: getRequest{
			Key: e.key, Want: e.want, ReplyTo: eng.ActorID(c.id),
		}}}
	case getResponse:
		if c.gi >= len(c.goals) {
			return nil
		}
		g := c.goals[c.gi]
		if e.Key != g.key || e.Want != g.value {
			return nil
		}
		if e.Value == e.Want {
			c.gi++
			cx.Assert().Reachable("goal confirmed")
			return c.issue(cx)
		}
		cx.Assert().Sometimes(true, "put uncertain")
		gap := sched.TimeNs(cx.Rand().Between(1, 10) * 1_000_000)
		return []any{sim.SetTimer{ID: uint64(1000 + c.gi), After: gap, Body: retry{}}}
	default:
		return nil
	}
}

func (c *client) issue(cx sim.Context) []any {
	if c.gi >= len(c.goals) {
		return nil
	}
	g := c.goals[c.gi]
	c.attempts++
	probeAt := sched.TimeNs(cx.Rand().Between(1, 5) * 1_000_000)
	// Watchdog: if the get was lost (node crash window), retry the MaybeApplied put.
	watchAt := probeAt + sched.TimeNs(cx.Rand().Between(10, 30)*1_000_000)
	return []any{
		sim.Send{To: c.node, Body: putRequest{Key: g.key, Value: g.value}},
		sim.SetTimer{ID: uint64(c.gi + 1), After: probeAt, Body: getProbe{key: g.key, want: g.value, gen: c.gi}},
		sim.SetTimer{ID: uint64(5000 + c.attempts), After: watchAt, Body: retry{}},
	}
}
