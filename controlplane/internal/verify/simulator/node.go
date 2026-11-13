package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// simNode is one replica of the simulation. It lives for the whole run. It is a
// node of the dst kernel: each boot gives it a new machine and a new boot
// number, so nothing of the old boot reaches the new one. The route function
// turns the actions that Step returns into kernel calls: timers, calls to the
// storage service, replies to clients. It can crash, pause and restart.
type simNode struct {
	s       *sim
	idx     int
	id      machine.NodeID
	factory replica.Factory
	addr    kernel.Addr[runtime.EventData]
	m       *replica.Replica // the machine of the last boot (see replica)

	paused bool // the host paused the node (the kernel holds its inputs)
	cut    bool // cut off from the log and the object store

	boots int

	// groups hosts the rebalance machine of the node and its group calls
	// (groups.go). It is nil in a run with no consumer groups.
	groups *groupHost
}

func nodeID(idx int) machine.NodeID { return machine.NodeID(fmt.Sprintf("cp-%d", idx)) }

// newNode spawns the node in the world of s. The node is down until boot.
func newNode(s *sim, idx int, factory replica.Factory) *simNode {
	n := &simNode{s: s, idx: idx, id: nodeID(idx), factory: factory}
	n.addr = kernel.Spawn(s.w, kernel.NodeSpec[runtime.EventData, runtime.Action]{
		Name:      n.id,
		New:       n.newMachine,
		Route:     n.route,
		Boot:      func(self machine.NodeInstance) runtime.EventData { return runtime.NodeStarted{Self: self} },
		AfterStep: n.afterStep,
	})
	return n
}

func (n *simNode) name() string { return string(n.id) }

// up reports that the node has an incarnation that runs.
func (n *simNode) up() bool { return n.s.w.Up(n.addr) }

// replica is the machine, or nil while the node is down.
func (n *simNode) replica() *replica.Replica {
	if !n.up() {
		return nil
	}
	return n.m
}

// newMachine is the factory of the kernel node: it runs at each boot.
func (n *simNode) newMachine(env machine.Env, self machine.NodeInstance) machine.Machine[runtime.EventData, runtime.Action] {
	n.boots++
	n.paused = false
	n.m = n.factory(env, self)
	n.s.noteBoot(n)
	return n.m
}

// boot starts a new incarnation of the node and of its rebalance machine,
// unless the node is up.
func (n *simNode) boot() {
	if n.up() {
		return
	}
	n.s.w.Boot(n.addr)
	if n.groups != nil {
		n.s.w.Boot(n.groups.addr)
	}
}

// down notes that the incarnation is gone. Events that are on their way to it
// are dropped by the kernel when they arrive.
func (n *simNode) down(why string) {
	n.s.stats.Crashes++
	n.s.tracef("%s crashes (%s)", n.name(), why)
	n.paused = false
	if n.groups != nil {
		n.groups.stop()
	}
}

// crashGroups crashes the rebalance machine of the node, when it has one.
func (n *simNode) crashGroups(cause string) {
	if n.groups != nil {
		n.s.w.Crash(n.groups.addr, cause)
	}
}

// crash kills the node (the nemesis) and starts it again after d.
func (n *simNode) crash(why string, d machine.Duration) {
	if !n.up() {
		return
	}
	n.down(why)
	n.s.w.Crash(n.addr, why)
	n.crashGroups(why)
	n.s.w.After(d, n.boot)
}

// pause stops the node from taking events. They wait in its inbox and run in
// order when it resumes.
func (n *simNode) pause() {
	if n.up() && !n.paused {
		n.s.stats.Pauses++
		n.paused = true
		n.s.w.Pause(n.addr)
		if n.groups != nil {
			n.s.w.Pause(n.groups.addr)
		}
	}
}

func (n *simNode) resume() {
	if !n.paused {
		return
	}
	n.paused = false
	n.s.w.Resume(n.addr)
	if n.groups != nil {
		n.s.w.Resume(n.groups.addr)
	}
}

// deliverNow gives body to whatever incarnation is up (a client request).
func (n *simNode) deliverNow(body runtime.EventData) {
	if n.up() {
		kernel.Inject(n.s.w, n.addr, 0, body)
	}
}

// route handles one output of the replica. It is a copy of routeReplica of
// the server (controlplane/internal/controlplane/server), with the storage
// service in place of the executor. Every case of runtime.Action is listed, so
// a new action without a case does not compile.
func (n *simNode) route(r *kernel.Router[runtime.EventData], out runtime.Action) {
	store := n.s.storeAddr
	switch a := out.(type) {
	case runtime.SetTimer:
		r.SetTimer(a.ID, a.After, runtime.TimerFired{ID: a.ID, Gen: a.Gen})
	case runtime.CancelTimer:
		r.CancelTimer(a.ID)
	case runtime.StopForRestart:
		n.tripped(r, a.Cause)
	case runtime.ShutdownDone:
		n.shutDown(r)
	case runtime.ControlPlaneReply:
		n.reply(r, a)
	case runtime.ObjectPut:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.ObjectGet:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.ObjectDelete:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.ObjectList:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.LogAppend:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.LogRead:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.LogGetEnd:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.LogDeleteOld:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.LogClaimEpoch:
		kernel.Call(r, store, runtime.IOCall(a))
	case runtime.ControlPlaneCall:
		n.s.violation(knownAction, "node %s returned %T", n.name(), a)
	}
}

// tripped is the replica asking to be stopped for a restart. The rebalance
// machine goes down with it, and both start again after a random time.
func (n *simNode) tripped(r *kernel.Router[runtime.EventData], cause string) {
	d := machine.Duration(n.s.rand.Between(int64(100*machine.Millisecond), int64(500*machine.Millisecond)))
	n.s.stats.Trips++
	n.down("stop for restart: " + cause)
	n.crashGroups("the replica stopped")
	r.End(machine.Trip, cause)
	n.s.w.After(d, n.boot)
}

// shutDown is the replica ending on purpose. The node stays down until the
// quiet tail boots it again.
func (n *simNode) shutDown(r *kernel.Router[runtime.EventData]) {
	n.down("shutdown")
	n.crashGroups("the replica stopped")
	r.End(machine.Exit, "shutdown")
}

// reply handles a ControlPlaneReply of the replica. A reply to the rebalance
// machine goes to it (as Node.reply does in production), a reply to a client
// goes to the client pool.
func (n *simNode) reply(r *kernel.Router[runtime.EventData], a runtime.ControlPlaneReply) {
	if n.groups != nil && a.To == rebalanceFrom() {
		n.groups.replied(r, a.Done)
		return
	}
	n.s.clients.reply(n, a)
}

// afterStep runs after each Step of the replica and its outputs. It checks the
// properties of one node, and tells the rebalance machine when its role changes.
func (n *simNode) afterStep(self kernel.Ref[runtime.EventData], _ runtime.EventData, _ []runtime.Action) {
	n.s.checkNode(n)
	if n.groups != nil && n.s.w.Up(self) {
		n.groups.afterStep()
	}
}
