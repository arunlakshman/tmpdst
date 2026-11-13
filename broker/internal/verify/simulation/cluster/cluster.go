// Package cluster is the host of one broker simulation run on dst/kernel. It
// builds the world of a seed: the three fake outside systems (object store,
// control plane, network), the broker nodes, the workload node (a test client),
// the phases, the node attrition and the quiesce task. A scenario (kafka, echo)
// gives it the machines and the rules.
//
// The broker machines and the workloads are core machines: they take one core.Event
// and return core actions. The kernel runs machines with the dst v2 contract
// (machine.Machine), so this package wraps each core machine in a small adapter
// (adapter.go). The route function (route.go) sends each core action to the
// kernel or to the right fake service. It is one exhaustive type switch.
package cluster

import (
	"fmt"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/fakecontrolplane"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/fakenetwork"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/fakeobjectstore"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/internal/span"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// wallStart is the virtual UTC time at the start of every run (2023-11-14).
const wallStart machine.RealTime = 1_700_000_000 * 1_000_000_000

// tailLines is how many of the last step lines a failed run keeps.
const tailLines = 48

// Settings are the knobs of one run. A scenario starts from the defaults of
// internal/preset, and its swarm changes them for each seed.
type Settings struct {
	ObjectStore  fakeobjectstore.Faults
	ControlPlane fakecontrolplane.Faults
	Network      fakenetwork.Faults
	// ClockSkewMax is the largest fixed clock skew of one node. It is drawn once
	// for each node when the node is spawned.
	ClockSkewMax machine.Duration
	// Attrition kills and restarts broker nodes during the chaos window.
	Attrition kernel.Attrition
	// RecoveryDelay is the time from the end of a broker node to its next boot.
	RecoveryDelay [2]machine.Duration
	// Cost is the CPU model of every node: how long a node is busy after a Step.
	Cost kernel.CostParams
}

// Plan says how to build one run.
type Plan struct {
	Seed     uint64
	Settings Settings
	Phases   kernel.Phases
	// QuiesceEvery is the time between two quiesce points. Zero: no quiesce point.
	QuiesceEvery machine.Duration
	// MaxSteps is the step limit of the run. Zero: the kernel default.
	MaxSteps uint64
	// Log keeps every step line. HaltOnFirst stops the run at the first failure.
	// VerifyHashes recomputes every cached state hash at each step.
	Log, HaltOnFirst, VerifyHashes bool
}

// Workload is a test client. It is a machine that records every operation in a
// history and does not judge. It never ends and it boots after every broker.
type Workload interface {
	core.Machine
	// OnPhase tells the workload that the phase changed (into Recovering and into
	// Recover). The returned actions are routed like the actions of a Step.
	OnPhase(p kernel.Phase) []core.Action
	// Quiescent reports that no operation is in flight.
	Quiescent() bool
}

// WorkloadFactory builds a fresh workload for a boot. brokers are the names of the
// broker nodes, in order.
type WorkloadFactory func(env core.Environment, self machine.NodeInstance, brokers []machine.NodeID) Workload

// BrokerAddr is the address of a node that takes core events.
type BrokerAddr = kernel.Addr[core.EventData]

// Cluster is the world of one run and the handles a scenario needs.
type Cluster struct {
	// World is the kernel world. A scenario adds its rules to it.
	World *kernel.World
	// Objects, ControlPlane and Network are the fake services. A rule reads the
	// ground truth from the first two (see package truth).
	Objects      *fakeobjectstore.Engine
	ControlPlane *fakecontrolplane.Engine
	Network      *fakenetwork.Engine

	plan      Plan
	svc       services
	brokers   []BrokerAddr
	brokerIDs []machine.NodeID
	workloads []*workloadSlot
}

// workloadSlot remembers the workload of the last boot of one workload node.
type workloadSlot struct {
	live Workload
}

// New builds the world, the three services and nothing else. Add the brokers,
// then the workload, then the rules, then call Run.
func New(p Plan) *Cluster {
	w := kernel.New(kernel.Config{
		Seed: p.Seed, WallStart: wallStart, MaxSteps: p.MaxSteps,
		ClockSkewMax: p.Settings.ClockSkewMax, Attrition: p.Settings.Attrition,
		Log: p.Log, Tail: tailLines, HaltOnFirst: p.HaltOnFirst, VerifyHashes: p.VerifyHashes,
	})
	c := &Cluster{World: w, plan: p}
	c.Objects = fakeobjectstore.New(w, p.Settings.ObjectStore)
	c.ControlPlane = fakecontrolplane.New(w, p.Settings.ControlPlane)
	c.Network = fakenetwork.New(w, p.Settings.Network)
	c.svc = services{
		world:        w,
		objects:      kernel.AddService[core.ObjectCall, core.EventData](w, c.Objects),
		controlPlane: kernel.AddService[core.ControlPlaneCall, core.EventData](w, c.ControlPlane),
		network:      kernel.AddService[fakenetwork.Call, core.EventData](w, c.Network),
	}
	return c
}

// restart is the restart rule of a broker node: every end (crash, trip or exit)
// is followed by a boot after a delay in RecoveryDelay.
func (c *Cluster) restart(_ machine.End, r *rng.Rand) (machine.Duration, bool) {
	d := c.plan.Settings.RecoveryDelay
	return machine.Duration(span.Between(r, int64(d[0]), int64(d[1]))), true
}

// AddBrokers spawns n broker nodes named broker-0, broker-1, ... They can be
// dialed and the kernel may kill them. factory builds the machine of each boot.
func (c *Cluster) AddBrokers(n int, factory core.MachineFactory) {
	for i := range n {
		name := machine.NodeID(fmt.Sprintf("broker-%d", i))
		addr := kernel.Spawn(c.World, kernel.NodeSpec[core.EventData, core.Action]{
			Name:      name,
			New:       adaptFactory(factory),
			Route:     c.svc.route,
			Boot:      bootEvent,
			StopMsg:   stopEvent,
			Class:     core.InboxKindOf,
			Cost:      c.plan.Settings.Cost,
			Restart:   c.restart,
			Attrition: true,
		})
		c.Network.AddNode(addr)
		c.brokers = append(c.brokers, addr)
		c.brokerIDs = append(c.brokerIDs, name)
	}
}

// AddWorkload spawns a workload node named workload-0, workload-1, ... Add the
// brokers first: the workload gets their names.
func (c *Cluster) AddWorkload(factory WorkloadFactory) {
	slot := &workloadSlot{}
	c.workloads = append(c.workloads, slot)
	name := machine.NodeID(fmt.Sprintf("workload-%d", len(c.workloads)-1))
	addr := kernel.Spawn(c.World, kernel.NodeSpec[core.EventData, core.Action]{
		Name: name,
		New: func(env machine.Env, self machine.NodeInstance) machine.Machine[core.EventData, core.Action] {
			wl := factory(env, self, c.brokerIDs)
			if wl == nil {
				return nil
			}
			slot.live = wl
			return driverAdapter{wl}
		},
		Route:   c.svc.route,
		Boot:    bootEvent,
		Class:   core.InboxKindOf,
		Cost:    c.plan.Settings.Cost,
		Driver:  true,
		OnPhase: phaseEvent,
	})
	c.Network.AddNode(addr)
}

// quiesce appends a "sim_quiesce" fact that says whether no workload has an
// operation in flight, then queues the next one.
func (c *Cluster) quiesce() {
	q := true
	for _, s := range c.workloads {
		if s.live != nil && !s.live.Quiescent() {
			q = false
		}
	}
	c.World.Fact(machine.Fact{Name: "sim_quiesce", Fields: []machine.Field{{Key: "quiescent", Value: machine.Bool(q)}}})
	c.World.At(c.World.Now()+kernel.Time(c.plan.QuiesceEvery), kernel.HostLane, c.quiesce)
}

// Run schedules the phases and the quiesce points, boots the nodes, runs the
// world to the end and returns the result.
func (c *Cluster) Run() kernel.Result {
	kernel.SchedulePhases(c.World, c.plan.Phases)
	if c.plan.QuiesceEvery > 0 {
		c.World.At(kernel.Time(c.plan.QuiesceEvery), kernel.HostLane, c.quiesce)
	}
	c.World.Start()
	c.World.RunUntil(nil)
	return c.World.Finish()
}
