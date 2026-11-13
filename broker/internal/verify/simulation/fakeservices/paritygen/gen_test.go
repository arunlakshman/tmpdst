//go:build paritygen

package paritygen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/broker/internal/verify/simulator"
	oldcp "github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakecontrolplane"
	oldnet "github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakenetwork"
	oldobj "github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakeobjectstore"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// ---- the adapter: an OLD fake as a service of the new kernel ----

type ref = kernel.Ref[core.EventData]
type cx = kernel.ServiceContext[core.EventData]

// adapter lends the context of the new kernel to an old fake. The old fake
// schedules entries and delivers events by node incarnation (a plain name); the
// adapter finds the typed reference it was told about.
type adapter struct {
	eng   simulator.FakeService
	lanes *kernel.Lanes[string]
	addrs map[machine.NodeID]kernel.Addr[core.EventData]
	refs  map[machine.NodeInstance]ref
}

var _ kernel.Service[core.Action, core.EventData] = (*adapter)(nil)

func (a *adapter) Name() string      { return a.eng.Name() }
func (a *adapter) StateHash() uint64 { return a.eng.StateHash() }
func (a *adapter) EnterRecovery()    { a.eng.EnterRecovery() }

func (a *adapter) Handle(c *cx, from ref, req core.Action) {
	a.refs[from.Instance()] = from
	a.eng.Execute(&oldCtx{a: a, c: c}, from.Instance(), req)
}

func (a *adapter) OnCrash(c *cx, who machine.NodeInstance) {
	a.eng.OnCrash(&oldCtx{a: a, c: c}, who)
}

// oldCtx is simulator.FakeServiceContext (and NodeCurrent) on the new kernel.
type oldCtx struct {
	a *adapter
	c *cx
}

var (
	_ simulator.FakeServiceContext = (*oldCtx)(nil)
	_ simulator.NodeCurrent        = (*oldCtx)(nil)
)

func (o *oldCtx) Now() simulator.Time         { return simulator.Time(o.c.Now()) }
func (o *oldCtx) Rand() simulator.CountedRand { return oldRand{o.c.Rand()} }
func (o *oldCtx) Lane(key string) simulator.DeliveryLane {
	return simulator.DeliveryLane(o.a.lanes.Get(key))
}

func (o *oldCtx) Deliver(to machine.NodeInstance, at simulator.Time, lane simulator.DeliveryLane, body core.EventData) simulator.EntryID {
	r, ok := o.a.refs[to]
	if !ok {
		panic("paritygen: delivery to a node the adapter never saw")
	}
	return simulator.EntryID(o.c.Deliver(r, kernel.Time(at), kernel.Lane(lane), body))
}

func (o *oldCtx) Self(at simulator.Time, lane simulator.DeliveryLane, body any) simulator.EntryID {
	return simulator.EntryID(o.c.Later(kernel.Time(at), kernel.Lane(lane), func(c *cx) {
		o.a.eng.OnInternal(&oldCtx{a: o.a, c: c}, body)
	}))
}

func (o *oldCtx) Cancel(id simulator.EntryID) { o.c.Cancel(kernel.EntryID(id)) }

func (o *oldCtx) Fault(kind string, fields ...machine.Field) { o.c.Fault(kind, fields...) }

func (o *oldCtx) Assert() machine.Assertions { return o.c.Assert() }

func (o *oldCtx) Current(node machine.NodeID) (machine.NodeInstance, bool) {
	addr, known := o.a.addrs[node]
	if !known {
		return machine.NodeInstance{}, false
	}
	r, ok := o.c.Current(addr)
	if !ok {
		return machine.NodeInstance{}, false
	}
	o.a.refs[r.Instance()] = r
	return r.Instance(), true
}

// oldRand is simulator.CountedRand over the stream of the new kernel. Between keeps
// the old meaning: the upper end is left out.
type oldRand struct{ r *rng.Rand }

var _ simulator.CountedRand = oldRand{}

func (o oldRand) Uint64() uint64        { return o.r.Uint64() }
func (o oldRand) Intn(n int) int        { return o.r.Intn(n) }
func (o oldRand) Float64() float64      { return o.r.Float64() }
func (o oldRand) Chance(p float64) bool { return o.r.Chance(p) }
func (o oldRand) Calls() uint64         { return o.r.Calls() }
func (o oldRand) NewID() machine.ID     { var id machine.ID; o.r.Bytes(id[:]); return id }
func (o oldRand) Derive(string) simulator.CountedRand {
	panic("paritygen: Derive is not used by the fakes")
}
func (o oldRand) Reseed(uint64)       { panic("paritygen: Reseed is not used by the fakes") }
func (o oldRand) Fingerprint() uint64 { panic("paritygen: Fingerprint is not used by the fakes") }

func (o oldRand) Between(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + int64(o.r.Intn(int(hi-lo)))
}

// install puts an old fake in a world as one service that takes every action.
func install(h *servicetest.Harness, eng simulator.FakeService) {
	a := &adapter{eng: eng, lanes: kernel.NewLanes[string](h.W), addrs: map[machine.NodeID]kernel.Addr[core.EventData]{}, refs: map[machine.NodeInstance]ref{}}
	for _, n := range []string{"a", "b", "c", "broker-0", "broker-1", "cp-0", "cp-1"} {
		if addr := h.Addr(n); addr != (kernel.Addr[core.EventData]{}) {
			a.addrs[machine.NodeID(n)] = addr
		}
	}
	svc := kernel.AddService[core.Action, core.EventData](h.W, a)
	h.Route = func(r *servicetest.Router, out core.Action) { kernel.Call(r, svc, out) }
}

// ---- the cases against the old fakes ----

func timing(t servicetest.Timing) simulator.FastWithRareSlow {
	return simulator.FastWithRareSlow{FastMin: t.FastMin, FastMax: t.FastMax, SlowMin: t.SlowMin, SlowMax: t.SlowMax, SlowProb: t.SlowProb}
}

func outage(m servicetest.Mix) simulator.ServiceOutage {
	return simulator.ServiceOutage{Prob: m.OutageProb, Dur: m.OutageDur}
}

func cpFaults(m servicetest.Mix) simulator.ControlPlaneFaults {
	return simulator.ControlPlaneFaults{
		ApplyLatency: timing(m.Apply), RespondLatency: timing(m.Respond),
		Unavailable503: m.P503, Timeout: m.Timeout, LostResponse: m.Lost,
		Outage: outage(m), CallerTimeout: m.CallerTimeout, MaxCommitItems: m.MaxCommitItems,
	}
}

func objectStore(h *servicetest.Harness, c servicetest.Case) servicetest.Subject {
	e := oldobj.New(simulator.ObjectStoreFaults{
		ApplyLatency: timing(c.Mix.Apply), RespondLatency: timing(c.Mix.Respond),
		Unavailable503: c.Mix.P503, Timeout: c.Mix.Timeout, LostResponse: c.Mix.Lost, GetCorruptProb: c.Mix.Corrupt,
		Outage: outage(c.Mix), CallerTimeout: c.Mix.CallerTimeout,
	})
	install(h, e)
	return servicetest.Subject{StateHash: e.StateHash, SpecHash: e.SpecStateHash}
}

func controlPlane(h *servicetest.Harness, c servicetest.Case) servicetest.Subject {
	e := oldcp.New(cpFaults(c.Mix))
	install(h, e)
	return servicetest.Subject{StateHash: e.StateHash, SpecHash: e.SpecStateHash, CreateTopic: e.CreateTopic}
}

func network(h *servicetest.Harness, c servicetest.Case) servicetest.Subject {
	n := c.Net
	e := oldnet.New(simulator.NetworkFaults{
		Latency: timing(n.Latency), PartialIOMax: n.PartialIOMax,
		PartitionProb: n.PartitionProb, PartitionDur: n.PartitionDur,
		ClogProb: n.ClogProb, ClogDur: n.ClogDur,
		RandomCloseProb: n.RandomCloseProb, BlackHoleProb: n.BlackHoleProb, PairLatencyMax: n.PairLatencyMax,
	})
	install(h, e)
	// The old network finds the nodes by name in the adapter, so there is nothing to add.
	return servicetest.Subject{StateHash: e.StateHash, AddNode: func(servicetest.Addr) {}}
}

func generate(t *testing.T, file string, cases []servicetest.Case, setup func(*servicetest.Harness, servicetest.Case) servicetest.Subject) {
	t.Helper()
	var out strings.Builder
	for _, c := range cases {
		h := servicetest.New(t, c.Seed, c.Nodes...)
		sv := setup(h, c)
		c.Run(h, sv)
		out.WriteString(h.Transcript(c.Name))
	}
	path := filepath.Join("..", "testdata", file)
	if err := os.WriteFile(path, []byte(out.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s (%d cases)", path, len(cases))
}

func TestGenerateParityFiles(t *testing.T) {
	generate(t, "parity_objectstore.txt", servicetest.ObjectCases(), objectStore)
	generate(t, "parity_controlplane.txt", servicetest.ControlPlaneCases(), controlPlane)
	generate(t, "parity_network.txt", servicetest.NetworkCases(), network)
}
