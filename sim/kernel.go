package sim

import (
	"fmt"

	"manifester/core"
)

// Constants of the run model. Lanes at or above laneReserved belong to the
// kernel (node timers, node wakeups, simulator entries); engines pick lanes
// below it.
const (
	laneReserved Lane = 1 << 62
	laneSim      Lane = laneReserved

	// wallEpoch is the scripted UTC at simulation time zero (2023-11-14).
	wallEpoch core.WallTime = 1_700_000_000 * 1_000_000_000

	// stopGrace is how long a node has to Exit after core.Stop before it is
	// killed. Node-local Stop.Deadline is the sim deadline plus the node's skew.
	stopGrace = 5 * core.Second

	// defaultMaxSteps applies when Config.MaxSteps is 0, so a timer loop at one
	// instant cannot hang a test.
	defaultMaxSteps uint64 = 10_000_000
)

// Counter indexes. Append-only: Stats is an ordered slice and golden rows pin it.
const (
	cNodeSteps = iota
	cEngineSteps
	cSimSteps
	cDeliveries
	cDroppedStale
	cDroppedByCrash
	cTimersSet
	cTimersCancelled
	cFaults
	cWakes
	cRestarts
	cGracefulStops
	cDeadlineKills
	nCounters
)

func counterNames() []string {
	return []string{
		"node_steps", "engine_steps", "sim_steps", "deliveries", "dropped_stale",
		"dropped_by_crash", "timers_set", "timers_cancelled", "faults", "wakes",
		"restarts", "graceful_stops", "deadline_kills",
	}
}

// Kernel-private entry bodies.
type (
	wake        struct{} // a node should try to Step (it was busy, or just booted)
	phaseChange struct{ to Phase }
)

// node is one simulated process or workload instance. The struct outlives
// incarnations; ref, machine, inbox and timers are per incarnation.
type node struct {
	name      core.NodeID
	idx       int
	workload  bool
	factory   core.Factory
	wlFactory WorkloadFactory
	skew      core.Duration

	ref         core.NodeRef
	alive       bool
	broken      bool // factory failed: never restarted
	stopping    bool
	m           core.Machine
	wl          Workload
	inbox       *nodeInbox
	timers      map[core.TimerID]EntryID
	stepped     uint64
	wakePending bool
	boots       int

	// sum caches m.StateHash(); valid while sumOK. A machine changes only when
	// the kernel calls it (boot, Step, OnPhase), and each of those clears sumOK.
	sum   uint64
	sumOK bool

	noAttrition bool
}

type endKind uint8

const (
	endCrash endKind = iota
	endTrip
	endExit
)

type run struct {
	cfg  Config
	sc   SeedConfig
	seed uint64

	rng    CountedRand
	sched  *sched
	policy core.InboxPolicy
	cost   CostModel

	nodes   []*node
	byName  map[core.NodeID]*node
	engines []Engine
	engCtx  []*engineCtx
	engSum  []uint64 // cached StateHash per engine, valid while engOK
	engOK   []bool   // cleared whenever the kernel calls into the engine
	byEng   map[string]int

	tl    *MemTimeline
	hist  *history
	as    *assertions
	env   env
	sites map[core.Site]bool
	invs  []Invariant

	phase Phase
	steps uint64
	trace hasher
	tEnd  Time

	res      SeedResult
	counters [nCounters]uint64
	halt     bool
	attr     int // pending attrition evaluations

	lanes    map[string]Lane // engine lanes by key, allocated on first use
	nextLane Lane
}

// RunSeed runs one seed to completion and returns its result.
func RunSeed(cfg Config, seed uint64) SeedResult {
	res, _ := runSeed(cfg, seed)
	return res
}

func newRun(cfg Config, seed uint64) *run {
	r := &run{cfg: cfg, seed: seed, policy: core.ControlFirst{}, trace: newHasher(),
		byName: map[core.NodeID]*node{}, lanes: map[string]Lane{}, nextLane: 1, byEng: map[string]int{}, sites: map[core.Site]bool{}}
	r.res.Seed = seed
	r.rng = NewRand(seed)
	r.sched = newSched(r.rng)
	r.tl = NewTimeline()
	r.hist = newHistory(func() Time { return r.sched.Now() }, func(m string) { r.fail(FailProtocol, "history", m) })
	r.as = newAssertions(r)
	r.env = env{Rand: r.rng, Buggify: &buggify{r}, Assertions: r.as}

	r.sc = SeedConfig{ObjectStore: cfg.ObjectStore, ControlPlane: cfg.ControlPlane,
		LogStore: cfg.LogStore, Network: cfg.Network, Attrition: cfg.Attrition, Cost: cfg.Cost, Params: cfg.Params}
	if cfg.Swarm != nil {
		cfg.Swarm(seed, r.rng.Derive("swarm"), &r.sc)
	}
	r.cost = NewCostModel(r.sc.Cost)
	r.tEnd = Time(cfg.Phases.Chaos + cfg.Phases.Settling + cfg.Phases.Settle)

	return r
}

func runSeed(cfg Config, seed uint64) (SeedResult, []AssertionStat) {
	r := newRun(cfg, seed)
	if !r.guard("setup", r.setup) {
		r.halt = true
	}
	if !r.halt {
		r.loop()
	}
	if !r.halt {
		r.phase = PhaseSettle
		for _, inv := range r.invs {
			if inv != nil {
				r.guard("invariant AtEnd "+inv.Name(), func() { inv.AtEnd(view{r}) })
			}
		}
	}
	r.res.Steps = r.steps
	r.res.RandCalls = r.rng.Calls()
	r.res.TraceHash = r.trace.Sum()
	for i, name := range counterNames() {
		r.res.Stats = append(r.res.Stats, Counter{Name: name, N: r.counters[i]})
	}
	return r.res, r.as.stats
}

func (r *run) now() Time { return r.sched.Now() }

// local is node n's clock: simulation time plus its fixed skew.
func (r *run) local(n *node) core.Time { return core.Time(r.now()) + core.Time(n.skew) }

func (r *run) wall() core.WallTime { return wallEpoch + core.WallTime(r.now()) }

func (r *run) fail(k FailKind, p core.Prop, msg string) {
	r.res.Failures = append(r.res.Failures, Failure{Seed: r.seed, Kind: k, Prop: p, Step: r.steps, At: r.now(), Msg: msg})
	if r.cfg.HaltOnFirst {
		r.halt = true
	}
}

func (r *run) logf(format string, args ...any) {
	if r.cfg.Log {
		r.res.Log = append(r.res.Log, LogEntry{Step: r.steps, At: r.now(), Text: fmt.Sprintf(format, args...)})
	}
}

// guard runs fn, turning a panic into FailPanic. It reports whether fn returned.
func (r *run) guard(what string, fn func()) bool {
	defer func() {
		if x := recover(); x != nil {
			r.fail(FailPanic, "panic", fmt.Sprintf("%s: %v", what, x))
		}
	}()
	fn()
	return true
}

// ---- setup ----

func (r *run) setup() {
	r.wireEngines()
	r.addInvariants()
	r.addNodes()
	r.bootNodes()
	r.scheduleCampaign()
}

func (r *run) wireEngines() {
	if r.cfg.Engines != nil {
		r.engines = r.cfg.Engines(r.sc)
	}
	for i, e := range r.engines {
		r.byEng[e.Name()] = i
		r.engCtx = append(r.engCtx, &engineCtx{r: r, name: e.Name()})
		r.engSum = append(r.engSum, 0)
		r.engOK = append(r.engOK, false)
	}
}

func (r *run) addInvariants() {
	for _, f := range r.cfg.Invariants {
		r.invs = append(r.invs, f())
	}
	if r.cfg.InvariantsFor != nil {
		r.invs = append(r.invs, r.cfg.InvariantsFor(r.sc)...)
	}
}

func (r *run) addNodes() {
	add := func(name string, w bool) *node {
		n := &node{name: core.NodeID(name), idx: len(r.nodes), workload: w}
		if max := r.sc.Network.ClockSkewMax; max > 0 {
			n.skew = core.Duration(r.rng.Between(0, int64(max)+1))
		}
		r.nodes = append(r.nodes, n)
		r.byName[n.name] = n
		return n
	}
	for _, p := range r.cfg.Processes {
		for i := range p.Count {
			n := add(fmt.Sprintf("%s-%d", p.Name, i), false)
			n.factory = p.Factory
			n.noAttrition = p.NoAttrition
			if p.FactoryFor != nil {
				n.factory = p.FactoryFor(r.sc)
			}
		}
	}
	for i, wf := range r.cfg.Workloads {
		n := add(fmt.Sprintf("workload-%d", i), true)
		n.wlFactory = wf
	}
}

func (r *run) bootNodes() {
	for _, n := range r.nodes {
		if !n.workload { // workloads boot after every broker has a ref
			r.boot(n)
		}
	}
	for _, n := range r.nodes {
		if n.workload {
			r.boot(n)
		}
	}
}

func (r *run) scheduleCampaign() {
	at := Time(r.cfg.Phases.Chaos)
	r.sched.Schedule(Entry{At: at, Lane: laneSim, Kind: EntryPhase, Body: phaseChange{PhaseSettling}})
	at += Time(r.cfg.Phases.Settling)
	r.sched.Schedule(Entry{At: at, Lane: laneSim, Kind: EntryPhase, Body: phaseChange{PhaseSettle}})
	if q := r.cfg.QuiesceEvery; q > 0 {
		r.sched.Schedule(Entry{At: Time(q), Lane: laneSim, Kind: EntryQuiesce})
	}
	for _, sf := range r.cfg.Scripted {
		switch sf.Kind {
		case EntryCrash, EntryStop, EntryRestart:
			r.sched.Schedule(Entry{At: sf.At, Lane: laneSim, Kind: sf.Kind, Target: Target{Node: core.NodeRef{Node: sf.Node}}})
		default:
			r.fail(FailProtocol, "scripted_fault", fmt.Sprintf("unsupported scripted kind %d", sf.Kind))
		}
	}
}

func nodeLane(n *node, k uint64) Lane { return laneReserved + 1 + Lane(n.idx)<<8 + Lane(k) }

// boot starts a new incarnation of n: fresh ID, fresh machine, Started queued
// (control class, so it is the first Step whatever else arrives) and a wakeup.
func (r *run) boot(n *node) {
	n.ref = core.NodeRef{Node: n.name, Inc: core.Incarnation(r.rng.NewID())}
	n.inbox = &nodeInbox{}
	n.timers = map[core.TimerID]EntryID{}
	n.stepped, n.wakePending, n.stopping = 0, false, false
	n.m, n.wl = nil, nil
	n.boots++
	var m core.Machine
	ok := r.guard("factory "+string(n.name), func() {
		if n.workload {
			var brokers []core.NodeRef
			for _, b := range r.nodes {
				if !b.workload {
					brokers = append(brokers, b.ref)
				}
			}
			n.wl = n.wlFactory(r.env, n.ref, brokers)
			if n.wl != nil {
				m = n.wl
			}
		} else {
			m = n.factory(r.env, n.ref)
		}
	})
	if !ok || m == nil {
		if ok {
			r.fail(FailProtocol, "factory", string(n.name)+": factory returned nil")
		}
		n.alive, n.broken = false, true
		if n.workload {
			r.halt = true
		}
		return
	}
	if sh, isH := m.(interface{ SetHistory(History) }); isH {
		sh.SetHistory(r.hist)
	}
	n.m, n.alive, n.sumOK = m, true, false
	if n.boots > 1 {
		r.counters[cRestarts]++
		r.simFact("sim_restarted", core.Field{Key: "node", Value: core.Str(n.name)})
	}
	n.inbox.Push(core.Started{Self: n.ref})
	r.ensureWake(n, r.now())
}

func (r *run) ensureWake(n *node, at Time) {
	if n.wakePending {
		return
	}
	n.wakePending = true
	r.sched.Schedule(Entry{At: at, Lane: nodeLane(n, 1), Kind: EntryDeliver, Target: Target{Node: n.ref}, Body: wake{}})
}

// ---- facts ----

func (r *run) appendFact(src core.NodeRef, f core.Fact) {
	r.tl.Append(src, r.now(), f)
	r.trace.fact(f)
}

func (r *run) simFact(name string, fields ...core.Field) {
	r.appendFact(core.NodeRef{}, core.Fact{Name: name, Fields: fields})
}

// ---- main loop ----

func (r *run) loop() {
	maxSteps := r.cfg.MaxSteps
	if maxSteps == 0 {
		maxSteps = defaultMaxSteps
	}
	for !r.halt {
		at, ok := r.sched.Peek()
		if !ok || at > r.tEnd {
			return
		}
		if r.steps >= maxSteps {
			r.fail(FailMaxSteps, "max_steps", fmt.Sprintf("seed exceeded %d steps", maxSteps))
			r.halt = true
			return
		}
		e, _ := r.sched.Pop()
		r.steps++
		r.foldEntry(e)
		r.process(e)
		for r.attr > 0 && !r.halt {
			r.attr--
			r.attrition()
		}
		r.foldState()
		for _, inv := range r.invs {
			if inv != nil && !r.halt {
				r.guard("invariant Observe "+inv.Name(), func() { inv.Observe(view{r}) })
			}
		}
	}
}

func (r *run) foldEntry(e Entry) {
	h := &r.trace
	h.I64(int64(e.At))
	h.U64(uint64(e.Kind))
	h.U64(uint64(e.Lane))
	h.ref(e.Target.Node)
	h.Str(e.Target.Engine)
	switch b := e.Body.(type) {
	case core.EventBody:
		h.event(b)
	case wake:
		h.U64(901)
	case phaseChange:
		h.U64(902)
		h.U64(uint64(b.to))
	}
}

// foldState folds every node's and engine's state into the trace hash.
func (r *run) foldState() {
	h := &r.trace
	for _, n := range r.nodes {
		h.Bool(n.alive)
		if !n.alive {
			continue
		}
		if !n.sumOK {
			if !r.guard("StateHash "+string(n.name), func() { n.sum = n.m.StateHash() }) {
				r.halt = true
				return
			}
			n.sumOK = true
		} else if r.cfg.VerifyHashes {
			r.verifyHash("node "+string(n.name), n.sum, n.m.StateHash)
		}
		h.U64(n.sum)
		h.U64(uint64(len(n.inbox.ctrl.q)))
		h.U64(uint64(len(n.inbox.data.q)))
		h.I64(int64(n.inbox.busy))
	}
	for i, e := range r.engines {
		if !r.engOK[i] {
			if !r.guard("StateHash "+e.Name(), func() { r.engSum[i] = e.StateHash() }) {
				r.halt = true
				return
			}
			r.engOK[i] = true
		} else if r.cfg.VerifyHashes {
			r.verifyHash("engine "+e.Name(), r.engSum[i], e.StateHash)
		}
		h.U64(r.engSum[i])
	}
}

// verifyHash fails the seed if the cached sum is not the full StateHash, which
// means a machine or engine changed without the kernel calling it.
func (r *run) verifyHash(what string, cached uint64, full func() uint64) {
	var sum uint64
	if !r.guard("StateHash "+what, func() { sum = full() }) {
		r.halt = true
		return
	}
	if sum != cached {
		r.fail(FailProtocol, "stale_state_hash", what+": state changed outside Step/Execute/OnInternal/OnCrash/EnterRecovery/OnPhase")
		r.halt = true
	}
}

func (r *run) process(e Entry) {
	switch e.Kind {
	case EntryDeliver:
		r.processDeliver(e)
	case EntryEngineInternal:
		r.processInternal(e)
	case EntryCrash:
		r.processCrash(e)
	case EntryRestart:
		r.processRestart(e)
	case EntryStop:
		r.processStop(e)
	case EntryPhase:
		r.processPhase(e)
	case EntryQuiesce:
		r.processQuiesce()
	default:
		r.fail(FailProtocol, "bad_entry", fmt.Sprintf("unknown entry kind %d", e.Kind))
	}
}

func (r *run) processRestart(e Entry) {
	r.counters[cSimSteps]++
	n := r.byName[e.Target.Node.Node]
	if n != nil && !n.alive && !n.broken {
		r.boot(n)
	}
}

func (r *run) processStop(e Entry) {
	r.counters[cSimSteps]++
	n := r.byName[e.Target.Node.Node]
	if n != nil && n.alive && !n.workload && !n.stopping {
		r.stopNode(n)
	}
}

func (r *run) processDeliver(e Entry) {
	n := r.byName[e.Target.Node.Node]
	if n == nil || !n.alive || n.ref != e.Target.Node {
		r.counters[cDroppedStale]++
		r.logf("drop stale %s", desc{e.Body})
		return
	}
	if _, isWake := e.Body.(wake); isWake {
		n.wakePending = false
		r.counters[cWakes]++
		r.logf("wake %s", n.name)
	} else if !r.enqueueDelivery(n, e) {
		return
	}
	r.pump(n)
}

// enqueueDelivery pushes the event body of e onto n's inbox; false (after
// failing the seed) if the body is not an event.
func (r *run) enqueueDelivery(n *node, e Entry) bool {
	body, isEv := e.Body.(core.EventBody)
	if !isEv {
		r.fail(FailProtocol, "bad_delivery", fmt.Sprintf("delivery body %T is not an event", e.Body))
		return false
	}
	r.counters[cDeliveries]++
	r.logf("deliver %s <- %s", n.name, desc{body})
	n.inbox.Push(body)
	return true
}

func (r *run) processInternal(e Entry) {
	i, ok := r.byEng[e.Target.Engine]
	if !ok {
		r.fail(FailProtocol, "bad_internal", "entry for unknown engine "+e.Target.Engine)
		return
	}
	r.counters[cEngineSteps]++
	r.logf("engine %s internal %s", e.Target.Engine, desc{e.Body})
	r.engOK[i] = false
	if !r.guard("OnInternal "+e.Target.Engine, func() { r.engines[i].OnInternal(r.engCtx[i], e.Body) }) {
		r.halt = true
		return
	}
	r.attr++
}

func (r *run) processCrash(e Entry) {
	r.counters[cSimSteps]++
	n := r.byName[e.Target.Node.Node]
	if n == nil || !n.alive || (e.Target.Node.Inc != (core.Incarnation{}) && n.ref != e.Target.Node) {
		return
	}
	if n.stopping && e.Target.Node.Inc != (core.Incarnation{}) {
		r.counters[cDeadlineKills]++
	}
	r.end(n, endCrash, "crash")
}

func (r *run) processPhase(e Entry) {
	r.counters[cSimSteps]++
	pc := e.Body.(phaseChange)
	r.phase = pc.to
	r.logf("phase %d", pc.to)
	r.simFact("sim_phase", core.Field{Key: "phase", Value: core.U64(uint64(pc.to))})
	if pc.to == PhaseSettling {
		r.enterRecovery()
	}
	r.notifyPhase(pc.to)
}

// enterRecovery tells every engine the settling phase began.
func (r *run) enterRecovery() {
	for i, eng := range r.engines {
		r.engOK[i] = false
		if !r.guard("EnterRecovery "+eng.Name(), eng.EnterRecovery) {
			r.halt = true
		}
	}
}

func (r *run) processQuiesce() {
	r.counters[cSimSteps]++
	q := true
	for _, n := range r.nodes {
		if n.workload && n.alive && !n.wl.Quiescent() {
			q = false
		}
	}
	r.simFact("sim_quiesce", core.Field{Key: "quiescent", Value: core.Bool(q)})
	r.sched.Schedule(Entry{At: r.now() + Time(r.cfg.QuiesceEvery), Lane: laneSim, Kind: EntryQuiesce})
}

// notifyPhase calls OnPhase on every live PhaseAware workload, in node order,
// as if it were a Step: the trace hashes the call and the actions, and the
// actions are dispatched like a Step's. It costs no node time and draws no
// randomness of its own. A panic aborts the seed, as for a workload Step.
func (r *run) notifyPhase(p Phase) {
	for _, n := range r.nodes {
		if r.halt {
			return
		}
		pa, ok := n.m.(PhaseAware)
		if !n.workload || !n.alive || !ok {
			continue
		}
		r.trace.Str(string(n.name))
		r.trace.U64(903)
		r.trace.U64(uint64(p))
		r.logf("on_phase %s %d", n.name, p)
		n.sumOK = false
		var acts []core.Action
		if !r.guard("OnPhase "+string(n.name), func() { acts = pa.OnPhase(p) }) {
			r.halt = true
			return
		}
		r.dispatch(n, acts)
	}
}

// pump gives node n one Step if it is idle and has something queued. A busy
// node gets a wakeup at BusyUntil instead.
func (r *run) pump(n *node) {
	if !n.alive {
		return
	}
	now := r.now()
	if n.inbox.busy > now {
		if !n.inbox.empty() {
			r.ensureWake(n, n.inbox.busy)
		}
		return
	}
	body, ok := r.policy.Next(n.inbox.Control(), n.inbox.Data())
	if !ok {
		return
	}
	r.counters[cNodeSteps]++
	if _, started := body.(core.Started); n.stepped == 0 && !started {
		r.fail(FailProtocol, "started_first", fmt.Sprintf("%s: first Step was %T, not Started", n.name, body))
	}
	n.stepped++
	r.trace.Str(string(n.name))
	r.trace.event(body)
	r.logf("step %s %s", n.name, desc{body})
	n.sumOK = false
	var acts []core.Action
	if !r.guard("Step "+string(n.name), func() { acts = n.m.Step(core.Event{At: r.local(n), Wall: r.wall(), Body: body}) }) {
		if n.workload {
			r.halt = true
		} else {
			r.end(n, endCrash, "panic")
		}
		return
	}
	r.dispatch(n, acts)
	if !n.alive {
		return
	}
	var cost core.Duration
	if !r.guard("CostModel", func() { cost = r.cost.Sample(body, r.rng) }) {
		r.halt = true
		return
	}
	if cost < 0 {
		cost = 0
	}
	n.inbox.busy = now + Time(cost)
	if !n.inbox.empty() {
		r.ensureWake(n, n.inbox.busy)
	}
	r.attr++
}

// dispatch starts a Step's actions in slice order.
func (r *run) dispatch(n *node, acts []core.Action) {
	for i, a := range acts {
		if !n.alive || r.halt {
			return
		}
		last := i == len(acts)-1
		r.trace.action(a)
		switch x := a.(type) {
		case core.Emit:
			r.appendFact(n.ref, x.Fact)
		case core.SetTimer:
			after := max(x.After, 0)
			r.counters[cTimersSet]++
			n.timers[x.ID] = r.sched.Schedule(Entry{At: r.now() + Time(after), Lane: nodeLane(n, 0), Kind: EntryDeliver,
				Target: Target{Node: n.ref}, Body: core.TimerFired{ID: x.ID, Gen: x.Gen}})
		case core.CancelTimer:
			if id, ok := n.timers[x.ID]; ok {
				r.counters[cTimersCancelled]++
				r.sched.Cancel(id)
				delete(n.timers, x.ID)
			}
		case core.Trip:
			if !last {
				r.fail(FailProtocol, "trip_not_last", string(n.name)+": Trip is not the last action")
			}
			r.ender(n, endTrip, x.Cause)
			return
		case core.Exit:
			if !last {
				r.fail(FailProtocol, "exit_not_last", string(n.name)+": Exit is not the last action")
			}
			r.ender(n, endExit, "exit")
			return
		default:
			r.route(n, a)
		}
	}
}

// ender ends an incarnation on the node's own request. A workload never ends:
// asking is a protocol failure that aborts the seed.
func (r *run) ender(n *node, how endKind, cause string) {
	if n.workload {
		r.fail(FailProtocol, "workload_ended", string(n.name)+": a workload may not Trip or Exit")
		r.halt = true
		return
	}
	r.end(n, how, cause)
}

// route hands a non-kernel action to the one engine that handles it.
func (r *run) route(n *node, a core.Action) {
	target, count := -1, 0
	ok := r.guard("Handles", func() {
		for i, e := range r.engines {
			if e.Handles(a) {
				target = i
				count++
			}
		}
	})
	if !ok {
		r.halt = true
		return
	}
	if count != 1 {
		r.fail(FailProtocol, "unrouted_action", fmt.Sprintf("%s: action %T handled by %d engines", n.name, a, count))
		return
	}
	r.engOK[target] = false
	if !r.guard("Execute "+r.engines[target].Name(), func() { r.engines[target].Execute(r.engCtx[target], n.ref, a) }) {
		r.halt = true
	}
}

// end finishes n's incarnation: pending entries addressed to it are dropped,
// every engine is told, the machine is discarded and a restart is scheduled.
// Effects the outside world already applied stand.
func (r *run) end(n *node, how endKind, cause string) {
	if !n.alive {
		return
	}
	ref := n.ref
	n.alive, n.stopping = false, false
	r.counters[cDroppedByCrash] += uint64(r.sched.DropNode(ref))
	name := "sim_crashed"
	switch how {
	case endCrash:
		r.res.Crashes++
	case endTrip:
		r.res.Trips++
		name = "sim_tripped"
	case endExit:
		r.res.Exits++
		name = "sim_exited"
	}
	r.logf("%s %s (%s)", name, n.name, cause)
	r.simFact(name, core.Field{Key: "node", Value: core.Str(n.name)}, core.Field{Key: "cause", Value: core.Str(cause)})
	for i, e := range r.engines {
		r.engOK[i] = false
		if !r.guard("OnCrash "+e.Name(), func() { e.OnCrash(r.engCtx[i], ref) }) {
			r.halt = true
		}
	}
	n.m, n.wl, n.inbox, n.timers = nil, nil, &nodeInbox{}, nil
	a := r.sc.Attrition
	delay := core.Duration(r.rng.Between(int64(a.RecoveryDelay[0]), int64(a.RecoveryDelay[1])))
	r.sched.Schedule(Entry{At: r.now() + Time(delay), Lane: laneSim, Kind: EntryRestart, Target: Target{Node: core.NodeRef{Node: n.name}}})
}

// stopNode asks n to shut down gracefully and arms the kill at the deadline.
// The kill entry is addressed to this incarnation, so ending it first (Exit)
// drops the kill with everything else.
func (r *run) stopNode(n *node) {
	n.stopping = true
	r.counters[cGracefulStops]++
	dl := r.now() + Time(stopGrace)
	r.sched.Schedule(Entry{At: dl, Lane: laneSim, Kind: EntryCrash, Target: Target{Node: n.ref}})
	r.simFact("sim_stop_requested", core.Field{Key: "node", Value: core.Str(n.name)})
	n.inbox.Push(core.Stop{Deadline: core.Time(dl) + core.Time(n.skew)})
	r.pump(n)
}

// attrition evaluates one CrashProb roll (Chaos only): pick a live process,
// then a hard crash or, by weight, a graceful Stop. MaxDead caps processes
// that are down or stopping (0 means attrition is off).
func (r *run) attrition() {
	a := r.sc.Attrition
	if r.phase != PhaseChaos || !r.rng.Chance(a.CrashProb) {
		return
	}
	var live []*node
	down := 0
	for _, n := range r.nodes {
		switch {
		case n.workload, n.noAttrition:
		case !n.alive || n.stopping:
			down++
		default:
			live = append(live, n)
		}
	}
	if down >= a.MaxDead || len(live) == 0 {
		return
	}
	n := live[r.rng.Intn(len(live))]
	if total := a.CrashWeight + a.GracefulWeight; total > 0 && r.rng.Float64()*total < a.GracefulWeight {
		r.stopNode(n)
		return
	}
	r.end(n, endCrash, "attrition")
}

// ---- EngineCtx ----

type engineCtx struct {
	r    *run
	name string
}

func (c *engineCtx) Now() Time         { return c.r.now() }
func (c *engineCtx) Rand() CountedRand { return c.r.rng }

// Lane allocates lanes 1, 2, 3... in first-use order. Allocation draws no
// randomness, and first use is itself deterministic, so a key maps to the same
// lane for the whole seed. The range stays below laneReserved.
func (c *engineCtx) Lane(key string) Lane {
	if l, ok := c.r.lanes[key]; ok {
		return l
	}
	l := c.r.nextLane
	if l >= laneReserved {
		panic("sim: engine lane space exhausted")
	}
	c.r.nextLane++
	c.r.lanes[key] = l
	return l
}

func (c *engineCtx) Deliver(to core.NodeRef, at Time, lane Lane, body core.EventBody) EntryID {
	n := c.r.byName[to.Node]
	if n == nil || !n.alive || n.ref != to {
		return 0 // addressed to a dead incarnation: dropped, no draw
	}
	return c.r.sched.Schedule(Entry{At: at, Lane: lane, Kind: EntryDeliver, Target: Target{Node: to}, Body: body})
}

func (c *engineCtx) Self(at Time, lane Lane, body any) EntryID {
	return c.r.sched.Schedule(Entry{At: at, Lane: lane, Kind: EntryEngineInternal, Target: Target{Engine: c.name}, Body: body})
}

func (c *engineCtx) Cancel(id EntryID) { c.r.sched.Cancel(id) }

func (c *engineCtx) Fault(kind string, fields ...core.Field) {
	c.r.counters[cFaults]++
	fs := append([]core.Field{{Key: "kind", Value: core.Str(kind)}, {Key: "engine", Value: core.Str(c.name)}}, fields...)
	c.r.simFact("sim_fault", fs...)
}

func (c *engineCtx) Assert() core.Assertions { return c.r.as }

func (c *engineCtx) Current(node core.NodeID) (core.NodeRef, bool) {
	n := c.r.byName[node]
	if n == nil || !n.alive {
		return core.NodeRef{}, false
	}
	return n.ref, true
}

var (
	_ EngineCtx   = (*engineCtx)(nil)
	_ NodeCurrent = (*engineCtx)(nil)
)

// desc defers rendering to the moment the log line is written; log text is
// never hashed.
type desc struct{ v any }

func (d desc) String() string {
	s := fmt.Sprintf("%T %+v", d.v, d.v)
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}
