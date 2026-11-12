package kernel

import (
	"fmt"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// Time is global virtual time in nanoseconds since the run started. Machines
// never see it directly: they see machine.Now, with the clock skew of their
// node.
type Time int64

// Lane is an ordering group of the event queue. Entries on one lane at one
// time run in the order they were added (FIFO).
type Lane uint64

// EntryID names one queue entry, so that it can be cancelled.
type EntryID uint64

// The lanes the kernel gives a meaning to.
const (
	// NoLane is no FIFO group: every entry gets a fresh random tie.
	NoLane Lane = 0
	// HostLane is the lane of the kernel itself (restarts, kills, phases).
	// Lanes at or above it belong to the kernel: it also keeps one timer lane
	// and one wake-up lane per node there. Product lanes are below it.
	HostLane Lane = 1 << 62
)

const (
	defaultMaxSteps  uint64 = 10_000_000
	defaultStopGrace        = 5 * machine.Second
)

// Config configures one run.
type Config struct {
	Seed         uint64
	WallStart    machine.RealTime // UTC at time zero
	End          Time             // stop before the first entry after End; 0: no end
	MaxSteps     uint64           // 0 means 10_000_000; more steps is FailMaxSteps
	ClockSkewMax machine.Duration // per-node clock skew in [0, ClockSkewMax], drawn once at Spawn
	Attrition    Attrition
	Log          bool // keep every step line in Result.Log
	Tail         int  // keep the last Tail step lines for Result.Tail of a failed run
	HaltOnFirst  bool // stop the run at the first failure
	VerifyHashes bool // recompute cached state hashes every step; a mismatch fails the seed
}

// Stopped says why RunUntil returned.
type Stopped uint8

// The reasons RunUntil returns.
const (
	StopAsked Stopped = iota // the stop function returned true
	Drained                  // the queue is empty
	TimeUp                   // the next entry is after Config.End
	StepLimit                // Config.MaxSteps was reached (a FailMaxSteps)
	Halted                   // the run halted (HaltOnFirst, or a panic outside a node)
)

// FailKind says what kind of failure a seed had. It is not folded into the
// trace hash.
type FailKind uint8

// The kinds of failure.
const (
	FailViolation     FailKind = iota // an Always, AlwaysOrUnreachable or Unreachable broke
	FailProtocol                      // the kernel was used the wrong way (output after End, zero address, ...)
	FailPanic                         // a panic in a Step, a factory, a route, a service, a host task or a rule
	FailMaxSteps                      // the run took more than Config.MaxSteps steps
	FailNotRepeatable                 // the same seed gave another run (set by the campaign canary)
)

// Failure is the first failure of one property in a run. Later failures of
// the same property only raise Repeats.
type Failure struct {
	Seed     uint64
	Kind     FailKind
	Property string
	Detail   string
	Step     uint64
	At       Time
	Repeats  int
}

// Counter is one run statistic.
type Counter struct {
	Name string
	N    uint64
}

// LogEntry is one step line. Its text is never hashed.
type LogEntry struct {
	Step uint64
	At   Time
	Text string
}

// Result is one run. (Steps, RandCalls, Hash, Stats) is what a golden pin holds.
type Result struct {
	Seed, Steps, RandCalls, Hash uint64
	Crashes, Trips, Exits        int
	Stats                        []Counter
	Failures                     []Failure
	Coverage                     map[string]assert.Coverage
	Sites                        map[string]fault.SiteCount
	Log                          []LogEntry // every step, only with Config.Log
	Tail                         []LogEntry // the last Config.Tail steps, only on failure
}

// OK reports a run without failures.
func (r Result) OK() bool { return len(r.Failures) == 0 }

// Counter indexes. The names and their order are the ones of the old broker
// kernel (without "freezes": freeze is deleted).
const (
	countNodeSteps = iota
	countServiceSteps
	countKernelSteps
	countDeliveries
	countDroppedStale
	countDroppedByCrash
	countTimersSet
	countTimersCancelled
	countFaults
	countWakes
	countRestarts
	countGracefulStops
	countDeadlineKills
	counterCount
)

func counterNames() [counterCount]string {
	return [counterCount]string{
		countNodeSteps:       "node_steps",
		countServiceSteps:    "engine_steps",
		countKernelSteps:     "sim_steps",
		countDeliveries:      "deliveries",
		countDroppedStale:    "dropped_stale",
		countDroppedByCrash:  "dropped_by_crash",
		countTimersSet:       "timers_set",
		countTimersCancelled: "timers_cancelled",
		countFaults:          "faults",
		countWakes:           "wakes",
		countRestarts:        "restarts",
		countGracefulStops:   "graceful_stops",
		countDeadlineKills:   "deadline_kills",
	}
}

// World is one simulation run: one seed, one random stream, one event queue,
// the nodes, the services and the rules.
// It is not safe for concurrent use. Use one World per run.
type World struct {
	cfg     Config
	rand    *rng.Rand
	q       *queue
	faults  *fault.Injector
	asserts *assert.Log
	hash    trace.Hash
	tl      *Timeline

	nodes    []nodeSlot
	names    map[machine.NodeID]bool
	services []serviceSlot
	svcNames map[string]bool
	rules    []Rule
	nextLane Lane

	phase    Phase
	steps    uint64
	attr     int // attrition rolls to make after this queue pop
	halted   bool
	started  bool
	finished bool
	result   Result

	counters              [counterCount]uint64
	crashes, trips, exits int
	failures              []Failure
	lines                 []logLine
	tail                  *trace.Ring[logLine]
}

// New makes a World for cfg.
func New(cfg Config) *World {
	if cfg.MaxSteps == 0 {
		cfg.MaxSteps = defaultMaxSteps
	}
	w := &World{
		cfg:      cfg,
		rand:     rng.New(cfg.Seed),
		hash:     trace.New(),
		tl:       NewTimeline(),
		names:    map[machine.NodeID]bool{},
		svcNames: map[string]bool{},
		nextLane: 1,
	}
	w.q = newQueue(w.rand)
	w.faults = fault.New(w.rand)
	w.asserts = assert.New(func(property, detail string) { w.Fail(FailViolation, property, detail) })
	w.asserts.SetStepCounter(w.Steps)
	if cfg.Tail > 0 {
		w.tail = trace.NewRing[logLine](cfg.Tail)
	}
	return w
}

// Now is the current virtual time.
func (w *World) Now() Time { return w.q.now }

// Wall is the UTC time now: Config.WallStart plus Now.
func (w *World) Wall() machine.RealTime { return w.cfg.WallStart + machine.RealTime(w.q.now) }

// Steps is the number of queue entries taken so far.
func (w *World) Steps() uint64 { return w.steps }

// Rand is the one random stream of the run.
func (w *World) Rand() *rng.Rand { return w.rand }

// Env is the one machine.Env that every machine of the run shares.
func (w *World) Env() machine.Env { return env{w} }

// Faults is the buggify injector of the run.
func (w *World) Faults() *fault.Injector { return w.faults }

// Assert is the assertion log of the run.
func (w *World) Assert() *assert.Log { return w.asserts }

// Logging reports Config.Log.
func (w *World) Logging() bool { return w.cfg.Log }

// Timeline is the fact timeline of the run.
func (w *World) Timeline() *Timeline { return w.tl }

// Phase is the current phase.
func (w *World) Phase() Phase { return w.phase }

// Note adds a log line. It is never hashed.
func (w *World) Note(text string) { w.logMsg(text, nil) }

// Fact appends a host fact (source zero) to the timeline.
func (w *World) Fact(f machine.Fact) { w.appendFact(machine.NodeInstance{}, f) }

// Cancel retires a pending queue entry. An entry that already ran, or an
// unknown id, is ignored.
func (w *World) Cancel(id EntryID) { w.q.cancel(id) }

// AddRule adds a rule. Add every rule before the run starts.
func (w *World) AddRule(r Rule) { w.rules = append(w.rules, r) }

// Fail records a failure of property. Only the first failure of one property
// is kept; later ones raise its Repeats. With Config.HaltOnFirst the run halts.
func (w *World) Fail(kind FailKind, property, detail string) {
	for i := range w.failures {
		if w.failures[i].Property == property {
			w.failures[i].Repeats++
			return
		}
	}
	w.failures = append(w.failures, Failure{
		Seed: w.cfg.Seed, Kind: kind, Property: property, Detail: detail, Step: w.steps, At: w.q.now,
	})
	w.logMsg("fail "+property+": "+detail, nil)
	if w.cfg.HaltOnFirst {
		w.halted = true
	}
}

// At runs fn as a host task at time t on lane. Host code (clients, nemesis,
// phase changes) uses host tasks. A panic in fn halts the run.
func (w *World) At(t Time, lane Lane, fn func()) EntryID {
	e := &entry{at: t, lane: lane, kind: kindHost, node: -1}
	e.run = func() {
		w.counters[countKernelSteps]++
		if !w.guard("host task", fn) {
			w.halted = true
		}
	}
	return w.q.add(e)
}

// After runs fn as a host task after d, with no lane.
func (w *World) After(d machine.Duration, fn func()) EntryID {
	return w.At(w.q.now+Time(max(d, 0)), NoLane, fn)
}

// Start boots every spawned node: first the nodes that are not drivers, in
// spawn order, then the drivers. Spawn every node before Start; a node
// spawned later stays down until Boot. RunUntil calls Start if it was not
// called.
func (w *World) Start() {
	if w.started {
		return
	}
	w.started = true
	for _, s := range w.nodes {
		if !s.base().driver {
			s.boot()
		}
	}
	for _, s := range w.nodes {
		if s.base().driver {
			s.boot()
		}
	}
}

// RunUntil takes queue entries until stop returns true (checked before each
// entry; stop may be nil), the queue is empty, the next entry is after
// Config.End, the step limit is hit, or the run halted.
func (w *World) RunUntil(stop func() bool) Stopped {
	w.Start()
	for {
		if w.halted {
			return Halted
		}
		if stop != nil && stop() {
			return StopAsked
		}
		at, ok := w.q.peek()
		if !ok {
			return Drained
		}
		if w.cfg.End > 0 && at > w.cfg.End {
			return TimeUp
		}
		if w.steps >= w.cfg.MaxSteps {
			w.Fail(FailMaxSteps, "max_steps", fmt.Sprintf("the run took more than %d steps", w.cfg.MaxSteps))
			w.halted = true
			return StepLimit
		}
		e, _ := w.q.pop()
		w.steps++
		w.foldEntry(e)
		e.run()
		for w.attr > 0 && !w.halted {
			w.attr--
			w.attrition()
		}
		w.foldState()
		w.observe()
	}
}

// Finish runs Rule.AtEnd of every rule (unless the run halted) and returns
// the result. Calling it again returns the same result.
func (w *World) Finish() Result {
	if w.finished {
		return w.result
	}
	w.finished = true
	for _, r := range w.rules {
		if w.halted {
			break
		}
		if !w.guard("rule AtEnd "+r.Name(), func() { r.AtEnd(view{w}) }) {
			w.halted = true
		}
	}
	res := Result{
		Seed: w.cfg.Seed, Steps: w.steps, RandCalls: w.rand.Calls(), Hash: w.hash.Sum(),
		Crashes: w.crashes, Trips: w.trips, Exits: w.exits,
		Failures: append([]Failure(nil), w.failures...),
		Coverage: w.asserts.Coverage(),
		Sites:    w.faults.Sites(),
	}
	for i, name := range counterNames() {
		res.Stats = append(res.Stats, Counter{Name: name, N: w.counters[i]})
	}
	for _, l := range w.lines {
		res.Log = append(res.Log, l.entry())
	}
	if len(w.failures) > 0 && w.tail != nil {
		for _, l := range w.tail.Tail() {
			res.Tail = append(res.Tail, l.entry())
		}
	}
	w.result = res
	return res
}

// guard runs fn and turns a panic into a FailPanic. It reports whether fn
// returned normally.
func (w *World) guard(what string, fn func()) bool {
	done := false
	defer func() {
		if !done {
			w.Fail(FailPanic, "panic", what+": "+fmt.Sprint(recover()))
		}
	}()
	fn()
	done = true
	return true
}

// appendFact puts f on the timeline and folds it into the trace hash.
func (w *World) appendFact(source machine.NodeInstance, f machine.Fact) {
	w.tl.Append(source, w.q.now, f)
	f.Fold(&w.hash)
}

// kernelFact appends a fact of the kernel itself (source zero).
func (w *World) kernelFact(name string, fields ...machine.Field) {
	w.appendFact(machine.NodeInstance{}, machine.Fact{Name: name, Fields: fields})
}

// foldEntry folds one queue entry: time, kind, lane, target, then the body.
func (w *World) foldEntry(e *entry) {
	h := &w.hash
	h.Int64(int64(e.at))
	h.Uint64(uint64(e.kind))
	h.Uint64(uint64(e.lane))
	name := ""
	if e.node >= 0 {
		name = string(w.nodes[e.node].base().name)
	}
	h.String(name)
	h.Bytes(e.inc[:])
	h.String(e.service)
	if e.body != nil {
		e.body.Fold(h)
	}
}

// foldState folds, for every node, up or down and (if up) its state hash,
// inbox lengths and busy-until time; then the state hash of every service.
// A cached hash is recomputed only when the kernel called into its owner.
func (w *World) foldState() {
	h := &w.hash
	for _, s := range w.nodes {
		b := s.base()
		h.Bool(b.up)
		if !b.up {
			continue
		}
		if !w.cachedHash("node "+string(b.name), &b.hashCache, s.stateHash) {
			return
		}
		c, d := s.inboxLens()
		h.Uint64(b.sum)
		h.Uint64(uint64(c))
		h.Uint64(uint64(d))
		h.Int64(int64(b.busy))
	}
	for _, s := range w.services {
		b := s.base()
		if !w.cachedHash("service "+b.name, &b.hashCache, s.stateHash) {
			return
		}
		h.Uint64(b.sum)
	}
}

// hashCache is a cached state hash. It is valid while ok.
type hashCache struct {
	sum uint64
	ok  bool
}

// cachedHash brings c up to date. With Config.VerifyHashes it also checks a
// valid cache against a fresh hash. It reports false when the run halted.
func (w *World) cachedHash(what string, c *hashCache, full func() uint64) bool {
	if c.ok && !w.cfg.VerifyHashes {
		return true
	}
	var sum uint64
	if !w.guard("StateHash "+what, func() { sum = full() }) {
		w.halted = true
		return false
	}
	if c.ok && sum != c.sum {
		w.Fail(FailProtocol, "stale_state_hash", what+": the state changed while the kernel did not call it")
		w.halted = true
		return false
	}
	c.sum, c.ok = sum, true
	return true
}

// observe gives every rule the view after one queue pop.
func (w *World) observe() {
	for _, r := range w.rules {
		if w.halted {
			return
		}
		if !w.guard("rule Observe "+r.Name(), func() { r.Observe(view{w}) }) {
			w.halted = true
		}
	}
}
