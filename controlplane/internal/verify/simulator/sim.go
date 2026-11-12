package simulator

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/check"
	dstclock "github.com/manifesterio/mds/dst/clock"
	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
	dstsim "github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

// Config is one run.
type Config struct {
	Seed     uint64
	Scenario string            // a name from ScenarioNames; empty means "basic"
	Nodes    int               // number of replicas; zero means 3
	Steps    int               // events in the fault phase; zero means DefaultSteps
	Bug      Bug               // a planted bug; normally BugNone
	Trace    func(line string) // when set, every step is passed to it

	// noGroups leaves out the rebalance machines and the consumers. A run is
	// then the run of before they were added, step for step: a test uses it
	// to show that the old trace hashes come back.
	noGroups bool
}

// DefaultSteps is the length of the fault phase of a run, in scheduler events.
const DefaultSteps = 4000

// Failure is one broken property.
type Failure struct {
	Property string
	Detail   string
	Step     int
	TimeMs   int64
	Repeats  int // later hits of the same property in the same run
}

// Stats counts what happened in a run.
type Stats struct {
	Ops, Acked, Rejected, GaveUp, Retries, ReadsOK int
	Crashes, Trips, Pauses, Partitions, Boots      int
	FailedCalls, LostReplies, CutCalls             int
	LogSlots                                       int
	QuietMs                                        int64
	GroupCalls, GroupOK                            int // tries of consumer calls, and the ones answered OK
}

// Result is the outcome of one run.
type Result struct {
	Seed     uint64
	Scenario string
	Hash     uint64 // the trace hash: same seed, same hash
	Steps    int
	EndMs    int64
	Failures []Failure
	Stats    Stats
	Coverage map[string]assert.Coverage
	Sites    map[string]fault.SiteCount // the buggify sites the run reached: on, asked, fired
	Tail     []string                   // the last trace lines, kept when the run fails
}

// OK reports that no property was broken.
func (r Result) OK() bool { return len(r.Failures) == 0 }

// Reproduce is the command that runs this seed again, with a trace.
func (r Result) Reproduce() string {
	return fmt.Sprintf("go run ./cmd/cpsim -scenario %s -seed %d", r.Scenario, r.Seed)
}

func nodeWriter(idx int) string { return fmt.Sprintf("cp-%d", idx) }

// sim is the state of one run.
type sim struct {
	cfg       Config
	scenario  Scenario
	runner    *dstsim.Runner // runs the node processes and the storage engine
	rand      *rng.Rand
	sched     *sched.Scheduler // shares its event heap with clock
	clock     *dstclock.Clock
	env       *env
	store     *storage
	nodes     []*simNode
	clients   *clientPool
	consumers *consumerPool // nil in a run with no consumer groups
	oracle    *oracle

	faults Faults // the faults that are on now; zero in the quiet tail
	quiet  bool
	bug    Bug

	hash     trace.Hash
	stats    Stats
	failures []Failure
	byProp   map[string]int // property -> index in failures

	holders  check.Holders[int]    // writer epoch -> node that held it
	prefixes check.Prefixes[int]   // log prefix -> first state hash that a node showed
	applied  []runtime.LogPosition // last applied slot seen for each node
	fresh    []bool                // the node has a new incarnation, so its hash must be checked again

	ring [traceRing]traceLine
	rpos int
}

// traceRing is how many of the last steps a run keeps for a failure report.
const traceRing = 48

// traceLine is one step, kept as plain data and formatted only when needed.
type traceLine struct {
	step int
	at   int64
	who  dstsim.ProcessID
	body any
	acts int
}

func (t traceLine) String() string {
	return fmt.Sprintf("step %d t=%dms %s <- %s (%d actions)", t.step, t.at/int64(machine.Millisecond), t.who, bodyText(t.body), t.acts)
}

// bodyText is a short text of an event. Long byte slices are cut.
func bodyText(b any) string {
	s := fmt.Sprintf("%T%+v", b, b)
	const limit = 160
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return s
}

// tracef writes a note about something that is not a step (a crash, a partition).
func (s *sim) tracef(format string, args ...any) {
	if s.cfg.Trace != nil {
		s.cfg.Trace(fmt.Sprintf("t=%dms %s", s.now()/int64(machine.Millisecond), fmt.Sprintf(format, args...)))
	}
}

// tail returns the kept trace lines, oldest first.
func (s *sim) tail() []string {
	first := max(0, s.rpos-traceRing)
	out := make([]string, 0, s.rpos-first)
	for i := first; i < s.rpos; i++ {
		out = append(out, s.ring[i%traceRing].String())
	}
	return out
}

func newSim(cfg Config) (*sim, error) {
	sc, ok := ScenarioByName(cfg.Scenario)
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q (have %v)", cfg.Scenario, ScenarioNames())
	}
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.Steps <= 0 {
		cfg.Steps = DefaultSteps
	}
	cfg.Scenario = sc.Name
	s := &sim{
		cfg: cfg, scenario: sc, faults: sc.Faults, bug: cfg.Bug,
		byProp: map[string]int{}, applied: make([]runtime.LogPosition, cfg.Nodes), fresh: make([]bool, cfg.Nodes),
		hash: trace.New(),
	}
	s.runner = dstsim.New(dstsim.Config{Seed: cfg.Seed, WallStart: sched.TimeNs(wallStart), MaxPops: maxSteps + 1, OnStep: s.onStep})
	s.rand, s.sched = s.runner.Rand(), s.runner.Sched()
	s.clock = dstclock.New(s.sched, sched.TimeNs(wallStart))
	s.env = newEnv(s.rand, s.addFailure)
	s.store = newStorage(s)
	s.runner.Register(s.store)
	s.runner.Register(replyEngine{s})
	s.runner.Spawn(controller{s})
	s.oracle = newOracle(s)
	s.clients = newClientPool(s, 3)
	for i := range cfg.Nodes {
		s.nodes = append(s.nodes, newNode(s, i, replicaFactory(i, sc.Faults, cfg.Bug)))
	}
	if sc.Consumers > 0 && !cfg.noGroups {
		for _, n := range s.nodes {
			n.groups = newGroupHost(s, n)
		}
		s.consumers = newConsumerPool(s, sc.Consumers)
	}
	return s, nil
}

// wallStart is the virtual UTC time at the start of every run (2026-01-01).
const wallStart = int64(1767225600) * int64(machine.Second)

// Time moves only when the scheduler takes an event. after queues on the heap
// directly so a node's CancelTimer stays a local token check.

func (s *sim) mono() machine.NodeTime { return machine.NodeTime(s.clock.Mono()) }
func (s *sim) now() int64             { return int64(s.clock.Mono()) }
func (s *sim) steps() int             { return s.sched.Steps() }

// after queues run to happen d from now. A negative d counts as zero.
func (s *sim) after(d machine.Duration, label string, run func()) {
	s.sched.After(sched.TimeNs(d), label, run)
}

func (s *sim) addFailure(property, detail string) {
	if i, seen := s.byProp[property]; seen {
		s.failures[i].Repeats++
		return
	}
	s.byProp[property] = len(s.failures)
	s.failures = append(s.failures, Failure{Property: property, Detail: detail, Step: s.steps(), TimeMs: s.now() / int64(machine.Millisecond)})
}

// violation records a broken property. The run goes on.
func (s *sim) violation(property, format string, args ...any) {
	s.env.Always(false, runtime.PropertyID("sim: "+property), runtime.ContextPair{Key: "detail", Value: fmt.Sprintf(format, args...)})
}

// check returns the checker that records verdicts as "sim: ..." properties.
func (s *sim) check() check.Checker { return check.Checker{Log: s.env.log, Prefix: "sim: "} }

// verdict records a check. It returns ok, so the caller can skip work after a failure.
// Callers on a hot path test the condition first and call violation themselves.
func (s *sim) verdict(ok bool, property, format string, args ...any) bool {
	return s.check().Verdict(ok, property, format, args...)
}

func (s *sim) noteBoot(n *simNode) {
	s.stats.Boots++
	s.fresh[n.idx] = true
	s.tracef("%s boots (boot %d)", n.name(), n.boots)
}

// onStep is dst's Config.OnStep hook. It runs after every Step of a node or
// of a rebalance machine: it folds the state into the trace hash, keeps the
// trace ring, and checks the properties that can be checked on one node. A
// step that did not run the machine (the node is down or paused and keeps the
// value) is not observed.
func (s *sim) onStep(info dstsim.StepInfo) {
	if info.Who == ctlID {
		return
	}
	if strings.HasPrefix(string(info.Who), "rb-") {
		s.onGroupsStep(info)
		return
	}
	n := s.nodeOf(eng.ActorID(info.Who))
	if !n.ran {
		return
	}
	body := info.Inbox
	if _, ok := body.(replicaWake); ok {
		body = n.groups.lastToReplica
	}
	acts := info.Out
	h := &s.hash
	h.Uint64(uint64(s.now()))
	h.Uint64(uint64(n.idx))
	h.Uint64(n.m.StateHash())
	h.Uint64(uint64(len(acts)))
	s.observe(info.Who, body, len(acts))
	s.checkNode(n)
}

// onGroupsStep folds a step of a rebalance machine into the trace hash.
func (s *sim) onGroupsStep(info dstsim.StepInfo) {
	n := s.nodeOf(eng.ActorID("cp-" + strings.TrimPrefix(string(info.Who), "rb-")))
	g := n.groups
	if !g.ran {
		return
	}
	h := &s.hash
	h.Uint64(uint64(s.now()))
	h.Uint64(uint64(len(s.nodes) + n.idx))
	h.Uint64(g.proc.m.StateHash())
	h.Uint64(uint64(len(info.Out)))
	s.observe(info.Who, g.last, len(info.Out))
}

// observe keeps one step in the trace ring, and prints it when tracing.
func (s *sim) observe(who dstsim.ProcessID, body any, acts int) {
	s.ring[s.rpos%traceRing] = traceLine{step: s.steps(), at: s.now(), who: who, body: body, acts: acts}
	s.rpos++
	if s.cfg.Trace != nil {
		s.cfg.Trace(s.ring[(s.rpos-1)%traceRing].String())
	}
}

// checkNode checks the epoch and the state of one node. An epoch has one
// holder. After the same number of applied slots, all nodes have the same state.
func (s *sim) checkNode(n *simNode) {
	r := n.replica()
	if r == nil {
		return
	}
	if e := r.Epoch(); e > 0 {
		if first, ok := s.holders.Note(e, n.idx); !ok {
			s.violation("one writer per epoch", "epoch %d is held by cp-%d and by cp-%d", e, first, n.idx)
		}
	}
	applied := r.Applied()
	if applied == s.applied[n.idx] && !s.fresh[n.idx] {
		return
	}
	s.applied[n.idx], s.fresh[n.idx] = applied, false
	h := r.Metadata().StateHash()
	if prevHash, prevNode, ok := s.prefixes.Note(check.Slot(applied), h, n.idx); !ok {
		s.violation("replicas agree on every slot", "after %d slots, cp-%d has state %x but cp-%d had %x", applied, n.idx, h, prevNode, prevHash)
	}
}
