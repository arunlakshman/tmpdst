package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/check"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// Config is one run.
type Config struct {
	Seed        uint64
	Scenario    string // a name from ScenarioNames; empty means "basic"
	Nodes       int    // number of replicas; zero means 3
	Steps       int    // events in the fault phase; zero means DefaultSteps
	Bug         Bug    // a planted bug; normally BugNone
	Log         bool   // keep every step line in Result.Log
	HaltOnFirst bool   // stop the run at the first failure (a replay)

	// noGroups leaves out the rebalance machines and the consumers. A run is
	// then the run of before they were added: a test uses it to show that the
	// groups are the only change to the trace.
	noGroups bool

	// verifyHashes makes the kernel check every step that a cached state hash
	// is still right (a test turns it on).
	verifyHashes bool
}

// DefaultSteps is the length of the fault phase of a run, in queue events.
const DefaultSteps = 4000

// Stats counts what happened in a run.
type Stats struct {
	Ops, Acked, Rejected, GaveUp, Retries, ReadsOK int
	Crashes, Trips, Pauses, Partitions, Boots      int
	FailedCalls, LostReplies, CutCalls             int
	LogSlots                                       int
	QuietMs                                        int64
	GroupCalls, GroupOK                            int // tries of consumer calls, and the ones answered OK
}

// Result is the outcome of one run: the result of the kernel (hash, steps,
// failures, coverage, buggify sites, the last step lines of a failed run) and
// the numbers of the product.
type Result struct {
	kernel.Result
	Scenario string
	EndMs    int64
	Stats    Stats
}

// Reproduce is the command that runs this seed again, with a trace.
func (r Result) Reproduce() string {
	return fmt.Sprintf("go run ./cmd/cpsim -scenario %s -seed %d", r.Scenario, r.Seed)
}

func nodeWriter(idx int) string { return fmt.Sprintf("cp-%d", idx) }

// sim is the state of one run.
type sim struct {
	cfg       Config
	scenario  Scenario
	w         *kernel.World // the nodes, the storage service, the queue and the hash
	env       machine.Env   // the Env of the world: the random stream, buggify and assertions
	rand      *rng.Rand
	lanes     *kernel.Lanes[groupLane]
	store     *storage
	storeAddr kernel.ServiceAddr[runtime.IOCall, runtime.EventData]
	nodes     []*simNode
	byName    map[machine.NodeID]*simNode
	clients   *clientPool
	consumers *consumerPool // nil in a run with no consumer groups
	oracle    *oracle

	faults Faults // the faults that are on now; zero in the quiet tail
	quiet  bool
	bug    Bug

	stats Stats

	holders  check.Holders[int]    // writer epoch -> node that held it
	prefixes check.Prefixes[int]   // log prefix -> first state hash that a node showed
	applied  []runtime.LogPosition // last applied slot seen for each node
	fresh    []bool                // the node has a new incarnation, so its hash must be checked again
}

// traceRing is how many of the last steps a run keeps for a failure report.
const traceRing = 48

// tracef writes a note about something that is not a step (a crash, a
// partition). A note is never hashed.
func (s *sim) tracef(format string, args ...any) {
	s.w.Note(fmt.Sprintf(format, args...))
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
		applied: make([]runtime.LogPosition, cfg.Nodes), fresh: make([]bool, cfg.Nodes),
		byName: map[machine.NodeID]*simNode{},
	}
	s.w = kernel.New(kernel.Config{
		Seed: cfg.Seed, WallStart: machine.RealTime(wallStart), MaxSteps: maxSteps + 1,
		Log: cfg.Log, Tail: traceRing, HaltOnFirst: cfg.HaltOnFirst, VerifyHashes: cfg.verifyHashes,
	})
	s.env, s.rand = s.w.Env(), s.w.Rand()
	// Start before the nodes are spawned: Start boots the nodes it finds, and
	// here every node boots at a random time (run.go boot).
	s.w.Start()
	s.lanes = kernel.NewLanes[groupLane](s.w)
	s.store = newStorage(s)
	s.storeAddr = kernel.AddService[runtime.IOCall, runtime.EventData](s.w, s.store)
	s.oracle = newOracle(s)
	s.clients = newClientPool(s, 3)
	for i := range cfg.Nodes {
		n := newNode(s, i, replicaFactory(i, sc.Faults, cfg.Bug))
		s.nodes = append(s.nodes, n)
		s.byName[n.id] = n
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

// Time moves only when the kernel takes an event from the queue.

func (s *sim) mono() machine.NodeTime { return machine.NodeTime(s.w.Now()) }
func (s *sim) now() int64             { return int64(s.w.Now()) }
func (s *sim) steps() int             { return int(s.w.Steps()) }

// after runs run as a host task d from now. A negative d counts as zero.
func (s *sim) after(d machine.Duration, run func()) { s.w.After(d, run) }

// violation records a broken property. The run goes on.
func (s *sim) violation(property assert.Always, format string, args ...any) {
	s.env.Always(false, property, assert.Pair{Key: "detail", Value: fmt.Sprintf(format, args...)})
}

// check returns the checker of dst/check. It records its own verdicts (a log with
// no hole, log entries that decode) as "sim: ..." properties.
func (s *sim) check() check.Checker { return check.Checker{Log: s.w.Assert(), Prefix: "sim: "} }

// verdict records a check. It returns ok, so the caller can skip work after a failure.
// Callers on a hot path test the condition first and call violation themselves.
func (s *sim) verdict(ok bool, property assert.Always, format string, args ...any) bool {
	if ok {
		s.env.Always(true, property)
		return true
	}
	s.env.Always(false, property, assert.Pair{Key: "detail", Value: fmt.Sprintf(format, args...)})
	return false
}

func (s *sim) noteBoot(n *simNode) {
	s.stats.Boots++
	s.fresh[n.idx] = true
	s.tracef("%s boots (boot %d)", n.name(), n.boots)
}

// checkNode checks the epoch and the state of one node. An epoch has one
// holder. After the same number of applied slots, all nodes have the same state.
// The node calls it after each Step.
func (s *sim) checkNode(n *simNode) {
	r := n.m
	if r == nil {
		return
	}
	if e := r.Epoch(); e > 0 {
		if first, ok := s.holders.Note(e, n.idx); !ok {
			s.violation(oneWriterPerEpoch, "epoch %d is held by cp-%d and by cp-%d", e, first, n.idx)
		}
	}
	applied := r.Applied()
	if applied == s.applied[n.idx] && !s.fresh[n.idx] {
		return
	}
	s.applied[n.idx], s.fresh[n.idx] = applied, false
	h := r.Metadata().StateHash()
	if prevHash, prevNode, ok := s.prefixes.Note(check.Slot(applied), h, n.idx); !ok {
		s.violation(replicasAgreeEverySlot, "after %d slots, cp-%d has state %x but cp-%d had %x", applied, n.idx, h, prevNode, prevHash)
	}
}

// logLines formats step lines of the kernel for a person.
func logLines(in []kernel.LogEntry) []string {
	out := make([]string, 0, len(in))
	for _, l := range in {
		out = append(out, fmt.Sprintf("#%d t=%dms %s", l.Step, int64(l.At)/int64(machine.Millisecond), l.Text))
	}
	return out
}
