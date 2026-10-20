// Package logengine is the simulated log-backed control plane (decisions 48-53,
// docs/log-cp-service.md, docs/sim-log-engine.md): a DynamoDB-like log of slots
// written with if-absent Puts, two replicas that apply it (an active writer and
// a hot standby), and the service boundary faults of the other engines.
//
// It models what the production service does and what can go wrong with it:
// K appends in flight per writer, a lost Put answer (retry the same slot), a
// lost slot (the same entry is appended later), holes (a later slot lands
// first; after HoleAfter any replica fills the gap with a no-op), takeover and
// failback (the slot is the fence), checkpoints, trimming that leaves
// tombstones, replica crash and restart from a checkpoint, and an idempotency
// window that is a function of the slot number.
//
// It is an independent implementation (decision 21): the state machine is in
// state.go and shares no code with veritas, cp/applier or cp/logcp. veritas
// remains the oracle through the Kafka simulator's invariants. A reference
// replay of the log (ref) is the engine's ground truth (CPTruth) and every
// replica's state digest is checked against it.
//
// The engine is deterministic: no goroutines, no map iteration, no wall clock;
// every draw comes from the run's CountedRand in event order.
package logengine

import (
	"manifester/core"
	"manifester/sim"
)

// Bug selects a mutant (sim.LogParams.Bug). Each is caught by a property.
const (
	BugNone             uint8 = iota
	BugWriterOffsets          // offsets chosen by the writer instead of by apply
	BugTrimDelete             // trimmed slots are deleted, not tombstoned
	BugPerReplicaWindow       // each replica forgets keys on its own clock
	BugHoleSkip               // a replica passes an empty slot without a no-op
)

type slotKind uint8

const (
	slotEmpty slotKind = iota
	slotEntry
	slotTomb
)

// slotRec is one slot of the log.
type slotRec struct {
	kind   slotKind
	writer int // rank
	id     uint64
	cmds   []cmd // empty for a no-op
}

// entry is what a writer appends: one or more commands.
type entry struct {
	id   uint64
	cmds []cmd
}

type waiter struct {
	eid uint64
	idx int
	req uint64
}

type qcmd struct {
	c   cmd
	req uint64
}

// request is one CPCall in flight at the service boundary.
type request struct {
	id       uint64
	from     core.NodeRef
	call     core.CPCall
	deadline sim.Time
	rank     int // replica that serves a read
	tail     int // slot a read must reach
	muted    bool
	done     bool
}

type appendOp struct {
	id      uint64
	rank    int
	inc     int
	slot    int
	ent     *entry
	filler  bool
	attempt int
}

type snapshot struct {
	slot int
	st   *state
}

type projEnd struct {
	tp  core.TopicPartition
	end core.Offset
}

type replica struct {
	rank, inc             int
	up, active, sawLower  bool
	stallUntil, downUntil sim.Time
	st                    *state
	applied, next         int
	holeSlot, fillSlot    int
	holeSince             sim.Time
	lastOther, lastAppend sim.Time
	q                     []qcmd
	ops                   []uint64
	waits                 []waiter
	proj                  []projEnd
}

type shadowKey struct {
	key    string
	ranges []core.BatchRange
}

// Engine is the simulated log control plane. It is also the sim.CPTruth.
type Engine struct {
	f    sim.ControlPlaneFaults
	p    sim.LogParams
	name string

	slots   []slotRec
	hi      int // highest landed slot, -1 when none
	trimmed int // slots below are tombstoned (or deleted by the mutant)
	reps    [2]*replica
	ref     *replica // reference replay of the log (ground truth)
	refDig  []uint64 // refDig[s] = reference digest after slot s
	fresh   *state   // topics created before the run
	snaps   []snapshot
	shadow  []shadowKey

	reqs    []*request
	ops     []*appendOp
	nextReq uint64
	nextOp  uint64
	nextEnt uint64
	ticks   uint64
	gen     uint64

	now      sim.Time
	until    sim.Time
	recovery bool
	started  bool
}

var (
	_ sim.Engine  = (*Engine)(nil)
	_ sim.CPTruth = (*Engine)(nil)
)

// DefaultParams are scaled-down production values for a 45 s run: a window
// larger than a run, checkpoints every 24 slots, 8 slots kept below them.
func DefaultParams() sim.LogParams {
	ms := core.Millisecond
	return sim.LogParams{Enabled: true, K: 4, GroupMax: 4, Tick: 10 * ms,
		HoleAfter: 60 * ms, TakeoverAfter: 600 * ms, PutRetry: 80 * ms, Heartbeat: 250 * ms,
		PutFail: 0.01, PutLost: 0.02, ReadFail: 0.05,
		StallProb: 0.0002, CrashProb: 0.0001,
		StallDur: [2]core.Duration{800 * ms, 2500 * ms}, DownDur: [2]core.Duration{300 * ms, 1500 * ms},
		WindowSlots: 1 << 20, TrimStride: 16, CkptEvery: 24, KeepSlots: 8}
}

// New returns an empty log control plane. Faults are f; the log knobs are
// f.Log (zero fields take DefaultParams).
func New(f sim.ControlPlaneFaults) *Engine {
	e := &Engine{f: f, p: withDefaults(f.Log), name: "logengine", hi: -1, fresh: &state{}}
	for i := range e.reps {
		e.reps[i] = newReplica(i, e.fresh)
	}
	e.reps[0].active = true
	e.ref = newReplica(-1, e.fresh)
	return e
}

func withDefaults(p sim.LogParams) sim.LogParams {
	d := DefaultParams()
	if p.K <= 0 {
		p.K = d.K
	}
	if p.GroupMax <= 0 {
		p.GroupMax = d.GroupMax
	}
	if p.Tick <= 0 {
		p.Tick = d.Tick
	}
	def := func(v *core.Duration, dv core.Duration) {
		if *v <= 0 {
			*v = dv
		}
	}
	def(&p.HoleAfter, d.HoleAfter)
	def(&p.TakeoverAfter, d.TakeoverAfter)
	def(&p.PutRetry, d.PutRetry)
	def(&p.Heartbeat, d.Heartbeat)
	if p.WindowSlots <= 0 {
		p.WindowSlots = d.WindowSlots
	}
	if p.TrimStride <= 0 {
		p.TrimStride = d.TrimStride
	}
	if p.CkptEvery <= 0 {
		p.CkptEvery = d.CkptEvery
	}
	if p.KeepSlots <= 0 {
		p.KeepSlots = d.KeepSlots
	}
	return p
}

func newReplica(rank int, base *state) *replica {
	return &replica{rank: rank, up: true, st: base.clone(), applied: -1, holeSlot: -1, fillSlot: -1}
}

// CreateTopic pre-creates a topic (setup, before the run starts).
func (e *Engine) CreateTopic(name string, partitions int32) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	e.fresh.grow(name, partitions)
	e.ref.st.grow(name, partitions)
	for _, r := range e.reps {
		r.st.grow(name, partitions)
	}
	e.gen++
	return core.OK
}

func (e *Engine) Name() string { return e.name }

// Handles reports whether a is a control plane call.
func (e *Engine) Handles(a core.Action) bool {
	_, ok := a.(core.CPCall)
	return ok
}

// EnterRecovery stops new faults for later actions and ticks.
func (e *Engine) EnterRecovery() { e.recovery = true }

// OnCrash: a caller's death changes nothing in the service.
func (e *Engine) OnCrash(sim.EngineCtx, core.NodeRef) {}

// Gen counts applied changes of the reference log.
func (e *Engine) Gen() uint64 { return e.gen }

// Committed returns the committed ranges of tp in offset order (reference replay).
func (e *Engine) Committed(tp core.TopicPartition) []core.BatchRange {
	i, ok := e.ref.st.find(tp)
	if !ok {
		return nil
	}
	return append([]core.BatchRange(nil), e.ref.st.parts[i].runs...)
}

// HighWatermark is the partition end: the next offset the log assigns.
func (e *Engine) HighWatermark(tp core.TopicPartition) core.Offset { return e.ref.st.end(tp) }

// Applied is the last slot replica rank has applied (-1 none); for tests.
func (e *Engine) Applied(rank int) int { return e.reps[rank].applied }

// Tail is the highest landed slot (-1 none); for tests.
func (e *Engine) Tail() int { return e.hi }

// Digest is the digest of replica rank's state; for tests.
func (e *Engine) Digest(rank int) uint64 { return e.reps[rank].st.digest }

// RefDigest is the reference digest after slot s; for tests.
func (e *Engine) RefDigest(s int) uint64 { return e.refDig[s] }

// StateHash folds everything that can change the future of the run.
func (e *Engine) StateHash() uint64 {
	h := uint64(0)
	for _, v := range []uint64{uint64(e.hi + 1), uint64(e.trimmed), e.gen, uint64(len(e.slots)),
		uint64(e.until), uint64(len(e.reqs)), uint64(len(e.ops)), e.ticks, e.nextEnt, uint64(len(e.snaps))} {
		h = mixU64(h, v)
	}
	for _, r := range e.reps {
		h = mixU64(h, r.hash())
	}
	return mixU64(mixU64(h, e.ref.st.digest), boolBit(e.recovery))
}

func (r *replica) hash() uint64 {
	h := uint64(0)
	for _, v := range []uint64{uint64(r.applied + 1), r.st.digest, boolBit(r.up), boolBit(r.active),
		uint64(r.stallUntil), uint64(r.downUntil), uint64(len(r.q)), uint64(len(r.ops)), uint64(r.next), uint64(len(r.waits)),
		uint64(r.holeSlot + 1), uint64(r.inc)} {
		h = mixU64(h, v)
	}
	return h
}

func boolBit(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
