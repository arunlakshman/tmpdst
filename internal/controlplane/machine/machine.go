package machine

import (
	"fmt"

	"manifester/internal/controlplane/applier"
	"manifester/internal/core"
)

const (
	timerCatchUp = core.TimerID(1)
	timerGapFill = core.TimerID(2)
)

// Machine is the control-plane DSM. Build it with New.
type Machine struct {
	env  core.Environment
	self core.NodeInstance
	cfg  Config

	started  bool
	stopping bool
	exited   bool
	booting  bool // restore/catch-up in progress; Ready flips when done
	ready    bool
	active   bool // advisory appender role

	fold *applier.StateUpdater

	now         core.NodeTime
	timerGen    core.TimerVersion
	holeGen     core.TimerVersion
	roleGen     core.TimerVersion
	ckptGen     core.TimerVersion
	nextReq     core.RequestID
	seq         uint64
	applied     core.LogPosition // next slot to apply
	goalTail    core.LogPosition // CheckTail answer for the current round
	high        core.LogPosition // next slot to allocate for commit appends
	buf         map[core.LogPosition][]byte
	pending     core.RequestID
	pendingKind pendingKind
	fillSlot    core.LogPosition // slot of in-flight hole-fill append
	fillData    []byte           // bytes we offered for that append
	holeArmed   bool             // hole timer is outstanding

	lastPullOK  core.NodeTime
	lastOtherAt core.NodeTime
	lastOwnAt   core.NodeTime
	promoSlot   core.LogPosition
	lowerSeen   bool

	ckptSlot    core.LogPosition // newest checkpoint slot (written or restored)
	ckptAt      core.NodeTime
	ckptPending core.LogPosition // slot of in-flight ObjectPut snapshot
	ckptData    []byte

	queue      []waitingCommand                   // client ops waiting for a commit slot
	flights    map[core.RequestID]*inFlightAppend // in-flight commit LogAppends (at most K)
	waiters    map[string][]waitingCommand
	answers    []ReplyToClient
	proj       map[core.TopicPartition]core.Offset // InjectedBugWriterOffsets projection
	trimFloor  core.LogPosition                    // slots below are trimmed
	stallUntil core.NodeTime
	downUntil  core.NodeTime
	crashed    bool
	needsCkpt  bool // hit a tomb; must restore before catch-up from zero
	seeds      []topicSeed
}

type topicSeed struct {
	name  string
	parts int32
}

type pendingKind uint8

const (
	pendingNone pendingKind = iota
	pendingTail
	pendingRead
	pendingAppend
	pendingObjGet
	pendingObjPut
	pendingTrim
)

// New returns the core.MachineFactory of the control-plane machine.
func New(cfg Config) core.MachineFactory {
	return func(env core.Environment, self core.NodeInstance) core.Machine {
		return &Machine{
			env: env, self: self, cfg: cfg,
			fold:    applier.NewMem(),
			buf:     map[core.LogPosition][]byte{},
			flights: map[core.RequestID]*inFlightAppend{},
			waiters: map[string][]waitingCommand{},
		}
	}
}

var _ core.Machine = (*Machine)(nil)

// Fold is the in-memory applier after the applied prefix. Tests compare its
// digest to an independent oracle.
func (m *Machine) Fold() *applier.StateUpdater { return m.fold }

// Applied is the next slot to apply (slots below are folded).
func (m *Machine) Applied() core.LogPosition { return m.applied }

// SeedTopic pre-creates a topic on the fold before NodeStarted (sim campaign setup).
func (m *Machine) SeedTopic(name string, partitions int32) core.ErrCode {
	err := m.fold.CreateTopic(name, partitions)
	if err == core.OK {
		m.seeds = append(m.seeds, topicSeed{name: name, parts: partitions})
	}
	return err
}

func (m *Machine) reseedTopics() {
	for _, t := range m.seeds {
		_ = m.fold.CreateTopic(t.name, t.parts)
	}
}

// Step implements core.StateMachine. Unknown completions and unhandled bodies
// leave state unchanged. After ShutdownDone, every event is a no-op.
func (m *Machine) Step(ev core.Event) []core.Action {
	if m.exited {
		return nil
	}
	m.now = ev.At
	var acts []core.Action
	switch body := ev.Body.(type) {
	case core.NodeStarted:
		acts = m.onStarted()
	case core.StopRequest:
		acts = m.stop()
	case core.TimerFired:
		acts = m.onTimer(body)
	case core.LogGetEndDone:
		acts = m.onCheckTailDone(body)
	case core.LogReadDone:
		acts = m.onReadDone(body)
	case core.LogAppendDone:
		acts = m.onAppendDone(body)
	case core.ObjectGetDone:
		acts = m.onObjGetDone(body)
	case core.ObjectPutDone:
		acts = m.onObjPutDone(body)
	case core.LogDeleteOldDone:
		acts = m.onTrimDone(body)
	case core.ControlPlaneDeliver:
		return m.onCPOffer(body) // already withReplies
	default:
		return nil
	}
	return m.withReplies(acts)
}

func (m *Machine) onStarted() []core.Action {
	m.started = true
	if m.ckptEnabled() {
		m.booting = true
		m.ready = false
		return m.emitCkptGet()
	}
	return m.afterRestore()
}

func (m *Machine) afterRestore() []core.Action {
	if m.cfg.CatchUpEvery > 0 {
		m.booting = true
		m.ready = false
		return m.armCatchUp()
	}
	return m.finishBoot()
}

func (m *Machine) finishBoot() []core.Action {
	m.booting = false
	m.ready = true
	m.lastOtherAt, m.lastPullOK, m.lastOwnAt = m.now, m.now, m.now
	if m.rolesArmed() && m.cfg.Rank == 0 {
		m.setActive(true)
	}
	var acts []core.Action
	acts = append(acts, m.armRole()...)
	acts = append(acts, m.armCkpt()...)
	return acts
}

func (m *Machine) maybeBecomeReady() []core.Action {
	if !m.booting || m.pending != 0 || m.applied < m.goalTail {
		return nil
	}
	return m.finishBoot()
}

func (m *Machine) onTimer(e core.TimerFired) []core.Action {
	switch e.ID {
	case timerCatchUp:
		if e.Gen != m.timerGen || m.pending != 0 || m.cfg.CatchUpEvery <= 0 {
			return nil
		}
		return m.emitCheckTail()
	case timerGapFill:
		if e.Gen != m.holeGen || !m.holeArmed || m.pending != 0 {
			return nil
		}
		m.holeArmed = false
		if !m.holeOpen() {
			return m.progress()
		}
		return m.emitHoleFill()
	case timerRole:
		return m.onRoleTimer(e)
	case timerCheckpoint:
		return m.onCkptTimer(e)
	default:
		return nil
	}
}

func (m *Machine) onCheckTailDone(d core.LogGetEndDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingTail {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != core.OK || d.Outcome != core.Applied {
		return m.armCatchUp()
	}
	m.notePullOK()
	m.goalTail = d.Tail
	return m.progress()
}

func (m *Machine) onReadDone(d core.LogReadDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingRead {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != core.OK || d.Outcome != core.Applied {
		return m.armCatchUp()
	}
	m.notePullOK()
	for _, e := range d.Entries {
		if e.Slot < m.applied {
			continue
		}
		m.buf[e.Slot] = append([]byte(nil), e.Data...)
		m.observeSlot(e.Slot)
	}
	if err := m.drain(); err != nil {
		return m.trip(err)
	}
	acts := m.progress()
	return append(acts, m.flushCommits()...)
}

func (m *Machine) onAppendDone(d core.LogAppendDone) []core.Action {
	if f, ok := m.flights[d.Req]; ok {
		return m.onCommitAppendDone(d, f)
	}
	if d.Req != m.pending || m.pendingKind != pendingAppend {
		return nil
	}
	slot := m.fillSlot
	offered := m.fillData
	m.pending, m.pendingKind = 0, pendingNone
	m.fillSlot, m.fillData = 0, nil
	if isDeleted(d) {
		// Hole fill landed on a trimmed slot: catch-up cannot rewrite tombs
		// (docs/log-cp-service.md). Restore from checkpoint and rejoin.
		return m.hitTrimmed()
	}
	if d.Err != core.OK || d.Outcome != core.Applied {
		return m.armCatchUp()
	}
	data := offered
	if d.Taken {
		data = append([]byte(nil), d.Existing...)
	}
	m.observeSlot(slot)
	if slot >= m.applied {
		m.buf[slot] = data
	}
	if err := m.drain(); err != nil {
		return m.trip(err)
	}
	acts := m.progress()
	return append(acts, m.flushCommits()...)
}

func (m *Machine) drain() error {
	for {
		data, ok := m.buf[m.applied]
		if !ok {
			return nil
		}
		delete(m.buf, m.applied)
		if err := m.applySlot(m.applied, data); err != nil {
			return err
		}
		m.applied++
		m.holeArmed = false
	}
}

func (m *Machine) progress() []core.Action {
	if m.pending != 0 {
		return nil
	}
	if m.applied < m.goalTail {
		if m.holeOpen() {
			return m.maybeHoleFill()
		}
		return m.emitRead()
	}
	if m.holeOpen() {
		return m.maybeHoleFill()
	}
	if acts := m.maybeBecomeReady(); acts != nil {
		var out []core.Action
		out = append(out, acts...)
		out = append(out, m.armCatchUp()...)
		return out
	}
	return m.armCatchUp()
}

func (m *Machine) holeOpen() bool {
	if len(m.buf) == 0 {
		return false
	}
	_, ok := m.buf[m.applied]
	return !ok
}

type gapPlan uint8

const (
	gapPlanWait gapPlan = iota
	gapPlanFill
	gapPlanArm
	gapPlanSkip
)

func (m *Machine) holeFillPlan() gapPlan {
	if m.cfg.InjectedBug == InjectedBugHoleSkip {
		return gapPlanSkip
	}
	if m.cfg.HoleAfter <= 0 {
		return gapPlanFill
	}
	if m.holeArmed {
		return gapPlanWait
	}
	return gapPlanArm
}

func (m *Machine) maybeHoleFill() []core.Action {
	switch m.holeFillPlan() {
	case gapPlanSkip:
		return m.skipHole()
	case gapPlanFill:
		return m.emitHoleFill()
	case gapPlanArm:
		return m.armHoleFill()
	default:
		return nil
	}
}

func (m *Machine) skipHole() []core.Action {
	// Mutant: advance past the hole without a no-op fill.
	m.applied++
	m.holeArmed = false
	m.env.Always(false, "logcp-sim: a replica's state equals the replay of the log at its slot",
		core.ContextPair{Key: "slot", Value: m.applied})
	return m.progress()
}

func (m *Machine) armHoleFill() []core.Action {
	m.holeGen++
	m.holeArmed = true
	return []core.Action{core.SetTimer{ID: timerGapFill, Gen: m.holeGen, After: m.cfg.HoleAfter}}
}

func (m *Machine) emitHoleFill() []core.Action {
	m.env.Reachable("logcp-sim: hole filled by a no-op")
	m.seq++
	inner := applier.Encode(applier.Entry{
		ID:   fmt.Sprintf("%s/%d", m.cfg.writer(), m.seq),
		Kind: applier.KindNoop,
	})
	data := wrap(m.cfg.Rank, m.cfg.writer(), inner)
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingAppend
	m.fillSlot = m.applied
	m.fillData = data
	return []core.Action{core.LogAppend{Req: m.pending, Slot: m.fillSlot, Data: data}}
}

func (m *Machine) emitCheckTail() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingTail
	return []core.Action{core.LogGetEnd{Req: m.pending}}
}

func (m *Machine) emitRead() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingRead
	return []core.Action{core.LogRead{Req: m.pending, From: m.applied, Limit: m.cfg.readLimit()}}
}

func (m *Machine) armCatchUp() []core.Action {
	if m.cfg.CatchUpEvery <= 0 {
		return nil
	}
	m.timerGen++
	return []core.Action{core.SetTimer{ID: timerCatchUp, Gen: m.timerGen, After: m.cfg.CatchUpEvery}}
}

func (m *Machine) trip(err error) []core.Action {
	m.exited = true
	return []core.Action{core.StopForRestart{Cause: err.Error()}}
}

// stop marks the machine shutting down and emits ShutdownDone. With no in-flight work
// yet that must drain, ShutdownDone is immediate (decision 8).
func (m *Machine) stop() []core.Action {
	if m.stopping {
		return nil
	}
	m.stopping = true
	m.exited = true
	return []core.Action{core.ShutdownDone{}}
}

// StateHash folds every field that influences behaviour, including the applied
// applier digest (comparable across machines that folded the same prefix).
func (m *Machine) StateHash() uint64 {
	h := core.NewHasher()
	h.Bool(m.started)
	h.Bool(m.stopping)
	h.Bool(m.exited)
	h.Bool(m.booting)
	h.Bool(m.ready)
	h.Bool(m.active)
	h.U64(uint64(m.timerGen))
	h.U64(uint64(m.holeGen))
	h.U64(uint64(m.roleGen))
	h.U64(uint64(m.ckptGen))
	h.U64(uint64(m.nextReq))
	h.U64(uint64(m.seq))
	h.U64(uint64(m.applied))
	h.U64(uint64(m.goalTail))
	h.U64(uint64(m.high))
	h.U64(uint64(m.pending))
	h.U64(uint64(m.pendingKind))
	h.U64(uint64(m.fillSlot))
	h.Bool(m.holeArmed)
	h.U64(uint64(m.promoSlot))
	h.Bool(m.lowerSeen)
	h.U64(uint64(m.ckptSlot))
	h.U64(uint64(len(m.queue)))
	h.U64(uint64(len(m.flights)))
	h.U64(uint64(len(m.waiters)))
	h.U64(uint64(m.cfg.CatchUpEvery))
	h.U64(uint64(m.cfg.ReadLimit))
	h.U64(uint64(m.cfg.HoleAfter))
	h.U64(uint64(m.cfg.K))
	h.U64(uint64(m.cfg.MaxCmdsPerSlot))
	h.U64(uint64(m.cfg.QueueMax))
	h.U64(uint64(m.cfg.Rank))
	h.U64(uint64(m.cfg.Heartbeat))
	h.U64(uint64(m.cfg.TakeoverAfter))
	h.U64(uint64(m.cfg.CkptEvery))
	h.U64(uint64(m.cfg.CkptMinSlots))
	h.U64(uint64(m.cfg.KeepSlots))
	h.U64(m.fold.StateHash())
	return h.Sum()
}
