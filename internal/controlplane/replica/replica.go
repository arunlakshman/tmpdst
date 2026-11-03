package replica

import (
	"bytes"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/core"
)

const (
	timerCatchUp = core.TimerID(1)
)

// Replica is the control-plane DSM. Build it with New.
type Replica struct {
	env    core.Environment
	self   core.NodeInstance
	config Config

	openErr error // the first store could not open; NodeStarted stops the machine

	started  bool
	stopping bool
	exited   bool
	booting  bool // restore/catch-up in progress; Ready flips when done
	ready    bool
	active   bool // advisory appender role

	fold *metadata.Store

	now           core.NodeTime
	timerGen      core.TimerVersion
	roleGen       core.TimerVersion
	checkpointGen core.TimerVersion
	nextReq       core.RequestID
	seq           uint64
	applied       core.LogPosition // next slot to apply
	goalTail      core.LogPosition // CheckTail answer for the current round
	high          core.LogPosition // next slot to allocate for commit appends
	buf           map[core.LogPosition][]byte
	pending       core.RequestID
	epoch         uint64         // writer epoch this node holds; zero when it is not the writer
	claimReq      core.RequestID // in-flight LogClaimEpoch; 0 means none
	claimExpect   uint64         // epoch that claim expected to find
	fencedFlag    bool           // a newer epoch was seen while applying; step down after the drain
	stepDowns     uint64         // times this node stopped being the writer because of a newer epoch
	tailChecked   bool           // a tail check finished since the last restore
	maxEpochSeen  uint64         // highest writer epoch seen in applied log entries
	restoreKey    string         // checkpoint object being read
	pendingKind   pendingKind

	lastPullOK  core.NodeTime
	lastOtherAt core.NodeTime
	lastOwnAt   core.NodeTime

	checkpointSlot    core.LogPosition // newest checkpoint slot (written or restored)
	checkpointAt      core.NodeTime
	checkpointPending core.LogPosition // slot of in-flight ObjectPut snapshot
	checkpointData    []byte
	// The checkpoint lane: the snapshot upload, then the log trim, one request at
	// a time, on its own request id. It never uses pending, so a slow upload
	// cannot block catch-up and catch-up cannot block the checkpoint.
	checkpointRequest core.RequestID // in-flight request of the lane; 0 means idle
	checkpointStage   checkpointStage
	trimThrough       core.LogPosition // Through of the in-flight trim

	queue           []waitingCommand                   // client ops waiting for a commit slot
	inFlightAppends map[core.RequestID]*inFlightAppend // in-flight commit LogAppends (at most one)
	waiters         map[string][]waitingCommand
	answers         []ReplyToClient
	proj            map[core.TopicPartition]core.Offset // InjectedBugWriterOffsets projection
	trimFloor       core.LogPosition                    // slots below are trimmed
	stallUntil      core.NodeTime
	downUntil       core.NodeTime
	crashed         bool
	needsCheckpoint bool // fell behind a trim; must restore before catch-up from zero
	checkpointRetry bool // the last checkpoint read failed; read again before catch-up
	seeds           []topicSeed

	readWaiters  []readWaiter // lookups and describes waiting for a fresh tail
	tailsStarted uint64       // tail checks started so far
	tailRound    uint64       // round of the tail check in flight
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
	pendingObjList
	pendingObjGet
)

// checkpointStage says which request the checkpoint lane is waiting for.
type checkpointStage uint8

const (
	stageUpload checkpointStage = iota + 1
	stageTrim
)

// New returns the core.MachineFactory of the control-plane machine. A factory
// cannot return an error, so a store that cannot open (Config.OpenStore) is
// remembered and the machine stops with StopForRestart on NodeStarted: the
// process cannot serve without its state.
func New(config Config) core.MachineFactory {
	return func(env core.Environment, self core.NodeInstance) core.Machine {
		fold, err := config.openFold()
		if err != nil {
			fold = metadata.NewMem() // placeholder; NodeStarted stops the machine
		}
		return &Replica{
			env: env, self: self, config: config, openErr: err,
			fold:            fold,
			buf:             map[core.LogPosition][]byte{},
			inFlightAppends: map[core.RequestID]*inFlightAppend{},
			waiters:         map[string][]waitingCommand{},
		}
	}
}

var _ core.Machine = (*Replica)(nil)

// Metadata is the metadata store after the applied prefix. Tests compare its
// digest to an independent oracle.
func (m *Replica) Metadata() *metadata.Store { return m.fold }

// Applied is the next slot to apply (slots below are folded).
func (m *Replica) Applied() core.LogPosition { return m.applied }

// SeedTopic pre-creates a topic on the fold before NodeStarted (sim campaign setup).
func (m *Replica) SeedTopic(name string, partitions int32) core.ErrCode {
	err := m.fold.CreateTopic(name, partitions)
	if err == core.OK {
		m.seeds = append(m.seeds, topicSeed{name: name, parts: partitions})
	}
	return err
}

func (m *Replica) reseedTopics() {
	for _, t := range m.seeds {
		_ = m.fold.CreateTopic(t.name, t.parts)
	}
}

// Step implements core.StateMachine. Unknown completions and unhandled bodies
// leave state unchanged. After ShutdownDone, every event is a no-op.
func (m *Replica) Step(event core.Event) []core.Action {
	if m.exited {
		return nil
	}
	m.now = event.At
	var acts []core.Action
	switch body := event.Body.(type) {
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
	case core.LogClaimEpochDone:
		acts = m.onClaimDone(body)
	case core.ObjectListDone:
		acts = m.onObjListDone(body)
	case core.ObjectGetDone:
		acts = m.onObjGetDone(body)
	case core.ObjectPutDone:
		acts = m.onObjPutDone(body)
	case core.LogDeleteOldDone:
		acts = m.onTrimDone(body)
	case core.ControlPlaneDeliver:
		return m.onControlPlaneDeliver(body) // already withReplies
	default:
		return nil
	}
	acts = append(acts, m.serveReads()...)
	return m.withReplies(acts)
}

func (m *Replica) onStarted() []core.Action {
	m.started = true
	if m.openErr != nil {
		return m.trip(m.openErr)
	}
	if m.checkpointEnabled() {
		m.booting = true
		m.ready = false
		return m.emitCheckpointList()
	}
	return m.afterRestore()
}

func (m *Replica) afterRestore() []core.Action {
	if m.config.CatchUpEvery > 0 {
		m.booting = true
		m.ready = false
		return m.armCatchUp()
	}
	return m.finishBoot()
}

func (m *Replica) finishBoot() []core.Action {
	m.booting = false
	m.ready = true
	m.lastOtherAt, m.lastPullOK, m.lastOwnAt = m.now, m.now, m.now
	var acts []core.Action
	acts = append(acts, m.claimNow()...)
	acts = append(acts, m.armRole()...)
	acts = append(acts, m.armCheckpoint()...)
	return acts
}

func (m *Replica) maybeBecomeReady() []core.Action {
	if !m.booting || m.pending != 0 || m.applied < m.goalTail {
		return nil
	}
	// A restore is not over until the checkpoint is read and a tail check
	// finished after it. An append completion alone must never make the node ready.
	if m.checkpointRetry || m.needsCheckpoint || (m.config.CatchUpEvery > 0 && !m.tailChecked) {
		return nil
	}
	return m.finishBoot()
}

func (m *Replica) onTimer(e core.TimerFired) []core.Action {
	switch e.ID {
	case timerCatchUp:
		if e.Gen != m.timerGen || m.pending != 0 || m.config.CatchUpEvery <= 0 {
			return nil
		}
		return m.emitCheckTail()
	case timerRole:
		return m.onRoleTimer(e)
	case timerCheckpoint:
		return m.onCheckpointTimer(e)
	default:
		return nil
	}
}

func (m *Replica) onCheckTailDone(d core.LogGetEndDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingTail {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != core.OK || d.Outcome != core.Applied {
		return m.armCatchUp()
	}
	m.notePullOK()
	m.tailChecked = true
	m.goalTail = d.Tail
	m.noteTail(m.tailRound, d.Tail)
	return m.progress()
}

func (m *Replica) onReadDone(d core.LogReadDone) []core.Action {
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
		m.buf[e.Slot] = bytes.Clone(e.Data)
		m.observeSlot(e.Slot)
	}
	// The writer appends one slot at a time and retries a slot until it knows
	// the result, so the log has no empty slot below its tail. LogRead starts at
	// m.applied and returns only slots that exist: if the next slot is missing but a
	// later one exists, this node fell behind a trim.
	if len(d.Entries) > 0 && m.nextSlotMissing() {
		return m.hitTrimmed()
	}
	if err := m.drain(); err != nil {
		return m.trip(err)
	}
	var acts []core.Action
	if m.fencedFlag {
		acts = m.stepDown()
	}
	acts = append(acts, m.progress()...)
	return append(acts, m.flushCommits()...)
}

func (m *Replica) onAppendDone(d core.LogAppendDone) []core.Action {
	if f, ok := m.inFlightAppends[d.Req]; ok {
		return m.onCommitAppendDone(d, f)
	}
	return nil
}

func (m *Replica) drain() error {
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
	}
}

func (m *Replica) progress() []core.Action {
	if m.pending != 0 {
		return nil
	}
	if m.applied < m.goalTail || m.nextSlotMissing() {
		return m.emitRead()
	}
	if acts := m.maybeBecomeReady(); acts != nil {
		var out []core.Action
		out = append(out, acts...)
		out = append(out, m.armCatchUp()...)
		return out
	}
	return m.armCatchUp()
}

// nextSlotMissing reports that a later slot is buffered (one of our own
// appends landed) while the next slot to apply has not been read yet.
func (m *Replica) nextSlotMissing() bool {
	if len(m.buf) == 0 {
		return false
	}
	_, ok := m.buf[m.applied]
	return !ok
}

func (m *Replica) emitCheckTail() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingTail
	m.tailsStarted++
	m.tailRound = m.tailsStarted
	return []core.Action{core.LogGetEnd{Req: m.pending}}
}

func (m *Replica) emitRead() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingRead
	return []core.Action{core.LogRead{Req: m.pending, From: m.applied, Limit: m.config.readLimit()}}
}

func (m *Replica) armCatchUp() []core.Action {
	if m.config.CatchUpEvery <= 0 {
		return nil
	}
	m.timerGen++
	return []core.Action{core.SetTimer{ID: timerCatchUp, Gen: m.timerGen, After: m.config.CatchUpEvery}}
}

func (m *Replica) trip(err error) []core.Action {
	m.exited = true
	return []core.Action{core.StopForRestart{Cause: err.Error()}}
}

// stop marks the machine shutting down and emits ShutdownDone. With no in-flight work
// yet that must drain, ShutdownDone is immediate (decision 8).
func (m *Replica) stop() []core.Action {
	if m.stopping {
		return nil
	}
	m.stopping = true
	m.exited = true
	return []core.Action{core.ShutdownDone{}}
}

// StateHash folds every field that influences behaviour, including the applied
// metadata store digest (comparable across machines that folded the same prefix).
func (m *Replica) StateHash() uint64 {
	h := core.NewHasher()
	h.Bool(m.started)
	h.Bool(m.stopping)
	h.Bool(m.exited)
	h.Bool(m.booting)
	h.Bool(m.ready)
	h.Bool(m.active)
	h.Uint64Value(uint64(m.timerGen))
	h.Uint64Value(uint64(m.roleGen))
	h.Uint64Value(uint64(m.checkpointGen))
	h.Uint64Value(uint64(m.nextReq))
	h.Uint64Value(uint64(m.seq))
	h.Uint64Value(uint64(m.applied))
	h.Uint64Value(uint64(m.goalTail))
	h.Uint64Value(uint64(m.high))
	h.Uint64Value(uint64(m.pending))
	h.Uint64Value(uint64(m.pendingKind))
	h.StringValue(m.restoreKey)
	h.Bool(m.checkpointRetry)
	h.Uint64Value(m.epoch)
	h.Uint64Value(m.maxEpochSeen)
	h.Uint64Value(uint64(m.claimReq))
	h.Bool(m.fencedFlag)
	h.Uint64Value(m.stepDowns)
	h.Bool(m.tailChecked)
	h.Uint64Value(uint64(m.checkpointSlot))
	h.Uint64Value(uint64(m.checkpointRequest))
	h.Uint64Value(uint64(len(m.queue)))
	h.Uint64Value(uint64(len(m.inFlightAppends)))
	h.Uint64Value(uint64(len(m.waiters)))
	h.Uint64Value(uint64(len(m.readWaiters)))
	h.Uint64Value(m.tailsStarted)
	h.Uint64Value(uint64(m.config.CatchUpEvery))
	h.Uint64Value(uint64(m.config.ReadLimit))
	h.Uint64Value(uint64(m.config.MaxCmdsPerSlot))
	h.Uint64Value(uint64(m.config.QueueMax))
	h.Uint64Value(uint64(m.config.Rank))
	h.Uint64Value(uint64(m.config.Heartbeat))
	h.Uint64Value(uint64(m.config.TakeoverAfter))
	h.Uint64Value(uint64(m.config.CheckpointEvery))
	h.Uint64Value(uint64(m.config.CheckpointMinSlots))
	h.Uint64Value(uint64(m.config.KeepSlots))
	h.Uint64Value(m.fold.StateHash())
	return h.Sum()
}
