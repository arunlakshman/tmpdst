package replica

import (
	"bytes"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
	"github.com/manifesterio/mds/dst/trace"
)

const (
	timerCatchUp = machine.TimerID(1)
)

// Replica is the control-plane DSM. Build it with New.
type Replica struct {
	env    runtime.Environment
	self   machine.NodeInstance
	config Config

	openErr error // the first store could not open; NodeStarted stops the machine

	started  bool
	stopping bool
	exited   bool
	booting  bool // restore/catch-up in progress; Ready flips when done
	ready    bool
	active   bool // advisory appender role

	meta *metadb.DB

	now           machine.NodeTime
	timerGen      machine.TimerVersion
	roleGen       machine.TimerVersion
	checkpointGen machine.TimerVersion
	nextReq       machine.RequestID
	seq           uint64
	applied       runtime.LogPosition // next slot to apply
	goalTail      runtime.LogPosition // CheckTail answer for the current round
	high          runtime.LogPosition // next slot to allocate for commit appends
	buf           map[runtime.LogPosition][]byte
	pending       machine.RequestID
	epoch         uint64            // writer epoch this node holds; zero when it is not the writer
	claimReq      machine.RequestID // in-flight LogClaimEpoch; 0 means none
	claimExpect   uint64            // epoch that claim expected to find
	fencedFlag    bool              // a newer epoch was seen while applying; step down after the drain
	stepDowns     uint64            // times this node stopped being the writer because of a newer epoch
	tailChecked   bool              // a tail check finished since the last restore
	maxEpochSeen  uint64            // highest writer epoch seen in applied log entries
	restoreKey    string            // checkpoint object being read
	pendingKind   pendingKind

	lastPullOK  machine.NodeTime
	lastOtherAt machine.NodeTime
	lastOwnAt   machine.NodeTime

	checkpointSlot    runtime.LogPosition // newest checkpoint slot (written or restored)
	checkpointAt      machine.NodeTime
	checkpointPending runtime.LogPosition // slot of in-flight ObjectPut snapshot
	checkpointData    []byte
	// The checkpoint lane: the snapshot upload, then the log trim, one request at
	// a time, on its own request id. It never uses pending, so a slow upload
	// cannot block catch-up and catch-up cannot block the checkpoint.
	checkpointRequest machine.RequestID // in-flight request of the lane; 0 means idle
	checkpointStage   checkpointStage
	trimThrough       runtime.LogPosition // Through of the in-flight trim

	queue           []waitingCommand                      // client ops waiting for a commit slot
	inFlightAppends map[machine.RequestID]*inFlightAppend // in-flight commit LogAppends (at most one)
	waiters         map[string][]waitingCommand
	answers         []ReplyToClient
	trimFloor       runtime.LogPosition // slots below are trimmed
	stallUntil      machine.NodeTime
	downUntil       machine.NodeTime
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

// Factory builds a replica for one incarnation of a node.
type Factory func(env runtime.Environment, self machine.NodeInstance) *Replica

// New returns the Factory of the control-plane machine. A factory
// cannot return an error, so a store that cannot open (Config.OpenStore) is
// remembered and the machine stops with Crash on NodeStarted: the
// process cannot serve without its state.
func New(config Config) Factory {
	return func(env runtime.Environment, self machine.NodeInstance) *Replica {
		meta, err := config.openMetadata()
		if err != nil {
			meta = metadb.NewMem() // placeholder; NodeStarted stops the machine
		}
		return &Replica{
			env: env, self: self, config: config, openErr: err,
			meta:            meta,
			buf:             map[runtime.LogPosition][]byte{},
			inFlightAppends: map[machine.RequestID]*inFlightAppend{},
			waiters:         map[string][]waitingCommand{},
		}
	}
}

// Metadata is the metadb after the applied prefix. Tests compare its
// digest to an independent oracle.
func (m *Replica) Metadata() *metadb.DB { return m.meta }

// Applied is the next slot to apply (slots below are folded).
func (m *Replica) Applied() runtime.LogPosition { return m.applied }

// SeedTopic pre-creates a topic on the fold before NodeStarted (sim campaign setup).
func (m *Replica) SeedTopic(name string, partitions int32) runtime.ErrCode {
	err := m.meta.CreateTopic(name, partitions)
	if err == runtime.OK {
		m.seeds = append(m.seeds, topicSeed{name: name, parts: partitions})
	}
	return err
}

func (m *Replica) reseedTopics() {
	for _, t := range m.seeds {
		_ = m.meta.CreateTopic(t.name, t.parts)
	}
}

var _ sim.Process = (*Replica)(nil)

// ID implements sim.Process: the name of the node.
func (m *Replica) ID() sim.ProcessID { return sim.ProcessID(m.self.Node) }

// Step implements sim.Process. The inbox value must be a runtime.EventData. It
// returns what the machine wants done, in order: dst values (timers, Crash,
// Exit) and IO calls. Unknown completions and unhandled values leave the state
// unchanged. After Exit or Crash, every value is a no-op.
func (m *Replica) Step(cx sim.Context, inbox any) []any {
	if m.exited {
		return nil
	}
	m.now = machine.NodeTime(cx.Now())
	var acts []any
	switch body := inbox.(type) {
	case runtime.NodeStarted:
		acts = m.onStarted()
	case runtime.StopRequest:
		acts = m.stop()
	case runtime.TimerFired:
		acts = m.onTimer(body)
	case runtime.LogGetEndDone:
		acts = m.onCheckTailDone(body)
	case runtime.LogReadDone:
		acts = m.onReadDone(body)
	case runtime.LogAppendDone:
		acts = m.onAppendDone(body)
	case runtime.LogClaimEpochDone:
		acts = m.onClaimDone(body)
	case runtime.ObjectListDone:
		acts = m.onObjListDone(body)
	case runtime.ObjectGetDone:
		acts = m.onObjGetDone(body)
	case runtime.ObjectPutDone:
		acts = m.onObjPutDone(body)
	case runtime.LogDeleteOldDone:
		acts = m.onTrimDone(body)
	case runtime.ControlPlaneDeliver:
		return m.onControlPlaneDeliver(body) // already withReplies
	case runtime.LocalRead:
		return m.onLocalRead(body)
	default:
		return nil
	}
	acts = append(acts, m.serveReads()...)
	return m.withReplies(acts)
}

func (m *Replica) onStarted() []any {
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

func (m *Replica) afterRestore() []any {
	if m.config.CatchUpEvery > 0 {
		m.booting = true
		m.ready = false
		return m.armCatchUp()
	}
	return m.finishBoot()
}

func (m *Replica) finishBoot() []any {
	m.booting = false
	m.ready = true
	m.lastOtherAt, m.lastPullOK, m.lastOwnAt = m.now, m.now, m.now
	var acts []any
	acts = append(acts, m.claimNow()...)
	acts = append(acts, m.armRole()...)
	acts = append(acts, m.armCheckpoint()...)
	return acts
}

func (m *Replica) maybeBecomeReady() []any {
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

func (m *Replica) onTimer(e runtime.TimerFired) []any {
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

func (m *Replica) onCheckTailDone(d runtime.LogGetEndDone) []any {
	if d.Req != m.pending || m.pendingKind != pendingTail {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		return m.armCatchUp()
	}
	m.notePullOK()
	m.tailChecked = true
	m.goalTail = d.Tail
	m.noteTail(m.tailRound, d.Tail)
	return m.progress()
}

func (m *Replica) onReadDone(d runtime.LogReadDone) []any {
	if d.Req != m.pending || m.pendingKind != pendingRead {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
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
	var acts []any
	if m.fencedFlag {
		acts = m.stepDown()
	}
	acts = append(acts, m.progress()...)
	return append(acts, m.flushCommits()...)
}

func (m *Replica) onAppendDone(d runtime.LogAppendDone) []any {
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

func (m *Replica) progress() []any {
	if m.pending != 0 {
		return nil
	}
	if m.applied < m.goalTail || m.nextSlotMissing() {
		return m.emitRead()
	}
	if acts := m.maybeBecomeReady(); acts != nil {
		var out []any
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

func (m *Replica) emitCheckTail() []any {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingTail
	m.tailsStarted++
	m.tailRound = m.tailsStarted
	return []any{runtime.LogGetEnd{Req: m.pending}}
}

func (m *Replica) emitRead() []any {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingRead
	return []any{runtime.LogRead{Req: m.pending, From: m.applied, Limit: m.config.readLimit()}}
}

func (m *Replica) armCatchUp() []any {
	if m.config.CatchUpEvery <= 0 {
		return nil
	}
	m.timerGen++
	return []any{arm(timerCatchUp, m.timerGen, m.config.CatchUpEvery)}
}

// arm returns the dst value that starts timer id. The generation goes back to
// the machine inside the TimerFired value, so a stale timer is ignored.
func arm(id machine.TimerID, gen machine.TimerVersion, after machine.Duration) sim.SetTimer {
	return sim.SetTimer{ID: uint64(id), After: sched.TimeNs(after), Body: runtime.TimerFired{ID: id, Gen: gen}}
}

// trip ends the machine: its memory can not be trusted any more, so the
// runner must start a new incarnation.
func (m *Replica) trip(err error) []any {
	m.exited = true
	return []any{sim.Crash{Cause: err.Error()}}
}

// stop marks the machine shutting down and emits Exit. With no in-flight work
// yet that must drain, Exit is immediate (decision 8).
func (m *Replica) stop() []any {
	if m.stopping {
		return nil
	}
	m.stopping = true
	m.exited = true
	return []any{sim.Exit{}}
}

// StateHash folds every field that influences behaviour, including the applied
// metadb digest (comparable across machines that folded the same prefix).
func (m *Replica) StateHash() uint64 {
	h := trace.New()
	h.Bool(m.started)
	h.Bool(m.stopping)
	h.Bool(m.exited)
	h.Bool(m.booting)
	h.Bool(m.ready)
	h.Bool(m.active)
	h.Uint64(uint64(m.timerGen))
	h.Uint64(uint64(m.roleGen))
	h.Uint64(uint64(m.checkpointGen))
	h.Uint64(uint64(m.nextReq))
	h.Uint64(uint64(m.seq))
	h.Uint64(uint64(m.applied))
	h.Uint64(uint64(m.goalTail))
	h.Uint64(uint64(m.high))
	h.Uint64(uint64(m.pending))
	h.Uint64(uint64(m.pendingKind))
	h.String(m.restoreKey)
	h.Bool(m.checkpointRetry)
	h.Uint64(m.epoch)
	h.Uint64(m.maxEpochSeen)
	h.Uint64(uint64(m.claimReq))
	h.Bool(m.fencedFlag)
	h.Uint64(m.stepDowns)
	h.Bool(m.tailChecked)
	h.Uint64(uint64(m.checkpointSlot))
	h.Uint64(uint64(m.checkpointRequest))
	h.Uint64(uint64(len(m.queue)))
	h.Uint64(uint64(len(m.inFlightAppends)))
	h.Uint64(uint64(len(m.waiters)))
	h.Uint64(uint64(len(m.readWaiters)))
	h.Uint64(m.tailsStarted)
	h.Uint64(uint64(m.config.CatchUpEvery))
	h.Uint64(uint64(m.config.ReadLimit))
	h.Uint64(uint64(m.config.MaxCmdsPerSlot))
	h.Uint64(uint64(m.config.QueueMax))
	h.Uint64(uint64(m.config.Rank))
	h.Uint64(uint64(m.config.Heartbeat))
	h.Uint64(uint64(m.config.TakeoverAfter))
	h.Uint64(uint64(m.config.CheckpointEvery))
	h.Uint64(uint64(m.config.CheckpointMinSlots))
	h.Uint64(uint64(m.config.KeepSlots))
	h.Uint64(m.meta.StateHash())
	return h.Sum()
}
