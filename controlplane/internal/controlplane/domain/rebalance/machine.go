// Package rebalance is the consumer group protocol (classic Kafka) as a pure
// state machine in the shape of a dst sim.Process. It has no locks,
// goroutines, channels or clock: time and randomness come from the step
// context, log writes and reads are outputs, and their answers are inputs.
// docs/rebalance.md is the design; the rule numbers in comments are its own.
//
// The machine is Standby (it serves nothing), Loading (it reads every group
// from the log) or Serving. The log holds decisions only. Phases, parked
// calls and timers are memory only, and are dropped when the role is lost or
// when a write that may have changed the log fails (a broken term).
package rebalance

import (
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
)

// Config holds the machine settings. A zero field takes its default.
type Config struct {
	// ID is the process id (default "rebalance").
	ID sim.ProcessID
	// InitialDelay is how long a new group waits for more members (default 3 s).
	InitialDelay machine.Duration
	// NewMemberTimeout is how long a new member may wait for its first round (default 5 min).
	NewMemberTimeout machine.Duration
	// WriteTimeout bounds one log write (default 10 s).
	WriteTimeout machine.Duration
	// LoadRetry is the wait before a refused load read is sent again (default 100 ms).
	LoadRetry machine.Duration
	// Faults are the buggify sites (sites.go). Nil means none fire: production
	// passes nil, the simulator passes its fault injector.
	Faults machine.Faults
}

// Machine is the group coordinator of one node.
type Machine struct {
	cfg       Config
	state     any // *standby, *loading or *serving
	nextReq   machine.RequestID
	nextTimer machine.TimerID

	// Set for the length of one Step only.
	cx  sim.Context
	now machine.NodeTime
	out []any
}

// standby serves nothing.
type standby struct{}

// serving is the memory of one term.
type serving struct {
	groups    map[string]*group
	names     []string // the group names, sorted
	reqs      map[machine.RequestID]any
	owners    map[machine.TimerID]timerRef
	where     map[CallID]string // the group of each parked, queued or writing group call
	describes []*describe
	commits   []*commit
}

var _ sim.Process = (*Machine)(nil)

// New returns a machine in Standby.
func New(cfg Config) *Machine {
	if cfg.ID == "" {
		cfg.ID = defaultProcess
	}
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = 3 * machine.Second
	}
	if cfg.NewMemberTimeout <= 0 {
		cfg.NewMemberTimeout = 300 * machine.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * machine.Second
	}
	if cfg.LoadRetry <= 0 {
		cfg.LoadRetry = 100 * machine.Millisecond
	}
	return &Machine{cfg: cfg, state: &standby{}}
}

// ID implements sim.Process.
func (m *Machine) ID() sim.ProcessID { return m.cfg.ID }

// Step implements sim.Process: it takes one input and returns the outputs.
func (m *Machine) Step(cx sim.Context, inbox any) []any {
	m.cx, m.now, m.out = cx, machine.NodeTime(cx.Now()), nil
	switch s := m.state.(type) {
	case *serving:
		m.serve(s, inbox)
	case *loading:
		m.loadStep(s, inbox)
	default:
		m.standbyStep(inbox)
	}
	out := m.out
	m.cx, m.out = nil, nil
	return out
}

func (m *Machine) standbyStep(inbox any) {
	if r, ok := inbox.(RoleChanged); ok && r.Active {
		m.startLoad(false)
		return
	}
	m.refuse(inbox)
}

// refuse answers a call when the machine does not serve (rules 1 and 2).
func (m *Machine) refuse(inbox any) {
	if j, ok := inbox.(Join); ok && invalidJoin(j) {
		m.reply(j.Call, runtime.ErrInvalid, nil)
		return
	}
	if call, ok := callOf(inbox); ok {
		m.reply(call, runtime.ErrUnavailable, nil)
	}
}

func (m *Machine) serve(s *serving, inbox any) {
	switch in := inbox.(type) {
	case RoleChanged:
		if !in.Active {
			m.dropMemory(s, nil)
			m.state = &standby{}
		}
	case Join:
		if invalidJoin(in) {
			m.reply(in.Call, runtime.ErrInvalid, nil)
			return
		}
		m.groupInput(s, in.Group, in.Call, in)
	case Sync:
		m.groupInput(s, in.Group, in.Call, in)
	case Heartbeat:
		m.groupInput(s, in.Group, in.Call, in)
	case Leave:
		m.groupInput(s, in.Group, in.Call, in)
	case OffsetCommit:
		m.commit(s, in)
	case Describe:
		m.describe(s, in)
	case List:
		m.list(s, in)
	case CallAbandoned:
		m.abandon(s, in.Call)
	case runtime.TimerFired:
		m.timerFired(s, in)
	case ReplicaReply:
		m.replicaReply(s, in)
	case drainLater:
		m.drainNow(s, in)
	}
}

// groupInput handles a call for one group, or queues it while the group
// waits for a log write (section 8).
func (m *Machine) groupInput(s *serving, name string, call CallID, in any) {
	if g := s.groups[name]; g != nil && g.busy() {
		s.where[call] = name
		g.queue = append(g.queue, in)
		return
	}
	m.handle(s, in)
}

// handle runs one group input on a group that is not busy.
func (m *Machine) handle(s *serving, in any) {
	switch in := in.(type) {
	case Join:
		m.join(s, in)
	case Sync:
		m.syncGroup(s, in)
	case Heartbeat:
		m.heartbeat(s, in)
	case Leave:
		m.leave(s, in)
	case runtime.TimerFired:
		m.groupTimer(s, in)
	case CallAbandoned:
		m.abandonParked(s, in.Call)
	}
}

func invalidJoin(j Join) bool {
	return j.Group == "" || j.ProtocolType == "" || len(j.Protocols) == 0 || j.SessionTimeoutMs <= 0
}

func callOf(inbox any) (CallID, bool) {
	switch in := inbox.(type) {
	case Join:
		return in.Call, true
	case Sync:
		return in.Call, true
	case Heartbeat:
		return in.Call, true
	case Leave:
		return in.Call, true
	case OffsetCommit:
		return in.Call, true
	case Describe:
		return in.Call, true
	case List:
		return in.Call, true
	}
	return 0, false
}

// reply answers call. A call that gets its reply is no longer waiting.
func (m *Machine) reply(call CallID, err runtime.ErrCode, body Answer) {
	if s, ok := m.state.(*serving); ok {
		delete(s.where, call)
	}
	m.out = append(m.out, Reply{Call: call, Err: err, Body: body})
}

// submit sends a command to the log and returns its request id.
func (m *Machine) submit(op runtime.ControlPlaneOperation) machine.RequestID {
	m.nextReq++
	m.out = append(m.out, SubmitCommand{Req: m.nextReq, Op: op})
	return m.nextReq
}

// read sends a read to the log and returns its request id.
func (m *Machine) read(op runtime.ControlPlaneOperation, fresh bool) machine.RequestID {
	m.nextReq++
	m.out = append(m.out, SubmitRead{Req: m.nextReq, Op: op, Fresh: fresh})
	return m.nextReq
}

// wallMs is the wall clock in milliseconds, for TimeMs in logged commands.
func (m *Machine) wallMs() int64 { return int64(m.cx.Wall()) / int64(machine.Millisecond) }

// send delivers body to the machine itself, as a later input.
func (m *Machine) send(body any) {
	m.out = append(m.out, sim.Send{To: m.cfg.ID, Body: body})
}

// newServing returns empty memory for a new term.
func newServing() *serving {
	return &serving{
		groups: map[string]*group{},
		reqs:   map[machine.RequestID]any{},
		owners: map[machine.TimerID]timerRef{},
		where:  map[CallID]string{},
	}
}

// newGroup adds an empty group to the memory.
func (m *Machine) newGroup(s *serving, name string) *group {
	g := &group{name: name, phase: empty{}, round: m.newTimer(), delay: m.newTimer()}
	s.owners[g.round.id] = timerRef{kind: timerRound, group: name}
	s.owners[g.delay.id] = timerRef{kind: timerDelay, group: name}
	s.groups[name] = g
	s.names = insertName(s.names, name)
	return g
}

// newMember adds a member to g with its session timer slot.
func (m *Machine) newMember(s *serving, g *group, id string) *member {
	g.joinSeq++
	mm := &member{id: id, order: g.joinSeq, session: m.newTimer()}
	s.owners[mm.session.id] = timerRef{kind: timerSession, group: g.name, member: id}
	g.insert(mm)
	return mm
}

// ms is a duration of n milliseconds.
func ms(n int32) machine.Duration { return machine.Duration(n) * machine.Millisecond }

// after is the time from now until at, never below zero.
func (m *Machine) after(at machine.NodeTime) machine.Duration {
	return machine.Duration(max(at-m.now, 0))
}

// timeNs converts a duration for sim.SetTimer.
func timeNs(d machine.Duration) sched.TimeNs { return sched.TimeNs(d) }

// describeOp is the read of one group.
func describeOp(name string) contract.DescribeGroup { return contract.DescribeGroup{Group: name} }
