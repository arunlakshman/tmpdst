package sim

import (
	"fmt"

	"manifester/toys3/core"
	"manifester/toys3/format"
)

// Simulator is one run in progress. It implements View for invariants.
// Everything is owned by the single goroutine that calls Run.
type Simulator struct {
	cfg  Config
	rng  *Rand
	now  core.Time
	step int
	q    Queue

	fs     *FakeFS
	server core.Machine // nil while crashed
	inc    Incarnation  // current (or last) incarnation; 0 before first boot
	alive  bool
	quiet  bool

	workload *Workload
	oracle   *Oracle
	facts    []TimedFact

	env       *env
	parked    []bool // idle clients waiting for a restart or the end of a quiesce
	quiescing bool   // clients hold new requests until every request replied
	stopped   bool   // QuietTail is over: clients issue nothing more
	halted    bool   // a failure stops the run

	trace uint64 // running trace hash
	res   Result
}

// New prepares a run: seeds the Rand, builds the FakeFS with the obj/ and tmp/
// directories of cfg.Server.Dir, the workload and the oracle, and schedules
// the first boot, the clients' first wakeups and the end of Chaos.
func New(cfg Config) *Simulator {
	if cfg.Factory == nil {
		cfg.Factory = core.NewMachine
	}
	if cfg.Invariants == nil {
		cfg.Invariants = DefaultInvariants()
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 2_000_000
	}
	if cfg.Server.Dir == "" {
		cfg.Server.Dir = "/data"
	}
	rng := NewRand(cfg.Seed)
	s := &Simulator{
		cfg:      cfg,
		rng:      rng,
		fs:       NewFakeFS(rng, cfg.FS, format.ObjDir(cfg.Server.Dir), format.TmpDir(cfg.Server.Dir)),
		workload: NewWorkload(cfg.Workload, rng),
		oracle:   NewOracle(),
		env:      &env{rng: rng, buggify: true},
		trace:    cfg.Seed,
	}
	s.parked = make([]bool, s.workload.Clients())
	s.res.Seed = cfg.Seed
	s.schedule(Entry{At: 0, Kind: KindRestart})
	for c := range s.parked {
		s.schedule(Entry{At: core.Time(s.workload.Think()), Kind: KindClientWake, Client: c})
	}
	if cfg.Workload.QuiesceEvery > 0 {
		s.schedule(Entry{At: core.Time(cfg.Workload.QuiesceEvery), Kind: KindQuiesce})
	}
	s.schedule(Entry{At: core.Time(cfg.Chaos), Kind: KindQuiet})
	s.schedule(Entry{At: core.Time(cfg.Chaos + cfg.QuietTail), Kind: KindTailEnd})
	return s
}

func (s *Simulator) schedule(e Entry) { s.q.Schedule(s.rng, s.now, e) }

// Run executes the main loop (see the package doc) until the queue drains or
// MaxSteps is hit, then runs AtEnd invariants and returns the Result.
func (s *Simulator) Run() Result {
	for !s.halted {
		e, ok := s.q.Pop()
		if !ok {
			break
		}
		if s.step >= s.cfg.MaxSteps {
			s.fail(Failure{Invariant: "max-steps", Msg: fmt.Sprintf("run did not drain within %d steps", s.cfg.MaxSteps)})
			break
		}
		s.step++
		s.now = e.At
		s.trace = Fold(Fold(Fold(Fold(s.trace, uint64(s.step)), uint64(s.now)), uint64(e.Kind)), uint64(e.Inc))
		s.handle(e)
		s.trace = Fold(s.trace, s.fs.Hash())
		if s.halted {
			break
		}
		s.afterStep()
	}
	if !s.halted {
		for _, inv := range s.cfg.Invariants {
			for _, f := range inv.AtEnd(s) {
				s.addFailure(inv.Name(), f)
			}
		}
	}
	s.res.Steps = s.step
	s.res.EndTime = s.now
	s.res.TraceHash = s.trace
	s.res.RandCalls = s.rng.Calls()
	fc := s.fs.Counters()
	s.res.Stats.FSOps = fc.Landed
	s.res.Stats.EIOs = fc.EIOs
	s.res.Stats.ReadCorruptions = fc.ReadCorruptions
	ost := s.oracle.Stats()
	s.res.Stats.Windows = ost.Windows
	s.res.Stats.MaxWindowOps = ost.MaxWindowOps
	s.res.Stats.Widenings = ost.Widenings
	s.res.Stats.Crashes = s.res.Crashes
	return s.res
}

func (s *Simulator) handle(e Entry) {
	switch e.Kind {
	case KindDeliver:
		if e.Inc != s.inc || !s.alive {
			s.note(e, "dropped: dead incarnation")
			return
		}
		s.deliver(e.Event)
	case KindFSLand:
		if e.Inc != s.inc || !s.alive {
			s.note(e, "dropped: dead incarnation")
			return
		}
		body := s.fs.Land(e.Action, e.Plan)
		s.trace = hashEvent(Fold(s.trace, 1), body)
		if s.cfg.Log {
			s.note(e, "land "+DescribeAction(e.Action)+" -> "+DescribeEvent(body))
		}
		s.schedule(Entry{At: s.now + core.Time(e.Plan.Reply), Kind: KindDeliver, Inc: s.inc, Event: body})
		s.maybeCrash()
	case KindClientWake:
		s.wake(e.Client)
	case KindCrash:
		if s.alive {
			s.crash("scheduled crash")
		}
	case KindRestart:
		s.restart()
	case KindQuiesce:
		if !s.stopped {
			s.quiescing = true
			s.note(e, "quiesce")
		}
	case KindQuiet:
		s.quiet = true
		s.fs.SetQuiet(true)
		s.env.buggify = false
		s.note(e, "chaos over: no more faults or crashes")
	case KindTailEnd:
		s.stopped = true
		s.note(e, "quiet tail over: clients stop")
	}
}

// deliver steps the server with one event and executes its actions.
func (s *Simulator) deliver(body core.EventBody) {
	acts, panicMsg := s.callStep(body)
	s.trace = hashEvent(s.trace, body)
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindDeliver, Event: body, Actions: acts})
	}
	if panicMsg != "" {
		s.fail(Failure{Invariant: "panic", Msg: "server panicked in Step(" + DescribeEvent(body) + "): " + panicMsg})
		return
	}
	for _, a := range acts {
		s.trace = hashAction(s.trace, a)
	}
	s.trace = Fold(s.trace, s.server.StateHash())
	for i, a := range acts {
		switch x := a.(type) {
		case core.Actuation:
			plan := s.fs.Plan(a)
			s.schedule(Entry{At: s.now + core.Time(plan.Land), Kind: KindFSLand, Inc: s.inc, Action: a, Plan: plan})
		case core.Reply:
			s.reply(x)
		case core.Emit:
			s.facts = append(s.facts, TimedFact{Step: s.step, At: s.now, Inc: s.inc, Fact: x.Fact})
			switch f := x.Fact.(type) {
			case core.CorruptObject:
				s.res.Stats.CorruptObjects++
			case core.Recovered:
				s.res.Stats.TempsRemoved += f.TempsRemoved
			}
		case core.Halt:
			if i != len(acts)-1 {
				s.fail(Failure{Invariant: "protocol", Msg: "Halt is not the last action of its step"})
				return
			}
			s.res.Stats.Halts++
			s.crash("halt: " + x.Reason)
			return
		default:
			s.fail(Failure{Invariant: "protocol", Msg: fmt.Sprintf("unknown action %T", a)})
			return
		}
		if s.halted {
			return
		}
	}
	s.maybeCrash()
}

// callStep runs server.Step, turning a panic into a message.
func (s *Simulator) callStep(body core.EventBody) (acts []core.Action, panicMsg string) {
	defer func() {
		if r := recover(); r != nil {
			acts, panicMsg = nil, fmt.Sprint(r)
		}
	}()
	return s.server.Step(core.Event{At: s.now, Body: body}), ""
}

func (s *Simulator) reply(r core.Reply) {
	okOracle := s.oracle.Return(s.now, r)
	client, okClient := s.workload.Complete(r)
	if !okOracle || !okClient {
		s.fail(Failure{Invariant: "protocol", Msg: fmt.Sprintf("reply to unknown or already answered request c%d", r.Client)})
		return
	}
	s.res.Stats.ByStatus[statusIndex(r.Status)]++
	s.schedule(Entry{At: s.now + core.Time(s.workload.Think()), Kind: KindClientWake, Client: client})
}

// wake lets an idle client issue its next request, or parks it.
func (s *Simulator) wake(c int) {
	if s.workload.Busy(c) {
		return
	}
	if s.stopped || s.quiescing || !s.alive {
		s.parked[c] = true
		return
	}
	req := s.workload.Issue(c, s.now)
	s.res.Stats.Requests++
	s.oracle.Invoke(s.now, c, req)
	s.trace = hashEvent(Fold(s.trace, 2), req)
	s.schedule(Entry{At: s.now, Kind: KindDeliver, Inc: s.inc, Event: req})
}

func (s *Simulator) wakeParked() {
	for c, p := range s.parked {
		if p {
			s.parked[c] = false
			s.schedule(Entry{At: s.now + core.Time(s.workload.Think()), Kind: KindClientWake, Client: c})
		}
	}
}

// maybeCrash injects a crash during Chaos with probability CrashProb.
func (s *Simulator) maybeCrash() {
	if s.alive && !s.quiet && s.rng.Chance(s.cfg.CrashProb) {
		s.crash("injected crash")
	}
}

// crash kills the current incarnation: the adversary resolves volatile
// state, in-flight FS actions and completions are dropped (un-landed actions
// never happen), outstanding requests are abandoned, and a restart is
// scheduled.
func (s *Simulator) crash(why string) {
	rep := s.fs.Crash()
	s.res.Stats.TornFiles += rep.Torn
	s.res.Stats.LostChanges += rep.Lost
	s.res.Stats.KeptChanges += rep.Kept
	dropped := s.q.DropIncarnation(s.inc)
	midPut := false
	for _, id := range s.workload.Crash() {
		s.res.Stats.Indetermined++
		if op := s.oracle.op(id); op != nil {
			switch op.Op.(type) {
			case core.Put, core.Copy:
				midPut = true
			}
			s.parked[op.Client] = true
		}
	}
	if midPut {
		s.res.Stats.CrashesMidPut++
	}
	s.oracle.Crash(s.now)
	s.alive = false
	s.server = nil
	s.res.Crashes++
	s.trace = Fold(Fold(Fold(Fold(s.trace, 3), uint64(rep.Kept)), uint64(rep.Lost)), uint64(rep.Torn))
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindCrash,
			Note: fmt.Sprintf("CRASH (%s): kept=%d lost=%d torn=%d, dropped %d queued entries", why, rep.Kept, rep.Lost, rep.Torn, dropped)})
	}
	delay := s.rng.Between(int64(s.cfg.RestartDelay[0]), int64(s.cfg.RestartDelay[1]))
	s.schedule(Entry{At: s.now + core.Time(delay), Kind: KindRestart})
}

// restart boots a new incarnation and delivers Started.
func (s *Simulator) restart() {
	s.inc++
	m, panicMsg := s.build()
	if panicMsg != "" {
		s.fail(Failure{Invariant: "panic", Msg: "Factory panicked: " + panicMsg})
		return
	}
	s.server = m
	s.alive = true
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindRestart, Note: "boot"})
	}
	s.schedule(Entry{At: s.now, Kind: KindDeliver, Inc: s.inc, Event: core.Started{}})
	if !s.quiescing {
		s.wakeParked()
	}
}

func (s *Simulator) build() (m core.Machine, panicMsg string) {
	defer func() {
		if r := recover(); r != nil {
			m, panicMsg = nil, fmt.Sprint(r)
		}
	}()
	return s.cfg.Factory(s.env, s.cfg.Server), ""
}

// afterStep ends a quiesce once every request has replied, then runs the
// per-step invariants.
func (s *Simulator) afterStep() {
	if s.quiescing && s.workload.Idle() {
		s.quiescing = false
		if !s.stopped {
			s.schedule(Entry{At: s.now + core.Time(s.cfg.Workload.QuiesceEvery), Kind: KindQuiesce})
		}
		if s.alive {
			s.wakeParked()
		}
	}
	for _, inv := range s.cfg.Invariants {
		for _, f := range inv.AfterStep(s) {
			s.addFailure(inv.Name(), f)
		}
	}
}

func (s *Simulator) addFailure(name string, f Failure) {
	if f.Invariant == "" {
		f.Invariant = name
	}
	s.fail(f)
}

// fail records f (stamped with the current step) and stops the run: after
// the first violation the rest of the history is noise.
func (s *Simulator) fail(f Failure) {
	if f.Step == 0 {
		f.Step, f.At = s.step, s.now
	}
	s.res.Failures = append(s.res.Failures, f)
	s.halted = true
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: s.inc, Kind: KindCrash,
			Note: "FAILURE " + f.Invariant + ": " + f.Msg})
	}
}

// note appends a non-delivery log line (Log mode only).
func (s *Simulator) note(e Entry, msg string) {
	if s.cfg.Log {
		s.res.Log = append(s.res.Log, LogEntry{Step: s.step, At: s.now, Inc: e.Inc, Kind: e.Kind, Note: msg})
	}
}

// View implementation.

func (s *Simulator) Step() int          { return s.step }
func (s *Simulator) Now() core.Time     { return s.now }
func (s *Simulator) Alive() bool        { return s.alive }
func (s *Simulator) Inc() Incarnation   { return s.inc }
func (s *Simulator) Quiet() bool        { return s.quiet }
func (s *Simulator) FS() *FakeFS        { return s.fs }
func (s *Simulator) Root() string       { return s.cfg.Server.Dir }
func (s *Simulator) Oracle() *Oracle    { return s.oracle }
func (s *Simulator) History() *History  { return s.oracle.History() }
func (s *Simulator) Facts() []TimedFact { return s.facts }

var _ View = (*Simulator)(nil)

// env is the core.Env handed to each server incarnation. It draws from the
// run's single Rand, so the server's random choices replay with the seed.
type env struct {
	rng     *Rand
	buggify bool // false during QuietTail
}

func (e *env) Uint64() uint64       { return e.rng.Uint64() }
func (e *env) Intn(n int) int       { return e.rng.Intn(n) }
func (e *env) Maybe(core.Site) bool { return e.buggify && e.rng.Chance(0.05) }

var _ core.Env = (*env)(nil)
