package sim

import (
	"errors"
	"fmt"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/trace"
)

// ErrProtocol means Step returned a value no control type and no engine accepted.
var ErrProtocol = errors.New("sim: unhandled outbound value")

// Runner owns one rng, sched, fault.Injector, assert.Log, and trace.Hash for a run.
type Runner struct {
	cfg      Config
	rand     *rng.Rand
	sched    *sched.Scheduler
	faults   *fault.Injector
	asserts  *assert.Log
	hash     trace.Hash
	engines  []eng.Engine
	procs    map[ProcessID]Process
	crashed  map[ProcessID]bool
	exited   map[ProcessID]bool
	inc      map[ProcessID]uint64 // incarnation counter of each process
	timers   map[timerKey]uint64  // latest generation per timer; never reset, so a replaced timer can not come back
	failures []Failure
	steps    int
	pops     int
	protoErr error
}

type timerKey struct {
	pid ProcessID
	id  uint64
}

// New builds a Runner for cfg. MaxPops defaults to 1_000_000 when zero.
func New(cfg Config) *Runner {
	if cfg.MaxPops <= 0 {
		cfg.MaxPops = defaultMaxPops
	}
	r := &Runner{
		cfg:     cfg,
		rand:    rng.New(cfg.Seed),
		procs:   map[ProcessID]Process{},
		crashed: map[ProcessID]bool{},
		exited:  map[ProcessID]bool{},
		inc:     map[ProcessID]uint64{},
		timers:  map[timerKey]uint64{},
		hash:    trace.New(),
	}
	r.sched = sched.New(r.rand)
	r.faults = fault.New(r.rand)
	r.asserts = assert.New(func(property, detail string) {
		r.failures = append(r.failures, Failure{Property: property, Detail: detail})
	})
	return r
}

// Register adds an engine. Dispatch tries engines in registration order.
func (r *Runner) Register(e eng.Engine) {
	r.engines = append(r.engines, e)
}

// Spawn adds or replaces a process and clears a prior crash for that ID.
func (r *Runner) Spawn(p Process) {
	id := p.ID()
	if _, known := r.procs[id]; known {
		r.inc[id]++
	}
	r.procs[id] = p
	delete(r.crashed, id)
	delete(r.exited, id)
}

// Exited reports that id ended with Exit and has not been restarted.
func (r *Runner) Exited(id ProcessID) bool { return r.exited[id] }

// Crashed reports that id is crashed or exited.
func (r *Runner) Crashed(id ProcessID) bool { return r.crashed[id] }

// Inject schedules an inbox value for process to after delay.
func (r *Runner) Inject(to ProcessID, after sched.TimeNs, inbox any) {
	r.deliver(to, after, inbox)
}

// Sched gives a product simulator the run's own scheduler, so its clients and
// nemesis share one clock and one seed with the processes.
func (r *Runner) Sched() *sched.Scheduler { return r.sched }

// Rand returns the run's random stream.
func (r *Runner) Rand() *rng.Rand { return r.rand }

// Faults returns the run's fault injector.
func (r *Runner) Faults() *fault.Injector { return r.faults }

// Assert returns the run's assertion log.
func (r *Runner) Assert() *assert.Log { return r.asserts }

// Steps is the number of Process.Step calls so far.
func (r *Runner) Steps() int { return r.steps }

// RunUntil takes events until stop returns true (checked before each event),
// the queue is empty, or MaxPops is reached. It reports true when it ended
// because nothing is left to run or the cap was hit, and false when stop said so.
func (r *Runner) RunUntil(stop func() bool) bool {
	for !stop() {
		if r.sched.Len() == 0 || r.pops >= r.cfg.MaxPops || !r.sched.Next() {
			return true
		}
		r.pops++
	}
	return false
}

// Run drains the scheduler under cfg limits and returns the run fingerprint.
func (r *Runner) Run() (Result, error) {
	r.RunUntil(func() bool { return r.cfg.Steps > 0 && r.steps >= r.cfg.Steps })
	for _, e := range r.engines {
		r.hash.Uint64(e.StateHash())
	}
	res := Result{
		Hash:     r.hash.Sum(),
		Steps:    r.steps,
		Pops:     r.pops,
		Calls:    r.rand.Calls(),
		Failures: append([]Failure(nil), r.failures...),
	}
	return res, r.protoErr
}

// Incarnation rules. Each process has an incarnation number; Restart with a
// new process (or Spawn over an old one) raises it. A value that is addressed
// to the incarnation of its sender is dropped when that incarnation is gone:
// a timer, a Send to oneself, and an engine reply to the process that made the
// call. A value for another process, or from Inject, goes to whatever
// incarnation is current when it arrives.

func (r *Runner) deliver(to ProcessID, after sched.TimeNs, inbox any) {
	r.sched.After(after, "deliver:"+string(to), func() {
		r.step(to, inbox)
	})
}

// deliverTo is deliver for a value that belongs to incarnation inc of to.
func (r *Runner) deliverTo(to ProcessID, inc uint64, after sched.TimeNs, inbox any) {
	r.sched.After(after, "deliver:"+string(to), func() {
		if r.inc[to] == inc {
			r.step(to, inbox)
		}
	})
}

func (r *Runner) step(to ProcessID, inbox any) {
	if r.crashed[to] {
		return
	}
	p, ok := r.procs[to]
	if !ok {
		r.asserts.Unreachable("sim: unknown process", assert.Pair{Key: "to", Value: string(to)})
		return
	}
	cx := procContext{r: r}
	out := p.Step(cx, inbox)
	r.steps++
	if r.cfg.OnStep != nil {
		r.cfg.OnStep(StepInfo{Who: to, At: r.sched.Mono(), Inbox: inbox, Out: out})
	}
	r.hash.Int64(int64(r.sched.Mono()))
	r.hash.String(string(to))
	r.hash.Uint64(p.StateHash())
	r.hash.Uint64(uint64(len(out)))
	for _, v := range out {
		r.route(to, v)
	}
}

func (r *Runner) route(from ProcessID, v any) {
	switch a := v.(type) {
	case Send:
		if a.To == from {
			r.deliverTo(from, r.inc[from], 0, a.Body)
		} else {
			r.deliver(a.To, 0, a.Body)
		}
		return
	case SetTimer:
		r.setTimer(from, a)
		return
	case CancelTimer:
		r.cancelTimer(from, a.ID)
		return
	case Crash:
		who := a.Who
		if who == "" {
			who = from
		}
		r.crashed[who] = true
		return
	case Exit:
		r.crashed[from] = true
		r.exited[from] = true
		return
	case Restart:
		delete(r.crashed, a.Who)
		delete(r.exited, a.Who)
		if a.New != nil {
			if a.New.ID() != a.Who {
				r.asserts.Unreachable("sim: restart with another ID", assert.Pair{Key: "who", Value: string(a.Who)})
				return
			}
			r.procs[a.Who] = a.New
			r.inc[a.Who]++
		}
		if a.Boot != nil {
			r.deliverTo(a.Who, r.inc[a.Who], 0, a.Boot)
		}
		return
	}

	ecx := engContext{r: r, from: eng.ActorID(from), inc: r.inc[from], engine: nil}
	for _, e := range r.engines {
		ecx.engine = e
		err := e.Handle(ecx, v)
		if errors.Is(err, eng.ErrNotMine) {
			continue
		}
		if err != nil {
			r.asserts.Always(false, "sim: engine handle", assert.Pair{Key: "err", Value: err.Error()})
			r.protoErr = errors.Join(r.protoErr, err)
		}
		return
	}
	r.asserts.Unreachable("sim: unhandled outbound", assert.Pair{Key: "type", Value: fmt.Sprintf("%T", v)})
	r.protoErr = errors.Join(r.protoErr, fmt.Errorf("%w: %T", ErrProtocol, v))
}

func (r *Runner) setTimer(pid ProcessID, t SetTimer) {
	key := timerKey{pid: pid, id: t.ID}
	r.timers[key]++
	gen := r.timers[key]
	body := t.Body
	inc := r.inc[pid]
	r.sched.After(t.After, "timer", func() {
		if r.timers[key] != gen || r.inc[pid] != inc {
			return
		}
		r.step(pid, body)
	})
}

func (r *Runner) cancelTimer(pid ProcessID, id uint64) {
	key := timerKey{pid: pid, id: id}
	r.timers[key]++
}

type procContext struct{ r *Runner }

func (c procContext) Now() sched.TimeNs       { return c.r.sched.Mono() }
func (c procContext) Wall() sched.TimeNs      { return c.r.cfg.WallStart + c.r.sched.Mono() }
func (c procContext) Rand() *rng.Rand         { return c.r.rand }
func (c procContext) Faults() *fault.Injector { return c.r.faults }
func (c procContext) Assert() *assert.Log     { return c.r.asserts }

type engContext struct {
	r      *Runner
	from   eng.ActorID
	inc    uint64 // incarnation of from when the engine was called
	engine eng.Engine
}

func (c engContext) Now() sched.TimeNs       { return c.r.sched.Mono() }
func (c engContext) Rand() *rng.Rand         { return c.r.rand }
func (c engContext) Faults() *fault.Injector { return c.r.faults }
func (c engContext) Assert() *assert.Log     { return c.r.asserts }
func (c engContext) From() eng.ActorID       { return c.from }

func (c engContext) Deliver(to eng.ActorID, after sched.TimeNs, inbox any) {
	if ProcessID(to) == ProcessID(c.from) {
		c.r.deliverTo(ProcessID(to), c.inc, after, inbox)
		return
	}
	c.r.deliver(ProcessID(to), after, inbox)
}

func (c engContext) Self(after sched.TimeNs, body any) {
	e := c.engine
	c.r.sched.After(after, "self:"+string(e.ID()), func() {
		ecx := engContext{r: c.r, from: c.from, inc: c.inc, engine: e}
		e.OnSelf(ecx, body)
	})
}
