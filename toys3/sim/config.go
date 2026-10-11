package sim

import "manifester/toys3/core"

// Config is one simulation run. Everything random derives from Seed.
type Config struct {
	Seed uint64

	// Chaos is how long clients issue requests while faults and crashes are
	// injected. QuietTail follows it: no new faults or crashes, the server is
	// (re)started and alive, clients keep issuing, and every outstanding
	// request must get a reply (liveness). The run then drains the queue.
	Chaos     core.Duration
	QuietTail core.Duration
	// MaxSteps aborts a run that never drains (reported as a failure).
	MaxSteps int

	Workload WorkloadConfig
	FS       FSFaults

	// CrashProb is the probability, evaluated after each server step during
	// Chaos, of crashing the server at that instant.
	CrashProb float64
	// RestartDelay is the [min, max] downtime between crash and Started.
	RestartDelay [2]core.Duration

	// Server is passed to Factory; Dir is the fake filesystem's root.
	Server core.Config
	// Factory builds the server under test; nil means core.NewMachine.
	// Learners can plug in a deliberately broken variant to watch DST catch it.
	Factory core.Factory
	// Invariants run after every step and at the end; nil means DefaultInvariants().
	Invariants []Invariant

	// Log keeps the full event/action log in Result.Log (otherwise only the
	// trace hash is kept). Replay a failing seed with Log on.
	Log bool
}

// Result is the outcome of one run.
type Result struct {
	Seed      uint64
	Steps     int
	EndTime   core.Time
	Crashes   int
	TraceHash uint64 // identical for two runs of one Config, or nondeterminism leaked
	RandCalls uint64
	Failures  []Failure // empty means the run passed
	Stats     Stats
	Log       []LogEntry // only with Config.Log
}

// Stats counts what happened, so a passing run can be checked for coverage
// (did any crash land mid-PUT? did any torn write happen?).
type Stats struct {
	Requests     int
	ByStatus     [4]int // 200, 400, 404, 500
	Indetermined int    // requests outstanding at a crash
	FSOps        int
	EIOs         int
	TornFiles    int // files left torn by a crash
	LostChanges  int // un-fsynced changes discarded by a crash
}

// Failure is one invariant violation.
type Failure struct {
	Invariant string
	Step      int
	At        core.Time
	Msg       string
}

// LogEntry is one step of the run: what was delivered and what came out.
type LogEntry struct {
	Step    int
	At      core.Time
	Inc     Incarnation
	Kind    EntryKind
	Event   core.EventBody // for KindDeliver: what the server consumed
	Actions []core.Action  // for KindDeliver: what it returned
	Note    string         // for other kinds: e.g. "crash", "fs land FSRename io=7 EIO"
}

// Run executes one simulation. It never panics on server misbehavior: a panic
// inside Step is recovered and reported as a Failure.
func Run(cfg Config) Result { return New(cfg).Run() }
