package sim

import (
	"fmt"

	"manifester/toys3/control"
)

// Config is one simulation run. Everything random derives from Seed.
type Config struct {
	Seed uint64

	// Chaos is how long clients issue requests while faults and crashes are
	// injected. QuietTail follows it: no new faults or crashes, the server is
	// (re)started and alive, clients keep issuing, and every outstanding
	// request must get a reply (liveness). The run then drains the queue.
	Chaos     control.Duration
	QuietTail control.Duration
	// MaxSteps aborts a run that never drains (reported as a failure).
	MaxSteps int

	Workload WorkloadConfig
	FS       FSFaults

	// CrashProb is the probability, evaluated after each server step during
	// Chaos, of crashing the server at that instant.
	CrashProb float64
	// RestartDelay is the [min, max] downtime between crash and Started.
	RestartDelay [2]control.Duration

	// Server is passed to Factory; Dir is the fake filesystem's root.
	Server control.Config
	// Factory builds the server under test; nil means control.NewMachine.
	// Learners can plug in a deliberately broken variant to watch DST catch it.
	Factory control.Factory
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
	EndTime   control.Time
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
	Requests      int
	ByStatus      [4]int // 200, 400, 404, 500
	Indeterminate int    // requests outstanding at a crash
	FSOps         int
	EIOs          int
	TornFiles     int // files left torn by a crash
	LostChanges   int // un-fsynced changes discarded by a crash

	Crashes             int // crashes, including Trips
	Trips               int // crashes requested by the server (Trip)
	CrashesMidPut       int // crashes with a PUT or COPY outstanding
	KeptChanges         int // un-fsynced changes a crash made durable anyway
	ReadCorruptions     int // FSReads returned with a flipped bit
	CorruptionsDetected int // control.CorruptionDetected samples (GET/COPY 500 on a bad file)
	TempsRemoved        int // sum of Recovered.TempsRemoved
	Windows             int // linearizability windows checked
	MaxWindowOps        int // largest window (max, not sum, when aggregated)
	Widenings           int // oracle gave up precision on some keys (see Oracle)
}

// Add accumulates o into s (MaxWindowOps takes the maximum).
func (s *Stats) Add(o Stats) {
	s.Requests += o.Requests
	for i := range s.ByStatus {
		s.ByStatus[i] += o.ByStatus[i]
	}
	s.Indeterminate += o.Indeterminate
	s.FSOps += o.FSOps
	s.EIOs += o.EIOs
	s.TornFiles += o.TornFiles
	s.LostChanges += o.LostChanges
	s.Crashes += o.Crashes
	s.Trips += o.Trips
	s.CrashesMidPut += o.CrashesMidPut
	s.KeptChanges += o.KeptChanges
	s.ReadCorruptions += o.ReadCorruptions
	s.CorruptionsDetected += o.CorruptionsDetected
	s.TempsRemoved += o.TempsRemoved
	s.Windows += o.Windows
	s.MaxWindowOps = max(s.MaxWindowOps, o.MaxWindowOps)
	s.Widenings += o.Widenings
}

// String renders the stats on one line.
func (s Stats) String() string {
	return fmt.Sprintf("reqs=%d 200=%d 400=%d 404=%d 500=%d indet=%d fsops=%d eio=%d "+
		"crashes=%d trips=%d midput=%d torn=%d lost=%d kept=%d readcorrupt=%d corruptdet=%d "+
		"tempsrm=%d windows=%d maxwin=%d widened=%d",
		s.Requests, s.ByStatus[0], s.ByStatus[1], s.ByStatus[2], s.ByStatus[3], s.Indeterminate,
		s.FSOps, s.EIOs, s.Crashes, s.Trips, s.CrashesMidPut, s.TornFiles, s.LostChanges,
		s.KeptChanges, s.ReadCorruptions, s.CorruptionsDetected, s.TempsRemoved, s.Windows, s.MaxWindowOps, s.Widenings)
}

// statusIndex maps a reply status to its ByStatus slot.
func statusIndex(st control.Status) int {
	switch st {
	case control.StatusOK:
		return 0
	case control.StatusBadRequest:
		return 1
	case control.StatusNotFound:
		return 2
	}
	return 3
}

// DefaultConfig is the configuration used by the tests and the campaign
// runner: few clients and keys (contention), frequent crashes and faults
// during a short Chaos phase, then a QuietTail for liveness.
//
// It is "swarm" tested: the key count, client count, crash rate and EIO rate
// are drawn per seed (from a stream separate from the run's), because no
// single setting finds every bug. Few keys maximize same-key races; many
// keys leave objects untouched for long enough that slow-burning damage
// (e.g. a resurrected temp file aliasing a committed object) becomes visible.
func DefaultConfig(seed uint64) Config {
	cfg := BaseConfig(seed)
	r := NewRand(seed ^ 0x5a5a_c0ff_ee00_1234)
	cfg.Workload.Keys = []int{1, 2, 3, 3, 4, 8, 16, 32}[r.Intn(8)]
	cfg.Workload.Clients = []int{2, 4, 4, 6}[r.Intn(4)]
	cfg.CrashProb = []float64{0.001, 0.004, 0.004, 0.01}[r.Intn(4)]
	cfg.FS.EIOProb = []float64{0.002, 0.01, 0.01, 0.03}[r.Intn(4)]
	return cfg
}

// BaseConfig is DefaultConfig without the per-seed swarm variation.
func BaseConfig(seed uint64) Config {
	return Config{
		Seed:      seed,
		Chaos:     300 * control.Millisecond,
		QuietTail: 60 * control.Millisecond,
		MaxSteps:  2_000_000,
		Workload: WorkloadConfig{
			Clients:      4,
			Keys:         3,
			MaxValueSize: 40,
			Mix:          OpMix{Put: 40, Get: 40, Delete: 10, Copy: 15, Invalid: 2},
			Think:        [2]control.Duration{0, 1 * control.Millisecond},
			QuiesceEvery: 8 * control.Millisecond,
		},
		FS: FSFaults{
			EIOProb:         0.01,
			TornWriteProb:   0.3,
			ReadCorruptProb: 0.005,
			WritebackProb:   0.05,
			Latency:         [2]control.Duration{20 * control.Microsecond, 1500 * control.Microsecond},
			ReplyDelay:      [2]control.Duration{0, 500 * control.Microsecond},
			FsyncGate:       true,
		},
		CrashProb:    0.004,
		RestartDelay: [2]control.Duration{100 * control.Microsecond, 5 * control.Millisecond},
		Server:       control.Config{Dir: "/data", MaxObjectSize: 64},
	}
}

// Failure is one invariant violation.
type Failure struct {
	Invariant string
	Step      int
	At        control.Time
	Msg       string
}

// LogEntry is one step of the run: what was delivered and what came out.
type LogEntry struct {
	Step    int
	At      control.Time
	Inc     Incarnation
	Kind    EntryKind
	Event   control.EventBody // for KindDeliver: what the server consumed
	Actions []control.Action  // for KindDeliver: what it returned
	Note    string            // for other kinds: e.g. "crash", "fs land FSRename io=7 EIO"
}

// Run executes one simulation. It never panics on server misbehavior: a panic
// inside Step is recovered and reported as a Failure.
func Run(cfg Config) Result { return New(cfg).Run() }
