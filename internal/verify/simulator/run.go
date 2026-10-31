package simulator

import "mds/internal/core"

// Phases bound fault generation (lesson 8).
type Phases struct {
	Chaos core.Duration // faults injected
	// Settling is recovery mode with nodes alive: liveness/convergence. Liveness
	// rule: no ErrUnavailable or timeout for ops invoked in Settling. Faults
	// planned in Chaos may still land there.
	Settling core.Duration
	Settle   core.Duration // drain remaining events, then AtEnd
	// MaxQuietLatency bounds how long an op invoked in Settling may take.
	MaxQuietLatency core.Duration
}

// NodeKills kills and restarts broker processes during the chaos window.
type NodeKills struct {
	MaxDead int
	// CrashProb is evaluated after each node Step and each engine landing
	// during Chaos.
	CrashProb   float64
	CrashWeight float64
	// GracefulWeight drives core.StopRequest: a graceful shutdown that ends in core.ShutdownDone.
	GracefulWeight float64
	RecoveryDelay  [2]core.Duration
}

// SeedConfig holds the per-seed knobs a RandomSettingsPicker may change.
type SeedConfig struct {
	ObjectStore  ObjectStoreFaults
	ControlPlane ControlPlaneFaults
	LogStore     LogStoreFaults
	Network      NetworkFaults
	Attrition    NodeKills
	Cost         CostParams
	// Params is scenario-specific tuning that the kernel never reads: set from
	// Config.Params, adjusted by a RandomSettingsPicker, read by FactoryFor and InvariantsFor.
	Params any
}

// RandomSettingsPicker picks this seed's knobs from menus (lesson 9). It draws only from r,
// which is the run's Derive("swarm") stream, never the run stream itself.
// Replay re-derives the config from the seed alone.
type RandomSettingsPicker func(seed uint64, r CountedRand, c *SeedConfig)

// PlannedFault is a fault at a fixed time, for regression tests that need a
// crash at a specific point.
type PlannedFault struct {
	At   Time
	Node core.NodeID
	Kind EntryKind
}

// Config is one simulation campaign. The per-seed knobs live in SeedConfig;
// RandomSettingsPicker adjusts a copy per seed.
type Config struct {
	Seeds      []uint64
	Processes  []Process
	Workloads  []WorkloadFactory
	Invariants []func() SafetyRule // fresh per seed
	// InvariantsFor builds more invariants from this seed's (swarmed) config.
	InvariantsFor func(sc SeedConfig) []SafetyRule
	// Params is the initial SeedConfig.Params of every seed.
	Params any
	// Engines builds this seed's engines from its (swarmed) config. Fresh per
	// seed. Engines that hold ground truth also implement ObjectStoreRealState or
	// ControlPlaneRealState; the View finds them by type assertion.
	Engines      func(sc SeedConfig) []FakeService
	ObjectStore  ObjectStoreFaults
	ControlPlane ControlPlaneFaults
	LogStore     LogStoreFaults
	Network      NetworkFaults
	Attrition    NodeKills
	Swarm        RandomSettingsPicker
	Phases       Phases
	Cost         CostParams // node CPU model; zero = instantaneous Step
	MaxSteps     uint64     // a seed exceeding it fails with FailMaxSteps
	// Log records a LogEntry per step. It must not change TraceHash.
	Log bool
	// HaltOnFirst stops a seed at its first failure. Campaigns record and
	// continue (lesson 11); Replay sets it.
	HaltOnFirst bool
	// QuiesceEvery is the interval between quiesce points, where oracles may
	// close a window.
	QuiesceEvery core.Duration
	Scripted     []PlannedFault
	// ExpectReachable lists Reachable properties that must be reached at least
	// once across the campaign. A Reachable only records when it executes, so
	// code that is never run cannot declare itself; the campaign declares it
	// here. Report.Coverage lists each one never reached (Kind "reachable",
	// zero Checks if it never ran at all).
	ExpectReachable []core.PropertyID
	// CheckDeterminism replays every Kth seed and compares
	// (TraceHash, RandCalls, Steps). 0 = off.
	CheckDeterminism uint32
	// VerifyHashes recomputes every cached StateHash each step and fails the
	// seed (FailProtocol) on a mismatch. It does not change TraceHash.
	VerifyHashes bool
}

// FailKind classifies a failure. Append-only.
type FailKind uint8

// FailKind values.
const (
	// FailViolation is a safety/property violation.
	FailViolation FailKind = iota
	// FailUndecided means the run could not decide.
	FailUndecided
	FailProtocol // StopForRestart/ShutdownDone not last, unknown action, reply to unknown request, duplicate completion
	FailPanic    // recovered from Step / Factory
	FailMaxSteps
	FailNotRepeatable
)

// Failure is one recorded failure of a seed.
type Failure struct {
	Seed uint64
	Kind FailKind
	Prop core.PropertyID
	Step uint64
	At   Time
	Msg  string
}

// Counter is one named stat. Stats are an ordered slice so golden rows survive
// renames.
type Counter struct {
	Name string
	N    uint64
}

// LogEntry is one line of a seed's replay log.
type LogEntry struct {
	Step uint64
	At   Time
	Text string
}

// SeedResult is everything one seed produced. (Steps, RandCalls, TraceHash,
// Stats) is what a golden file pins.
type SeedResult struct {
	Seed                  uint64
	Steps, RandCalls      uint64
	TraceHash             uint64
	Crashes, Trips, Exits int
	Stats                 []Counter
	Failures              []Failure
	Log                   []LogEntry // only when Config.Log
}

// Report is the authority on pass/fail (lesson 11).
type Report struct {
	Seeds    []SeedResult
	Always   []AssertionStat // violations: definite bugs
	Coverage []AssertionStat // sometimes/reachable never satisfied, plus unreached ExpectReachable
	// Assertions is every assertion stat of the campaign aggregated over seeds
	// (per property and kind, in first-evaluated order), whatever its outcome,
	// followed by a zero stat for each ExpectReachable property never evaluated.
	Assertions     []AssertionStat
	Nondeterminism []uint64 // seeds whose two runs diverged
}

// AssertionStat is the tally of one assertion across a run.
type AssertionStat struct {
	Prop      core.PropertyID
	Kind      string
	Checks    uint64
	Passes    uint64
	Seeds     []uint64
	FirstSeed uint64
	FirstStep uint64
}

// Runner executes a campaign on one goroutine per seed.
type Runner interface {
	Run(cfg Config) (Report, error)
	// Replay runs one seed with HaltOnFirst and full logging. Neither may change
	// the seed's TraceHash.
	Replay(cfg Config, seed uint64) (SeedResult, error)
}
