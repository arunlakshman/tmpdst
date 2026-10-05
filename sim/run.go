package sim

import "manifester/core"

// Phases bound fault generation (lesson 8).
type Phases struct {
	Chaos     core.Duration // faults injected
	QuietTail core.Duration // recovery mode, nodes alive: liveness/convergence
	Settle    core.Duration // drain remaining events, then Check
}

// Attrition kills and restarts broker processes during the chaos window.
type Attrition struct {
	MaxDead        int
	CrashWeight    float64
	GracefulWeight float64
	RecoveryDelay  [2]core.Duration
}

// Swarm controls per-seed on/off subsets (lesson 9). The masks are drawn from
// the run's stream with a fixed number of draws so they replay.
type Swarm struct {
	FaultFamilies bool
	Operations    bool
}

// Config is one simulation campaign.
type Config struct {
	Seeds            []uint64
	Processes        []Process
	Workloads        []WorkloadFactory
	Invariants       []func() Invariant // fresh per seed
	ObjectStore      ObjectStoreFaults
	ControlPlane     ControlPlaneFaults
	Network          NetworkFaults
	Attrition        Attrition
	Swarm            Swarm
	Phases           Phases
	Cost             CostParams // node CPU model; zero = instantaneous Step
	CheckDeterminism bool       // run every seed twice; compare RNG fingerprints and state hashes
}

// Report is the authority on pass/fail (lesson 11).
type Report struct {
	SeedsRun       []uint64
	SeedsFailing   []uint64
	Always         []AssertionStat // violations: definite bugs
	Coverage       []AssertionStat // sometimes/reachable never satisfied
	Nondeterminism []uint64        // seeds whose two runs diverged
}

type AssertionStat struct {
	Prop   core.Prop
	Kind   string
	Checks uint64
	Passes uint64
	Seeds  []uint64
}

// Runner executes a campaign on one goroutine per seed.
type Runner interface {
	Run(cfg Config) (Report, error)
	// Replay runs one seed with full fact/event logging.
	Replay(cfg Config, seed uint64) (Report, error)
}
