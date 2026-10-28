package sim

import (
	"errors"

	"manifester/internal/core"
)

// runner is the Runner. It runs seeds one after another on the calling
// goroutine: the simulator itself never starts one.
type runner struct{}

// NewRunner returns the default Runner.
func NewRunner() Runner { return runner{} }

func validate(cfg Config) error {
	for _, p := range cfg.Processes {
		if p.Factory == nil && p.FactoryFor == nil {
			return errors.New("sim: process " + p.Name + " has no Factory")
		}
	}
	for _, w := range cfg.Workloads {
		if w == nil {
			return errors.New("sim: nil WorkloadFactory")
		}
	}
	return nil
}

// Run runs every seed in cfg.Seeds and aggregates the report.
//
// Determinism check (every CheckDeterminism-th seed, starting with the first):
// the seed is run again with Log flipped, so the check also proves logging
// cannot change the hash, and (TraceHash, RandCalls, Steps) must match. A
// mismatch adds a FailNondeterminism to the seed and lists it in
// Report.Nondeterminism.
//
// Report.Always lists safety properties (Always, AlwaysOrUnreachable,
// Unreachable) that were violated, with the violating seeds and the first
// seed/step. Report.Coverage lists Sometimes properties never satisfied in
// the campaign. A Reachable that is never executed is never recorded, so it
// appears there only if listed in Config.ExpectReachable (as an unreached stat
// with zero Checks). Report.Assertions holds every stat.
func (runner) Run(cfg Config) (Report, error) {
	if err := validate(cfg); err != nil {
		return Report{}, err
	}
	var rep Report
	agg := map[statKey]int{}
	var all []AssertionStat
	for i, seed := range cfg.Seeds {
		res, stats := runSeed(cfg, seed)
		noteDeterminism(cfg, i, seed, &res, &rep)
		rep.Seeds = append(rep.Seeds, res)
		all = mergeStats(all, agg, stats)
	}
	all = addUnreached(all, cfg.ExpectReachable)
	rep.Assertions = all
	rep.Always, rep.Coverage = classifyStats(all)
	return rep, nil
}

func noteDeterminism(cfg Config, i int, seed uint64, res *SeedResult, rep *Report) {
	k := cfg.CheckDeterminism
	if k == 0 || uint32(i)%k != 0 {
		return
	}
	again := cfg
	again.Log = !cfg.Log
	res2, _ := runSeed(again, seed)
	if res2.TraceHash != res.TraceHash || res2.RandCalls != res.RandCalls || res2.Steps != res.Steps {
		res.Failures = append(res.Failures, Failure{Seed: seed, Kind: FailNondeterminism, Prop: "determinism",
			Step: res.Steps, Msg: "second run of the seed diverged"})
		rep.Nondeterminism = append(rep.Nondeterminism, seed)
	}
}

func mergeStats(all []AssertionStat, agg map[statKey]int, stats []AssertionStat) []AssertionStat {
	for _, s := range stats {
		k := statKey{s.Prop, s.Kind}
		j, ok := agg[k]
		if !ok {
			j = len(all)
			agg[k] = j
			all = append(all, AssertionStat{Prop: s.Prop, Kind: s.Kind})
		}
		a := &all[j]
		a.Checks += s.Checks
		a.Passes += s.Passes
		if len(s.Seeds) > 0 {
			if len(a.Seeds) == 0 {
				a.FirstSeed, a.FirstStep = s.FirstSeed, s.FirstStep
			}
			a.Seeds = append(a.Seeds, s.Seeds...)
		}
	}
	return all
}

func addUnreached(all []AssertionStat, expect []core.PropertyID) []AssertionStat {
	for _, p := range expect {
		found := false
		for _, a := range all {
			if a.Kind == kindReachable && a.Prop == p {
				found = true
			}
		}
		if !found {
			all = append(all, AssertionStat{Prop: p, Kind: kindReachable})
		}
	}
	return all
}

func classifyStats(all []AssertionStat) ([]AssertionStat, []AssertionStat) {
	var always, coverage []AssertionStat
	for _, a := range all {
		switch {
		case isSafety(a.Kind) && len(a.Seeds) > 0:
			always = append(always, a)
		case !isSafety(a.Kind) && len(a.Seeds) == 0:
			coverage = append(coverage, a)
		}
	}
	return always, coverage
}

// Replay runs one seed with HaltOnFirst and full logging. For a seed that
// passes, neither changes the TraceHash; a failing seed stops at its first
// failure, so its hash covers the prefix up to there.
func (runner) Replay(cfg Config, seed uint64) (SeedResult, error) {
	if err := validate(cfg); err != nil {
		return SeedResult{}, err
	}
	cfg.HaltOnFirst, cfg.Log = true, true
	return RunSeed(cfg, seed), nil
}

var _ Runner = runner{}
