package main

import (
	"sync"

	"manifester/sim"
)

// runSharded runs seeds in jobs contiguous shards on separate goroutines and
// merges the shard reports as one Run would have. A seed's result does not
// depend on which shard ran it (every seed builds its own world).
func runSharded(config func([]uint64) sim.Config, seeds []uint64, jobs int) (sim.Report, error) {
	jobs = max(1, min(jobs, len(seeds)))
	if jobs == 1 {
		return sim.NewRunner().Run(config(seeds))
	}
	reps := make([]sim.Report, jobs)
	errs := make([]error, jobs)
	var wg sync.WaitGroup
	per := (len(seeds) + jobs - 1) / jobs
	for j := 0; j < jobs; j++ {
		lo, hi := min(j*per, len(seeds)), min((j+1)*per, len(seeds))
		wg.Add(1)
		go func() {
			defer wg.Done()
			reps[j], errs[j] = sim.NewRunner().Run(config(seeds[lo:hi]))
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return sim.Report{}, err
		}
	}
	return mergeReports(reps), nil
}

type statKey struct{ prop, kind string }

// mergeReports combines shard reports in shard order: seeds are concatenated,
// assertion stats are summed per (property, kind), and Always/Coverage are
// recomputed over the whole campaign by the same rule sim.Runner.Run uses.
func mergeReports(reps []sim.Report) sim.Report {
	var out sim.Report
	idx := map[statKey]int{}
	for _, r := range reps {
		out.Seeds = append(out.Seeds, r.Seeds...)
		out.Nondeterminism = append(out.Nondeterminism, r.Nondeterminism...)
		for _, a := range r.Assertions {
			mergeAssertion(&out, idx, a)
		}
	}
	for _, a := range out.Assertions {
		classify(&out, a)
	}
	return out
}

// mergeAssertion adds a's counts and seeds to the (property, kind) entry of out.
func mergeAssertion(out *sim.Report, idx map[statKey]int, a sim.AssertionStat) {
	k := statKey{string(a.Prop), a.Kind}
	i, ok := idx[k]
	if !ok {
		i = len(out.Assertions)
		idx[k] = i
		out.Assertions = append(out.Assertions, sim.AssertionStat{Prop: a.Prop, Kind: a.Kind})
	}
	m := &out.Assertions[i]
	m.Checks += a.Checks
	m.Passes += a.Passes
	if len(a.Seeds) == 0 {
		return
	}
	if len(m.Seeds) == 0 {
		m.FirstSeed, m.FirstStep = a.FirstSeed, a.FirstStep
	}
	m.Seeds = append(m.Seeds, a.Seeds...)
}

// classify files a into Always (a safety property with failing seeds) or
// Coverage (a Sometimes property never satisfied).
func classify(out *sim.Report, a sim.AssertionStat) {
	safety := a.Kind == "always" || a.Kind == "always_or_unreachable" || a.Kind == "unreachable"
	switch {
	case safety && len(a.Seeds) > 0:
		out.Always = append(out.Always, a)
	case !safety && len(a.Seeds) == 0:
		out.Coverage = append(out.Coverage, a)
	}
}
