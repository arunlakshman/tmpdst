package plantedbug

import (
	"fmt"
	"strings"

	"manifester/internal/verify/veritas/veritastest"
	"manifester/internal/verify/veritas/veritastest/generator"
	"manifester/internal/verify/veritas/veritastest/suites"
)

// Detection says how a planted bug was found.
type Detection struct {
	Profile   string // the generator profile that found it
	Seed      uint64 // the first seed that found it
	ShrunkLen int    // the length of the shrunk case; 0 when not known
}

// RequireDetected runs runOne for the seeds 1 to maxSeeds. runOne returns true
// when the suite found a failure. RequireDetected reports an error if no seed
// finds one, because a suite that misses a planted bug cannot be trusted.
func RequireDetected(t veritastest.Reporter, name string, runOne func(seed uint64) bool, maxSeeds int) {
	t.Helper()
	for seed := uint64(1); seed <= uint64(maxSeeds); seed++ {
		if runOne(seed) {
			t.Logf("the planted bug %s was caught by seed %d.", name, seed)
			return
		}
	}
	t.Errorf("the planted bug %s was not caught in %d seeds. The suite is too weak for this bug.", name, maxSeeds)
}

// findDetection tries every profile in order. For each profile it runs the seeds 1
// to seeds with run. run returns the shrunk length of a failure, or -1 for none.
func findDetection(seeds int, run func(p generator.InputProfile, seed uint64) int) (Detection, bool) {
	for _, p := range suites.AllObjectProfiles() {
		for seed := uint64(1); seed <= uint64(seeds); seed++ {
			if n := run(p, seed); n >= 0 {
				return Detection{Profile: p.Name, Seed: seed, ShrunkLen: n}, true
			}
		}
	}
	return Detection{}, false
}

type detectionRow struct {
	name  string
	catch Detection
	found bool
}

// table returns the catch table as text. It also lists the bugs that were not
// caught.
func table(title string, rows []detectionRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", title)
	fmt.Fprintf(&b, "%-40s %-14s %6s %13s\n", "planted bug", "profile", "seed", "shrunk length")
	for _, r := range rows {
		if !r.found {
			fmt.Fprintf(&b, "%-40s %-14s %6s %13s\n", r.name, "NOT CAUGHT", "-", "-")
			continue
		}
		fmt.Fprintf(&b, "%-40s %-14s %6d %13d\n", r.name, r.catch.Profile, r.catch.Seed, r.catch.ShrunkLen)
	}
	return b.String()
}

func report(t veritastest.Reporter, kind string, seeds int, rows []detectionRow) {
	t.Helper()
	t.Logf("%s", table("catch table for the "+kind+" suite", rows))
	var missed []string
	for _, r := range rows {
		if !r.found {
			missed = append(missed, r.name)
		}
	}
	if len(missed) > 0 {
		t.Errorf("these planted %s bugs were not caught in %d seeds on any profile: %s. The suite is too weak for them.",
			kind, seeds, strings.Join(missed, ", "))
	}
}

// stateHashBugName is the name of the planted bug that only a state hash can show.
const stateHashBugName = "StateHashDiffers"

// newHashBug makes the StateHashDiffers bug for one kind of subject. newBug is
// the constructor of that kind, and build wraps inner in a subject that uses
// the given wrong hash function.
func newHashBug[S interface{ StateHash() (uint64, bool) }, B any](
	newBug func(name, doc string, wrap func(S) S) B,
	build func(inner S, hash func() (uint64, bool)) S,
) B {
	return newBug(stateHashBugName, "StateHash returns a value that is not the hash of the spec state.",
		func(inner S) S {
			return build(inner, func() (uint64, bool) {
				h, ok := inner.StateHash()
				return h ^ 1, ok
			})
		})
}

// findSubjectDetection runs one suite for every profile and the seeds 1 to seeds on
// a subject made by wrap from newCorrect. It returns the first catch.
func findSubjectDetection[Sub, Pair, Op any](
	seeds int,
	newCorrect func() Sub,
	wrap func(Sub) Sub,
	comparator func(generator.InputProfile, func() Sub) veritastest.SpecChecker[Pair, Op],
) (Detection, bool) {
	return findDetection(seeds, func(p generator.InputProfile, seed uint64) int {
		c := comparator(p, func() Sub { return wrap(newCorrect()) })
		if f := c.RunSeed(seed); f != nil {
			return len(f.Ops)
		}
		return -1
	})
}

// requireAllDetected runs find for each bug and reports an error for each bug that
// is not caught. A subject with no spec hash cannot show a wrong hash, so that
// one bug is skipped for such a subject and the log says so.
func requireAllDetected[Sub interface{ StateHash() (uint64, bool) }, Bug interface{ Name() string }](
	t veritastest.Reporter,
	kind string,
	newCorrect func() Sub,
	seeds int,
	bugs []Bug,
	find func(Bug, func() Sub, int) (Detection, bool),
) {
	t.Helper()
	_, hasHash := newCorrect().StateHash()
	var rows []detectionRow
	for _, bug := range bugs {
		if !hasHash && bug.Name() == stateHashBugName {
			t.Logf("The planted bug %s is skipped because this subject has no spec state hash.", bug.Name())
			continue
		}
		c, found := find(bug, newCorrect, seeds)
		rows = append(rows, detectionRow{name: bug.Name(), catch: c, found: found})
	}
	report(t, kind, seeds, rows)
}
