package spectest

import (
	"testing"
)

// Bug is the name of a planted bug. A planted bug is a mistake that a test
// puts into the code on purpose, to prove that the checks catch it. The
// product decides how a name turns on its bug (for example a config field
// that the code under test reads).
type Bug string

// RequireCaught fails t unless every bug is caught by at least one seed from
// 1 to budget. run runs one test with bug turned on and the given seed, and
// returns true when the test failed (the bug was caught).
func RequireCaught(t testing.TB, bugs []Bug, budget int, run func(bug Bug, seed uint64) (caught bool)) {
	t.Helper()
	if budget < 1 {
		t.Fatalf("the seed budget must be 1 or more, but it is %d", budget)
		return
	}
	for _, bug := range bugs {
		caught := false
		for seed := uint64(1); seed <= uint64(budget); seed++ {
			if run(bug, seed) {
				t.Logf("the planted bug %s was caught by seed %d", bug, seed)
				caught = true
				break
			}
		}
		if !caught {
			t.Fatalf("the planted bug %s was not caught in seeds 1 to %d: the test is too weak for this bug", bug, budget)
			return
		}
	}
}

// RequireClean fails t if any seed from 1 to budget fails with no planted bug
// turned on. A failure here is a false alarm: the test or the spec is wrong,
// not the code. run returns true when the test failed.
func RequireClean(t testing.TB, budget int, run func(seed uint64) (failed bool)) {
	t.Helper()
	if budget < 1 {
		t.Fatalf("the seed budget must be 1 or more, but it is %d", budget)
		return
	}
	for seed := uint64(1); seed <= uint64(budget); seed++ {
		if run(seed) {
			t.Fatalf("false alarm at seed %d: the run failed with no planted bug", seed)
			return
		}
	}
}
