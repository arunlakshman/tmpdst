// Package mutant checks that planted bugs fail within a seed budget and that
// clean runs raise no false alarms.
package mutant

import "testing"

// Bug names a planted fault under test.
type Bug string

// Require fails t unless each bug fails at least once across seeds 1..budget.
func Require(t testing.TB, bugs []Bug, budget int, run func(bug Bug, seed uint64) (failed bool)) {
	t.Helper()
	if budget < 1 {
		t.Fatalf("budget must be >= 1, got %d", budget)
	}
	for _, bug := range bugs {
		found := false
		for seed := uint64(1); seed <= uint64(budget); seed++ {
			if run(bug, seed) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("bug %s was not found in %d seeds", bug, budget)
		}
	}
}

// RequireClean fails t if any seed in 1..budget reports a failure.
func RequireClean(t testing.TB, budget int, run func(seed uint64) (failed bool)) {
	t.Helper()
	if budget < 1 {
		t.Fatalf("budget must be >= 1, got %d", budget)
	}
	for seed := uint64(1); seed <= uint64(budget); seed++ {
		if run(seed) {
			t.Fatalf("false alarm at seed %d", seed)
		}
	}
}
