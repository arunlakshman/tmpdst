// Package mutant checks that planted bugs fail within a seed budget and that
// clean runs raise no false alarms.
//
// This package moved to dst/check/spectest (Bug, RequireCaught, RequireClean). It
// stays here only until its last user moves (plan task T1.16 deletes it).
// New code uses dst/check/spectest.
package mutant

import (
	"testing"

	"github.com/manifesterio/mds/dst/check/spectest"
)

// Bug names a planted fault under test. It is spectest.Bug.
type Bug = spectest.Bug

// Require fails t unless each bug fails at least once across seeds 1..budget.
// It is spectest.RequireCaught.
func Require(t testing.TB, bugs []Bug, budget int, run func(bug Bug, seed uint64) (failed bool)) {
	t.Helper()
	spectest.RequireCaught(t, bugs, budget, run)
}

// RequireClean fails t if any seed in 1..budget reports a failure. It is
// spectest.RequireClean.
func RequireClean(t testing.TB, budget int, run func(seed uint64) (failed bool)) {
	t.Helper()
	spectest.RequireClean(t, budget, run)
}
