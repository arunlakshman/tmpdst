package injectedbug_test

import (
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/injectedbug"
	"github.com/manifesterio/mds/dst/check/spectest"
)

const seedBudget = 60

func TestPlantedObjectBugsAreCaught(t *testing.T) {
	injectedbug.ObjectBugsCaught(t, conformance.NewSpecObjectSubject, seedBudget)
}

func TestPlantedControlPlaneBugsAreCaught(t *testing.T) {
	injectedbug.ControlPlaneBugsCaught(t, conformance.NewSpecControlPlaneSubject, seedBudget)
}

// A wrapper that changes nothing must never be flagged, on any profile.
func TestNoFalseAlarmObject(t *testing.T) {
	same := injectedbug.NewObjectBug("Identity", "Changes nothing.", func(s conformance.ObjectSubject) conformance.ObjectSubject { return s })
	injectedbug.ObjectSuiteIsClean(t, func() conformance.ObjectSubject { return same.Wrap(conformance.NewSpecObjectSubject()) }, seedBudget)
}

func TestNoFalseAlarmControlPlane(t *testing.T) {
	same := injectedbug.NewControlPlaneBug("Identity", "Changes nothing.", func(s conformance.ControlPlaneSubject) conformance.ControlPlaneSubject { return s })
	injectedbug.ControlPlaneSuiteIsClean(t, func() conformance.ControlPlaneSubject { return same.Wrap(conformance.NewSpecControlPlaneSubject()) }, seedBudget)
}

func TestBugsHaveNamesAndDocs(t *testing.T) {
	seen := map[string]bool{}
	check := func(kind, name, doc string) {
		name = kind + " " + name
		if name == "" || !strings.HasSuffix(doc, ".") {
			t.Errorf("the bug %q needs a name and a doc sentence that ends with a period.", name)
		}
		if seen[name+doc] {
			t.Errorf("the bug %q appears twice.", name)
		}
		seen[name+doc] = true
	}
	for _, b := range injectedbug.ObjectBugs() {
		check("object", b.Name(), b.Doc())
	}
	for _, b := range injectedbug.ControlPlaneBugs() {
		check("control plane", b.Name(), b.Doc())
	}
	if n := len(injectedbug.ObjectBugs()); n < 7 {
		t.Errorf("there are %d object bugs, but at least 7 are required.", n)
	}
	if n := len(injectedbug.ControlPlaneBugs()); n < 8 {
		t.Errorf("there are %d control plane bugs, but at least 8 are required.", n)
	}
}

// Without a state hash, every bug that changes answers must still be caught by
// the result comparison and the invariants alone. The bug StateHashDiffers is
// skipped for such a subject, and the log says so.
func TestPlantedBugsAreCaughtWithoutStateHash(t *testing.T) {
	injectedbug.ObjectBugsCaught(t, func() conformance.ObjectSubject {
		return conformance.WithoutStateHashObject(conformance.NewSpecObjectSubject())
	}, seedBudget)
	injectedbug.ControlPlaneBugsCaught(t, func() conformance.ControlPlaneSubject {
		return conformance.WithoutStateHashControlPlane(conformance.NewSpecControlPlaneSubject())
	}, seedBudget)
}

// Rapid shrinks a failing list of operations. A caught bug must shrink to a
// short list, so that a reader can see the cause. The old shrinker of the
// broker made the same lists (1 to 3 operations).
func TestPlantedBugsShrinkToAShortList(t *testing.T) {
	const longest = 6
	for _, b := range injectedbug.ObjectBugs() {
		found := false
		for seed := uint64(1); seed <= seedBudget && !found; seed++ {
			l := conformance.ObjectStoreLockstep(conformance.Default(), func() conformance.ObjectSubject { return b.Wrap(conformance.NewSpecObjectSubject()) })
			if f := l.RunSeed(seed); f != nil {
				found = true
				if len(f.Steps) > longest {
					t.Errorf("the object bug %s shrank to %d steps, want %d or fewer:\n%s", b.Name(), len(f.Steps), longest, l.GoCode(f.Steps))
				}
			}
		}
		if !found {
			t.Errorf("the object bug %s was not caught on the default profile in %d seeds.", b.Name(), seedBudget)
		}
	}
	for _, b := range injectedbug.ControlPlaneBugs() {
		found := false
		for seed := uint64(1); seed <= seedBudget && !found; seed++ {
			l := conformance.ControlPlaneLockstep(conformance.Default(), func() conformance.ControlPlaneSubject { return b.Wrap(conformance.NewSpecControlPlaneSubject()) })
			if f := l.RunSeed(seed); f != nil {
				found = true
				if len(f.Steps) > longest {
					t.Errorf("the control plane bug %s shrank to %d steps, want %d or fewer:\n%s", b.Name(), len(f.Steps), longest, l.GoCode(f.Steps))
				}
			}
		}
		if !found {
			t.Errorf("the control plane bug %s was not caught on the default profile in %d seeds.", b.Name(), seedBudget)
		}
	}
}

// A planted bug wraps a subject with a state hash into a subject with a state
// hash, and a subject without one into a subject without one. If a wrapper
// always had a hash, the run would compare a hash that cannot be right.
func TestWrappersKeepTheStateHashOfTheSubject(t *testing.T) {
	for _, b := range injectedbug.ObjectBugs() {
		if !hasHash(b.Wrap(conformance.NewSpecObjectSubject())) {
			t.Errorf("the object bug %s hides the state hash of the subject.", b.Name())
		}
		if hasHash(b.Wrap(conformance.WithoutStateHashObject(conformance.NewSpecObjectSubject()))) {
			t.Errorf("the object bug %s makes up a state hash.", b.Name())
		}
	}
	for _, b := range injectedbug.ControlPlaneBugs() {
		if !hasHash(b.Wrap(conformance.NewSpecControlPlaneSubject())) {
			t.Errorf("the control plane bug %s hides the state hash of the subject.", b.Name())
		}
		if hasHash(b.Wrap(conformance.WithoutStateHashControlPlane(conformance.NewSpecControlPlaneSubject()))) {
			t.Errorf("the control plane bug %s makes up a state hash.", b.Name())
		}
	}
}

func hasHash(s any) bool {
	_, ok := s.(spectest.HasStateHash)
	return ok
}
