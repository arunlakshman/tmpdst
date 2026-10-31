package injectedbug_test

import (
	"fmt"
	"strings"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/speccheck/checker/generator"
	"mds/internal/verify/speccheck/checker/injectedbug"
	"mds/internal/verify/speccheck/checker/observation"
	"mds/internal/verify/speccheck/checker/suites"
)

const seedBudget = 60

func TestCatchObjectBugs(t *testing.T) {
	injectedbug.RequireAllObjectBugsDetected(t, suites.NewSpecObjectSubject, seedBudget)
}

func TestCatchControlPlaneBugs(t *testing.T) {
	injectedbug.RequireAllControlPlaneBugsDetected(t, suites.NewSpecControlPlaneSubject, seedBudget)
}

// A wrapper that changes nothing must never be flagged, on any profile.
func TestNoFalseAlarmObject(t *testing.T) {
	same := injectedbug.NewObjectBug("Identity", "Changes nothing.", func(s suites.ObjectSubject) suites.ObjectSubject { return s })
	if c, found := injectedbug.FindObjectDetection(same, suites.NewSpecObjectSubject, seedBudget); found {
		t.Fatalf("the correct object subject was flagged by profile %s at seed %d.", c.Profile, c.Seed)
	}
}

func TestNoFalseAlarmControlPlane(t *testing.T) {
	same := injectedbug.NewControlPlaneBug("Identity", "Changes nothing.", func(s suites.ControlPlaneSubject) suites.ControlPlaneSubject { return s })
	if c, found := injectedbug.FindControlPlaneDetection(same, suites.NewSpecControlPlaneSubject, seedBudget); found {
		t.Fatalf("the correct control plane subject was flagged by profile %s at seed %d.", c.Profile, c.Seed)
	}
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

type recorder struct{ errors []string }

func (r *recorder) Helper()                   {}
func (r *recorder) Errorf(f string, a ...any) { r.errors = append(r.errors, fmt.Sprintf(f, a...)) }
func (r *recorder) Logf(string, ...any)       {}

func TestRequireCaughtFailsWhenNothingIsCaught(t *testing.T) {
	rec := &recorder{}
	injectedbug.RequireDetected(rec, "NeverCaught", func(uint64) bool { return false }, 5)
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "NeverCaught was not caught in 5 seeds") {
		t.Fatalf("RequireCaught gave this output: %v", rec.errors)
	}
	rec = &recorder{}
	injectedbug.RequireDetected(rec, "Caught", func(seed uint64) bool { return seed == 3 }, 5)
	if len(rec.errors) != 0 {
		t.Fatalf("RequireCaught reported an error for a caught bug: %v", rec.errors)
	}
}

// noHashObject hides the state hash, like real S3 does.
type noHashObject struct{ suites.ObjectSubject }

func (noHashObject) StateHash() (uint64, bool) { return 0, false }

func (n noHashObject) Do(operation core.IOCall) (observation.ObjectResult, error) {
	return n.ObjectSubject.Do(operation)
}

type noHashControlPlane struct{ suites.ControlPlaneSubject }

func (noHashControlPlane) StateHash() (uint64, bool) { return 0, false }

func (n noHashControlPlane) Do(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	return n.ControlPlaneSubject.Do(operation)
}

// Without a state hash, every bug that changes answers must still be caught by
// the result comparison and the invariants alone.
func TestCatchWithoutStateHash(t *testing.T) {
	newObject := func() suites.ObjectSubject { return noHashObject{suites.NewSpecObjectSubject()} }
	for _, b := range injectedbug.ObjectBugs() {
		if b.Name() == "StateHashDiffers" {
			continue
		}
		c, found := injectedbug.FindObjectDetection(b, newObject, seedBudget)
		if !found {
			t.Errorf("the object bug %s was not caught in %d seeds when the subject has no state hash.", b.Name(), seedBudget)
			continue
		}
		t.Logf("object bug %-26s caught without hash: profile %s, seed %d, shrunk length %d", b.Name(), c.Profile, c.Seed, c.ShrunkLen)
	}
	newControlPlane := func() suites.ControlPlaneSubject { return noHashControlPlane{suites.NewSpecControlPlaneSubject()} }
	for _, b := range injectedbug.ControlPlaneBugs() {
		if b.Name() == "StateHashDiffers" {
			continue
		}
		c, found := injectedbug.FindControlPlaneDetection(b, newControlPlane, seedBudget)
		if !found {
			t.Errorf("the control plane bug %s was not caught in %d seeds when the subject has no state hash.", b.Name(), seedBudget)
			continue
		}
		t.Logf("control plane bug %-38s caught without hash: profile %s, seed %d, shrunk length %d", b.Name(), c.Profile, c.Seed, c.ShrunkLen)
	}
}
