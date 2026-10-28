package suites_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"manifester/internal/core"
	"manifester/internal/verify/veritas/veritastest"
	"manifester/internal/verify/veritas/veritastest/observation"
	"manifester/internal/verify/veritas/veritastest/suites"
)

func options(t *testing.T, seeds int) veritastest.Options {
	return veritastest.Options{
		Seeds: seeds,
		Seed:  -1,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./internal/verify/veritas/veritastest/suites -run '%s' -count=1 (seed %d)", t.Name(), seed)
		},
	}
}

// The spec is the correct subject, so no profile may report a failure.
func TestObjectStoreSpecVsSpec(t *testing.T) {
	for _, p := range suites.AllObjectProfiles() {
		t.Run(p.Name, func(t *testing.T) {
			suites.ObjectStore(t, p, suites.NewSpecObjectSubject, options(t, 60))
		})
	}
}

func TestControlPlaneSpecVsSpec(t *testing.T) {
	for _, p := range suites.AllControlPlaneProfiles() {
		t.Run(p.Name, func(t *testing.T) {
			suites.ControlPlane(t, p, suites.NewSpecControlPlaneSubject, options(t, 60))
		})
	}
}

func TestPartitionSpecVsSpec(t *testing.T) {
	suites.Partition(t, suites.NewSpecPartitionSubject, options(t, 60))
}

// recorder is a Reporter that keeps what the suite prints.
type recorder struct{ errors, logs []string }

func (r *recorder) Helper() {}
func (r *recorder) Errorf(f string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(f, args...))
}
func (r *recorder) Logf(f string, args ...any) { r.logs = append(r.logs, fmt.Sprintf(f, args...)) }

// badObject answers a range outside the object with OK.
type badObject struct{ suites.ObjectSubject }

func (b badObject) Do(op core.IOCall) (observation.ObjectResult, error) {
	r, err := b.ObjectSubject.Do(op)
	if _, ok := op.(core.ObjectGet); ok && r.Code == core.ErrInvalid {
		return observation.ObjectResult{}, nil
	}
	return r, err
}

func TestFailureReportHasSeedReplayAndLiteral(t *testing.T) {
	rec := &recorder{}
	p := suites.AllObjectProfiles()[0]
	opts := options(t, 200)
	suites.ObjectStore(rec, p, func() suites.ObjectSubject { return badObject{suites.NewSpecObjectSubject()} }, opts)
	if len(rec.errors) != 1 {
		t.Fatalf("the suite reported %d errors, but it must report exactly 1.", len(rec.errors))
	}
	out := rec.errors[0]
	for _, want := range []string{"seed ", "replay: go test", "shrunk sequence:", "[]core.IOCall{", "core.ObjectGet{"} {
		if !strings.Contains(out, want) {
			t.Errorf("the failure report does not contain %q. The report is:\n%s", want, out)
		}
	}
}

// brokenPlumbing returns an error from Do.
type brokenPlumbing struct{ suites.ObjectSubject }

func (brokenPlumbing) Do(core.IOCall) (observation.ObjectResult, error) {
	return observation.ObjectResult{}, errors.New("no completion arrived")
}

func TestSubjectErrorIsReported(t *testing.T) {
	rec := &recorder{}
	suites.ObjectStore(rec, suites.AllObjectProfiles()[0], func() suites.ObjectSubject {
		return brokenPlumbing{suites.NewSpecObjectSubject()}
	}, options(t, 3))
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "no completion arrived") {
		t.Fatalf("the plumbing error was not reported: %v", rec.errors)
	}
}
