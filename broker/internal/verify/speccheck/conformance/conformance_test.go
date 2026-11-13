package conformance

import (
	"errors"
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/check/spectest"
	"github.com/manifesterio/mds/shared/objectstore"
)

// The spec is the correct subject, so no profile may report a failure.
func TestObjectStoreSpecVsSpec(t *testing.T) {
	for _, p := range InputProfiles() {
		t.Run(p.Name, func(t *testing.T) {
			l := ObjectStoreLockstep(p, NewSpecObjectSubject)
			l.Checks = 60
			l.Check(t)
		})
	}
}

func TestControlPlaneSpecVsSpec(t *testing.T) {
	for _, p := range InputProfiles() {
		t.Run(p.Name, func(t *testing.T) {
			l := ControlPlaneLockstep(p, NewSpecControlPlaneSubject)
			l.Checks = 60
			l.Check(t)
		})
	}
}

// The spec with its hash hidden is a correct subject too. The run must not
// fail for a subject that cannot give a hash.
func TestSpecVsSpecWithoutStateHash(t *testing.T) {
	p := Default()
	o := ObjectStoreLockstep(p, func() ObjectSubject { return WithoutStateHashObject(NewSpecObjectSubject()) })
	o.Checks = 20
	o.Check(t)
	c := ControlPlaneLockstep(p, func() ControlPlaneSubject { return WithoutStateHashControlPlane(NewSpecControlPlaneSubject()) })
	c.Checks = 20
	c.Check(t)
}

// badObject answers a range outside the object with OK.
type badObject struct{ ObjectSubject }

func (b badObject) Apply(operation objectstore.Call) (ObjectResult, error) {
	r, err := b.ObjectSubject.Apply(operation)
	if _, ok := operation.(objectstore.Get); ok && r.Code == core.ErrInvalid {
		return ObjectResult{}, nil
	}
	return r, err
}

// firstFailure runs the seeds 1 to 200 and returns the first failure.
func firstFailure[Op, Res any, S spectest.Subject[Op, Res]](t *testing.T, l spectest.Lockstep[Op, Res, S]) *spectest.Failure[Op] {
	t.Helper()
	for seed := uint64(1); seed <= 200; seed++ {
		if f := l.RunSeed(seed); f != nil {
			return f
		}
	}
	t.Fatalf("no seed from 1 to 200 found a failure")
	return nil
}

func TestFailureReportHasSeedRerunCommandAndGoCode(t *testing.T) {
	l := ObjectStoreLockstep(InputProfiles()[0], func() ObjectSubject { return badObject{NewSpecObjectSubject()} })
	out := l.Report(firstFailure(t, l), "TestX/sub")
	for _, want := range []string{"seed: ", "MDS_SEED=", "-run '^TestX$/^sub$'", "as Go code:", "[]objectstore.Call{", "objectstore.Get{"} {
		if !strings.Contains(out, want) {
			t.Errorf("the failure report does not contain %q. The report is:\n%s", want, out)
		}
	}
}

// badPut answers a Put that has data with a wrong code.
type badPut struct{ ObjectSubject }

func (b badPut) Apply(operation objectstore.Call) (ObjectResult, error) {
	r, err := b.ObjectSubject.Apply(operation)
	if put, ok := operation.(objectstore.Put); ok && len(put.Data) > 0 {
		r.Code = core.ErrIO
	}
	return r, err
}

// The suite writes over the data that it gave to the subject. The list in the
// failure report must still show the data that the generator made.
func TestFailureReportShowsTheOriginalPutData(t *testing.T) {
	l := ObjectStoreLockstep(InputProfiles()[0], func() ObjectSubject { return badPut{NewSpecObjectSubject()} })
	code := l.GoCode(firstFailure(t, l).Steps)
	if !strings.Contains(code, `Data: []byte("`) || strings.Contains(code, `!`) {
		t.Fatalf("the report does not show the data of the Put as the generator made it:\n%s", code)
	}
}

// brokenPlumbing returns an error from Apply.
type brokenPlumbing struct{ ObjectSubject }

func (brokenPlumbing) Apply(objectstore.Call) (ObjectResult, error) {
	return ObjectResult{}, errors.New("no completion arrived")
}

func TestSubjectErrorIsReported(t *testing.T) {
	l := ObjectStoreLockstep(InputProfiles()[0], func() ObjectSubject { return brokenPlumbing{NewSpecObjectSubject()} })
	f := l.RunSeed(1)
	if f == nil || !strings.Contains(f.Message, "no completion arrived") {
		t.Fatalf("the plumbing error was not reported: %+v", f)
	}
}
