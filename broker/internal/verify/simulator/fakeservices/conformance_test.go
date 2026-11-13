package fakeservices

import (
	"errors"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakecontrolplane"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakeobjectstore"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/injectedbug"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// Conformance C2. The services in this package are SIMULATED components. This
// file compares each service with its speccheck spec, with every fault turned
// off. With no faults, a service must give the same result as the spec for
// each operation, and its cached spec hash must equal the spec state hash
// after each step.
//
// There is no test for the log bridge here. kafka-log (replica + fakelogstore)
// moved to mds-controlplane with the control plane extraction; see
// docs/cp-extraction.md. Non-log kafka uses fakecontrolplane against speccheck.

// seedBudget is the number of seeds for a planted bug. checks is the number of
// checks (random lists of steps) for each profile.
const (
	seedBudget = 50
	checks     = 50
)

// quietTB is the TB of a subject that a suite makes by itself. The suite has no
// testing.T for it, and a harness that cannot do its job is a programming error.
type quietTB struct{}

func (quietTB) Helper()                      {}
func (quietTB) Errorf(string, ...any)        {}
func (quietTB) Cleanup(func())               {}
func (quietTB) Fatalf(f string, args ...any) { panic(fmt.Sprintf(f, args...)) }

// completionFor drains the world and returns the completion of req.
func completionFor(h *servicetest.Harness, request machine.RequestID) (core.IOResult, error) {
	h.Run()
	c := h.Find(request)
	h.Delivered = nil
	if c == nil {
		return nil, fmt.Errorf("request %d: the world was drained but no completion arrived", request)
	}
	return c, nil
}

// engineObjectSubject runs the object store service as a conformance.ObjectSubject.
type engineObjectSubject struct {
	engine *fakeobjectstore.Engine
	h      *servicetest.Harness
}

func newEngineObjectSubject() conformance.ObjectSubject {
	h := servicetest.New(quietTB{}, 1, "a")
	e := fakeobjectstore.New(h.W, fakeobjectstore.Faults{})
	svc := kernel.AddService[core.ObjectCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		if oc, ok := out.(core.ObjectCall); ok {
			kernel.Call(r, svc, oc)
		}
	}
	return &engineObjectSubject{engine: e, h: h}
}

func (s *engineObjectSubject) Apply(operation objectstore.Call) (conformance.ObjectResult, error) {
	s.h.Issue("a", core.ObjectCall{Call: operation})
	c, err := completionFor(s.h, operation.RequestID())
	if err != nil {
		return conformance.ObjectResult{}, err
	}
	done, ok := c.(core.ObjectDone)
	if !ok {
		return conformance.ObjectResult{}, fmt.Errorf("request %d: the service answered with %T, not an object store result", operation.RequestID(), c)
	}
	return conformance.FromObjectResult(done.Result), nil
}

func (s *engineObjectSubject) StateHash() uint64 { return s.engine.SpecStateHash() }

// engineControlPlaneSubject runs the control plane service as a
// conformance.ControlPlaneSubject.
type engineControlPlaneSubject struct {
	engine *fakecontrolplane.Engine
	h      *servicetest.Harness
}

func newEngineControlPlaneSubject() conformance.ControlPlaneSubject {
	h := servicetest.New(quietTB{}, 1, "a")
	e := fakecontrolplane.New(h.W, fakecontrolplane.Faults{})
	svc := kernel.AddService[core.ControlPlaneCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		if c, ok := out.(core.ControlPlaneCall); ok {
			kernel.Call(r, svc, c)
		}
	}
	return &engineControlPlaneSubject{engine: e, h: h}
}

func (s *engineControlPlaneSubject) Apply(operation conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
	switch {
	case operation.CreateTopic != nil:
		code := s.engine.CreateTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)
		return conformance.ControlPlaneResult{Code: code}, nil
	case operation.Call != nil:
		s.h.Issue("a", *operation.Call)
		c, err := completionFor(s.h, operation.Call.RequestID())
		if err != nil {
			return conformance.ControlPlaneResult{}, err
		}
		done, ok := c.(core.ControlPlaneDone)
		if !ok {
			return conformance.ControlPlaneResult{}, fmt.Errorf("request %d: the completion is %T, not a CPDone", operation.Call.RequestID(), c)
		}
		return conformance.FromControlPlaneDone(done), nil
	}
	return conformance.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (s *engineControlPlaneSubject) StateHash() uint64 { return s.engine.SpecStateHash() }

func TestConformanceObjStore(t *testing.T) {
	for _, profile := range conformance.InputProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			l := conformance.ObjectStoreLockstep(profile, newEngineObjectSubject)
			l.Checks = checks
			l.Check(t)
		})
	}
}

func TestConformanceFakeControlPlane(t *testing.T) {
	for _, profile := range conformance.InputProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			l := conformance.ControlPlaneLockstep(profile, newEngineControlPlaneSubject)
			l.Checks = checks
			l.Check(t)
		})
	}
}

// The suite must find each planted bug even when the correct subject is a
// service. This shows the service comparison is as strong as the spec one.
func TestConformanceObjStoreCatchesPlantedBugs(t *testing.T) {
	injectedbug.ObjectBugsCaught(t, newEngineObjectSubject, seedBudget)
}

func TestConformanceFakeControlPlaneCatchesPlantedBugs(t *testing.T) {
	injectedbug.ControlPlaneBugsCaught(t, newEngineControlPlaneSubject, seedBudget)
}
