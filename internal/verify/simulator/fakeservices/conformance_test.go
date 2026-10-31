package fakeservices

import (
	"errors"
	"fmt"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/fakeservices/fakecontrolplane"
	"mds/internal/verify/simulator/fakeservices/fakeobjectstore"
	"mds/internal/verify/simulator/fakeservices/internal/fakeservicetest"
	"mds/internal/verify/speccheck/checker/generator"
	"mds/internal/verify/speccheck/checker/injectedbug"
	"mds/internal/verify/speccheck/checker/observation"
	"mds/internal/verify/speccheck/checker/runsettings"
	"mds/internal/verify/speccheck/checker/suites"
)

// Conformance C2. The engines in this package are SIMULATED components. This
// file compares each engine with its speccheck spec, with every fault turned
// off. With no faults, an engine must give the same result as the spec for
// each operation, and its cached spec hash must equal the spec state hash
// after each step.
//
// There is no test for the log engine here. The log engine is now only a
// bridge for ControlPlaneCall, and it has no replica state machine of its own. Its
// comparison with speccheck lives in the lockstep tests of package
// internal/controlplane/statemachine (see TestLockstepMachineVsSpecCheck).

const seedBudget = 50

// completionFor drains the engine queue and returns the completion of req.
func completionFor(ctx *fakeservicetest.Ctx, req core.RequestID) (core.IOResult, error) {
	ctx.Run()
	c := ctx.Find(req)
	if c == nil {
		return nil, fmt.Errorf("request %d: the engine queue was drained but no completion arrived", req)
	}
	return c, nil
}

// engineObjectSubject runs the object store engine as a suites.ObjectSubject.
type engineObjectSubject struct {
	engine *fakeobjectstore.Engine
	ctx    *fakeservicetest.Ctx
	from   core.NodeInstance
}

func newEngineObjectSubject() suites.ObjectSubject {
	e := fakeobjectstore.New(simulator.ObjectStoreFaults{})
	return &engineObjectSubject{engine: e, ctx: fakeservicetest.New(e, 1), from: fakeservicetest.Ref("a")}
}

func (s *engineObjectSubject) Do(op core.IOCall) (observation.ObjectResult, error) {
	s.engine.Execute(s.ctx, s.from, op)
	c, err := completionFor(s.ctx, op.RequestID())
	if err != nil {
		return observation.ObjectResult{}, err
	}
	return observation.FromObjectCompletion(c), nil
}

func (s *engineObjectSubject) StateHash() (uint64, bool) { return s.engine.SpecStateHash(), true }

// engineControlPlaneSubject runs the control plane engine as a
// suites.ControlPlaneSubject.
type engineControlPlaneSubject struct {
	engine *fakecontrolplane.Engine
	ctx    *fakeservicetest.Ctx
	from   core.NodeInstance
}

func newEngineControlPlaneSubject() suites.ControlPlaneSubject {
	e := fakecontrolplane.New(simulator.ControlPlaneFaults{})
	return &engineControlPlaneSubject{engine: e, ctx: fakeservicetest.New(e, 1), from: fakeservicetest.Ref("a")}
}

func (s *engineControlPlaneSubject) Do(op generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	switch {
	case op.CreateTopic != nil:
		code := s.engine.CreateTopic(op.CreateTopic.Name, op.CreateTopic.Partitions)
		return observation.ControlPlaneResult{Code: code}, nil
	case op.Call != nil:
		s.engine.Execute(s.ctx, s.from, *op.Call)
		c, err := completionFor(s.ctx, op.Call.RequestID())
		if err != nil {
			return observation.ControlPlaneResult{}, err
		}
		done, ok := c.(core.ControlPlaneDone)
		if !ok {
			return observation.ControlPlaneResult{}, fmt.Errorf("request %d: the completion is %T, not a CPDone", op.Call.RequestID(), c)
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (s *engineControlPlaneSubject) StateHash() (uint64, bool) { return s.engine.SpecStateHash(), true }

func TestConformanceObjStore(t *testing.T) {
	for _, profile := range suites.AllObjectProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ObjectStore(t, profile, newEngineObjectSubject, runsettings.Options(t, seedBudget))
		})
	}
}

func TestConformanceFakeControlPlane(t *testing.T) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, newEngineControlPlaneSubject, runsettings.Options(t, seedBudget))
		})
	}
}

// The suite must find each injected bug even when the correct subject is an
// engine. This shows the engine comparison is as strong as the spec one.
func TestConformanceObjStoreCatchesInjectedBugs(t *testing.T) {
	injectedbug.RequireAllObjectBugsDetected(t, newEngineObjectSubject, seedBudget)
}

func TestConformanceFakeControlPlaneCatchesInjectedBugs(t *testing.T) {
	injectedbug.RequireAllControlPlaneBugsDetected(t, newEngineControlPlaneSubject, seedBudget)
}
