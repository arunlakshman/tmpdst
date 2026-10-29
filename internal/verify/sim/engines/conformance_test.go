package engines

import (
	"errors"
	"fmt"
	"testing"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/cpengine"
	"manifester/internal/verify/sim/engines/internal/enginetest"
	"manifester/internal/verify/sim/engines/objstore"
	"manifester/internal/verify/veritas/veritastest/generator"
	"manifester/internal/verify/veritas/veritastest/observation"
	"manifester/internal/verify/veritas/veritastest/plantedbug"
	"manifester/internal/verify/veritas/veritastest/runcontrol"
	"manifester/internal/verify/veritas/veritastest/suites"
)

// Conformance C2. The engines in this package are SIMULATED components. This
// file compares each engine with its veritas spec, with every fault turned
// off. With no faults, an engine must give the same result as the spec for
// each operation, and its cached spec hash must equal the spec state hash
// after each step.
//
// There is no test for the log engine here. The log engine is now only a
// bridge for ControlPlaneCall, and it has no replica state machine of its own. Its
// comparison with veritas lives in the lockstep tests of package
// internal/controlplane/machine (see TestLockstepMachineVsVeritas).

const seedBudget = 50

// completionFor drains the engine queue and returns the completion of req.
func completionFor(ctx *enginetest.Ctx, req core.RequestID) (core.IOResult, error) {
	ctx.Run()
	c := ctx.Find(req)
	if c == nil {
		return nil, fmt.Errorf("request %d: the engine queue was drained but no completion arrived", req)
	}
	return c, nil
}

// engineObjectSubject runs the object store engine as a suites.ObjectSubject.
type engineObjectSubject struct {
	engine *objstore.Engine
	ctx    *enginetest.Ctx
	from   core.NodeInstance
}

func newEngineObjectSubject() suites.ObjectSubject {
	e := objstore.New(sim.ObjectStoreFaults{})
	return &engineObjectSubject{engine: e, ctx: enginetest.New(e, 1), from: enginetest.Ref("a")}
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
	engine *cpengine.Engine
	ctx    *enginetest.Ctx
	from   core.NodeInstance
}

func newEngineControlPlaneSubject() suites.ControlPlaneSubject {
	e := cpengine.New(sim.ControlPlaneFaults{})
	return &engineControlPlaneSubject{engine: e, ctx: enginetest.New(e, 1), from: enginetest.Ref("a")}
}

func (s *engineControlPlaneSubject) Do(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
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
			suites.ObjectStore(t, profile, newEngineObjectSubject, runcontrol.Options(t, seedBudget))
		})
	}
}

func TestConformanceCPEngine(t *testing.T) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, newEngineControlPlaneSubject, runcontrol.Options(t, seedBudget))
		})
	}
}

// The suite must find each planted bug even when the correct subject is an
// engine. This shows the engine comparison is as strong as the spec one.
func TestConformanceObjStoreCatchesPlantedBugs(t *testing.T) {
	plantedbug.RequireAllObjectBugsDetected(t, newEngineObjectSubject, seedBudget)
}

func TestConformanceCPEngineCatchesPlantedBugs(t *testing.T) {
	plantedbug.RequireAllControlPlaneBugsDetected(t, newEngineControlPlaneSubject, seedBudget)
}
