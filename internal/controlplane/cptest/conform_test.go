package cptest

import (
	"testing"

	cp "manifester/internal/controlplane"
	"manifester/internal/verify/veritas/veritastest/plantedbug"
	"manifester/internal/verify/veritas/veritastest/runcontrol"
	"manifester/internal/verify/veritas/veritastest/suites"
)

// Conformance C3 for the control-plane stores: the shared control plane suite
// runs on each store, once for every generator profile.

func runProfiles(t *testing.T, newImpl func() impl, defaultSeeds int) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, subjectOf(newImpl), runcontrol.Options(t, defaultSeeds))
		})
	}
}

func TestConformanceMemstore(t *testing.T) {
	runProfiles(t, func() impl { return storeImpl{newMem()} }, 20)
}

// The gRPC test starts a server for each sequence, so it uses fewer seeds.
func TestConformanceMemstoreGRPC(t *testing.T) {
	runProfiles(t, func() impl { return grpcOver(t) }, 3)
}

// TestPlantedBugsCaughtMemstore proves that the suite fails when the store has
// a known bug, so a pass of TestConformanceMemstore means something.
func TestPlantedBugsCaughtMemstore(t *testing.T) {
	plantedbug.RequireAllControlPlaneBugsDetected(t, subjectOf(func() impl { return storeImpl{newMem()} }), 100)
}

var _ cp.Store = newMem()
