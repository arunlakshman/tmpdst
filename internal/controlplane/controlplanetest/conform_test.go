package controlplanetest

import (
	"testing"

	"manifester/internal/controlplane"
	"manifester/internal/verify/speccheck/checker/injectedbug"
	"manifester/internal/verify/speccheck/checker/runsettings"
	"manifester/internal/verify/speccheck/checker/suites"
)

// Conformance C3 for the control-plane stores: the shared control plane suite
// runs on each store, once for every generator profile.

func runProfiles(t *testing.T, newImpl func() impl, defaultSeeds int) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, subjectOf(newImpl), runsettings.Options(t, defaultSeeds))
		})
	}
}

func TestConformanceMemorystore(t *testing.T) {
	runProfiles(t, func() impl { return storeImpl{newMem()} }, 20)
}

// The gRPC test starts a server for each sequence, so it uses fewer seeds.
func TestConformanceMemorystoreGRPC(t *testing.T) {
	runProfiles(t, func() impl { return grpcOver(t) }, 3)
}

// TestInjectedBugsCaughtMemorystore proves that the suite fails when the store has
// a known bug, so a pass of TestConformanceMemorystore means something.
func TestInjectedBugsCaughtMemorystore(t *testing.T) {
	injectedbug.RequireAllControlPlaneBugsDetected(t, subjectOf(func() impl { return storeImpl{newMem()} }), 100)
}

var _ controlplane.Store = newMem()
