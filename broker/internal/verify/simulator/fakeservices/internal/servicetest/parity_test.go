package servicetest_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakenetwork"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
)

// Parity of Faulty with the hand-written fakes (plan tasks T1.12a and T1.15).
//
// Each case below is a scripted run (servicetest.ObjectCases and the like). The same
// case was run against the hand-written fakes (driven by the paritygen generator,
// deleted in T1.12d, through a small adapter) and its transcript was saved in
// ../../testdata/parity_*.txt. A transcript holds the number of steps and of random
// draws, the counters, the assertion coverage, samples of the state hash, the faults
// and a digest of every reply. In T1.15 the hand-written object store and control
// plane were replaced by dst/check/faulty around their specs. The files were NOT
// changed: Faulty runs the same case and its transcript must still be equal. So
// with one seed and one request sequence, Faulty gives the same replies, the same
// faults and the same number of random draws, in the same order, as the fakes did.
// The network (fakenetwork) is still hand-written and is compared in the same way.
//
// If a change of the kernel (queue, ties) moves these files, the old fakes can no
// longer make them again. Then a kernel change that is meant must re-pin them by
// hand, with a note in the commit.

func timing(t servicetest.Timing) kernel.FastWithRareSlow {
	return kernel.FastWithRareSlow{FastMin: t.FastMin, FastMax: t.FastMax, SlowMin: t.SlowMin, SlowMax: t.SlowMax, SlowProb: t.SlowProb}
}

// faults turns the plain fault knobs of a case into the faults of Faulty.
func faults(m servicetest.Mix) faulty.Faults {
	return faulty.Faults{
		ApplyLatency: timing(m.Apply), RespondLatency: timing(m.Respond),
		Unavailable503: m.P503, Timeout: m.Timeout, LostResponse: m.Lost,
		Outage:        faulty.Outage{Prob: m.OutageProb, Dur: m.OutageDur},
		CallerTimeout: m.CallerTimeout,
	}
}

// newObjectStore installs the simulated object store in a world for a case.
func newObjectStore(h *servicetest.Harness, c servicetest.Case) servicetest.Subject {
	e := fakeservices.NewObjectStore(h.W, fakeservices.ObjectStoreFaults{Faults: faults(c.Mix), GetCorruptProb: c.Mix.Corrupt})
	svc := kernel.AddService[core.ObjectCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		if x, ok := out.(core.ObjectCall); ok {
			kernel.Call(r, svc, x)
		}
	}
	return servicetest.Subject{StateHash: e.StateHash, SpecHash: e.SpecStateHash}
}

func newControlPlane(h *servicetest.Harness, c servicetest.Case) servicetest.Subject {
	e := fakeservices.NewControlPlane(h.W, fakeservices.ControlPlaneFaults{Faults: faults(c.Mix), MaxCommitItems: c.Mix.MaxCommitItems})
	svc := kernel.AddService[core.ControlPlaneCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		if x, ok := out.(core.ControlPlaneCall); ok {
			kernel.Call(r, svc, x)
		}
	}
	return servicetest.Subject{StateHash: e.StateHash, SpecHash: e.SpecStateHash, CreateTopic: e.CreateTopic}
}

func newNetwork(h *servicetest.Harness, c servicetest.Case) servicetest.Subject {
	n := c.Net
	e := fakenetwork.New(h.W, fakenetwork.Faults{
		Latency: timing(n.Latency), PartialIOMax: n.PartialIOMax,
		PartitionProb: n.PartitionProb, PartitionDur: n.PartitionDur,
		ClogProb: n.ClogProb, ClogDur: n.ClogDur,
		RandomCloseProb: n.RandomCloseProb, BlackHoleProb: n.BlackHoleProb, PairLatencyMax: n.PairLatencyMax,
	})
	svc := kernel.AddService[fakenetwork.Call, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		switch x := out.(type) {
		case core.Connect:
			kernel.Call(r, svc, fakenetwork.Call(fakenetwork.Dial{Connect: x}))
		case core.Send:
			kernel.Call(r, svc, fakenetwork.Call(fakenetwork.Write{Send: x}))
		case core.CloseConn:
			kernel.Call(r, svc, fakenetwork.Call(fakenetwork.Close{CloseConn: x}))
		case core.PauseReads:
			kernel.Call(r, svc, fakenetwork.Call(fakenetwork.Pause{PauseReads: x}))
		case core.ResumeReads:
			kernel.Call(r, svc, fakenetwork.Call(fakenetwork.Resume{ResumeReads: x}))
		}
	}
	return servicetest.Subject{StateHash: e.StateHash, AddNode: e.AddNode}
}

// transcripts runs every case against the current service and returns the transcripts
// by case name.
func transcripts(t *testing.T, cases []servicetest.Case, install func(*servicetest.Harness, servicetest.Case) servicetest.Subject) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, c := range cases {
		h := servicetest.New(t, c.Seed, c.Nodes...)
		sv := install(h, c)
		c.Run(h, sv)
		out[c.Name] = h.Transcript(c.Name)
	}
	return out
}

// goldenBlocks reads a parity file and splits it into one block for each case.
func goldenBlocks(t *testing.T, file string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", file)) //nolint:gosec // G304: the test reads its own testdata file
	if err != nil {
		t.Fatal(err)
	}
	blocks := map[string]string{}
	for _, b := range strings.SplitAfter(string(data), "\n\n") {
		if b = strings.TrimLeft(b, "\n"); b == "" {
			continue
		}
		first, _, _ := strings.Cut(b, "\n")
		fields := strings.Fields(first) // "case NAME seed N"
		if len(fields) < 2 || fields[0] != "case" {
			t.Fatalf("%s: bad block start %q", file, first)
		}
		blocks[fields[1]] = strings.TrimRight(b, "\n") + "\n\n"
	}
	return blocks
}

func checkParity(t *testing.T, file string, cases []servicetest.Case, install func(*servicetest.Harness, servicetest.Case) servicetest.Subject) {
	t.Helper()
	want := goldenBlocks(t, file)
	got := transcripts(t, cases, install)
	if len(want) != len(cases) {
		t.Fatalf("%s has %d cases, the test runs %d", file, len(want), len(cases))
	}
	for _, c := range cases {
		if want[c.Name] != got[c.Name] {
			t.Errorf("case %s differs from the saved transcript.\n--- saved\n%s--- now\n%s", c.Name, want[c.Name], got[c.Name])
		}
	}
}

func TestParityObjectStore(t *testing.T) {
	checkParity(t, "parity_objectstore.txt", servicetest.ObjectCases(), newObjectStore)
}

func TestParityControlPlane(t *testing.T) {
	checkParity(t, "parity_controlplane.txt", servicetest.ControlPlaneCases(), newControlPlane)
}

func TestParityNetwork(t *testing.T) {
	checkParity(t, "parity_network.txt", servicetest.NetworkCases(), newNetwork)
}

// The cases must exercise what they claim: every fault kind of every fake must
// happen in some case, or the comparison would not see it.
func TestParityCasesReachTheFaults(t *testing.T) {
	want := map[string][]string{
		"object":       {"obj_outage", "obj_503", "obj_timeout", "obj_lost_response", "obj_get_corrupt"},
		"controlplane": {"cp_outage", "cp_503", "cp_timeout", "cp_lost_response"},
		"network":      {"partition", "random_close", "black_hole", "clog"},
	}
	groups := map[string]struct {
		cases   []servicetest.Case
		install func(*servicetest.Harness, servicetest.Case) servicetest.Subject
	}{
		"object":       {servicetest.ObjectCases(), newObjectStore},
		"controlplane": {servicetest.ControlPlaneCases(), newControlPlane},
		"network":      {servicetest.NetworkCases(), newNetwork},
	}
	for name, g := range groups {
		seen := map[string]bool{}
		for _, c := range g.cases {
			h := servicetest.New(t, c.Seed, c.Nodes...)
			c.Run(h, g.install(h, c))
			for _, f := range h.Faults() {
				seen[f] = true
			}
		}
		for _, f := range want[name] {
			if !seen[f] {
				t.Errorf("%s: no case injects %s", name, f)
			}
		}
	}
}
