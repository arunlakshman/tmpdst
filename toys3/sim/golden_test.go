package sim

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

// The golden test pins the exact behavior of the simulator and the server:
// for a fixed set of seeds it records the step count, the number of random
// draws, the trace hash and every statistic, plus where each mutant is first
// caught. A pure refactor (renames, moves) must reproduce the file byte for
// byte. Regenerate it only for a deliberate behavior change:
//
//	go test ./toys3/sim -run TestGolden -update-golden
//
// Stats are recorded as a fixed-order vector of numbers (not Stats.String),
// so renaming a field or its printed key does not touch the golden file.
var updateGolden = flag.Bool("update-golden", false, "TestGolden: rewrite testdata/golden-v1.txt")

const goldenPath = "testdata/golden-v1.txt"

// statsVector lists every Stats field in a fixed order. When a field is
// renamed, rename it here too, keeping its position.
func statsVector(s Stats) []int {
	return []int{
		s.Requests, s.ByStatus[0], s.ByStatus[1], s.ByStatus[2], s.ByStatus[3],
		s.Indetermined, s.FSOps, s.EIOs, s.TornFiles, s.LostChanges,
		s.Crashes, s.Halts, s.CrashesMidPut, s.KeptChanges, s.ReadCorruptions,
		s.CorruptObjects, s.TempsRemoved, s.Windows, s.MaxWindowOps, s.Widenings,
	}
}

func goldenRow(kind string, seed uint64, r Result) string {
	return fmt.Sprintf("%s seed=%d steps=%d rand=%d trace=%016x crashes=%d end=%d fails=%d stats=%v",
		kind, seed, r.Steps, r.RandCalls, r.TraceHash, r.Crashes, r.EndTime, len(r.Failures), statsVector(r.Stats))
}

func goldenText() string {
	var b strings.Builder
	for seed := uint64(1); seed <= 500; seed++ {
		b.WriteString(goldenRow("default", seed, Run(DefaultConfig(seed))))
		b.WriteByte('\n')
	}
	for seed := uint64(1); seed <= 50; seed++ {
		b.WriteString(goldenRow("base", seed, Run(BaseConfig(seed))))
		b.WriteByte('\n')
	}
	for _, m := range mutants {
		for _, set := range []struct {
			name string
			invs func() []Invariant
		}{{"all", DefaultInvariants}, {"blackbox", BlackBoxInvariants}} {
			seed, f := firstCatch(m.factory, set.invs, 500)
			cfg := DefaultConfig(seed)
			cfg.Factory = m.factory
			cfg.Invariants = set.invs()
			r := Run(cfg)
			fmt.Fprintf(&b, "mutant %s %s caught seed=%d invariant=%s step=%d trace=%016x\n",
				m.name, set.name, seed, f.Invariant, f.Step, r.TraceHash)
		}
		// Seed 1 of every mutant, caught or not (the book quotes some of these).
		cfg := DefaultConfig(1)
		cfg.Factory = m.factory
		b.WriteString(goldenRow("mutant-seed1 "+m.name, 1, Run(cfg)))
		b.WriteByte('\n')
	}
	return b.String()
}

func TestGolden(t *testing.T) {
	got := goldenText()
	if *updateGolden {
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("%v (generate with -update-golden)", err)
	}
	if got == string(want) {
		return
	}
	g, w := strings.Split(got, "\n"), strings.Split(string(want), "\n")
	for i := 0; i < len(g) || i < len(w); i++ {
		var gl, wl string
		if i < len(g) {
			gl = g[i]
		}
		if i < len(w) {
			wl = w[i]
		}
		if gl != wl {
			t.Fatalf("behavior changed: %s line %d\n got  %s\n want %s", goldenPath, i+1, gl, wl)
		}
	}
}
