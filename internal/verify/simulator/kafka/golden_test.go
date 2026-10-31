package kafka

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"mds/internal/verify/simulator"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/golden_hashes.txt from the current code")

const goldenFile = "testdata/golden_hashes.txt"

// goldenSeeds is how many seeds, from seed 1, are pinned for each scenario.
const goldenSeeds = 5

// goldenLines runs the pinned seeds of the broker scenario (kafka) and of the
// control plane scenario (kafka-log) and returns one line per seed.
func goldenLines() []string {
	scenarios := []struct {
		name   string
		config simulator.Config
	}{
		{"kafka", Config(nil)},
		{"kafka-log", ConfigLog(nil)},
	}
	var out []string
	for _, sc := range scenarios {
		for seed := uint64(1); seed <= goldenSeeds; seed++ {
			r := simulator.RunSeed(sc.config, seed)
			out = append(out, fmt.Sprintf("%s seed=%d steps=%d rand=%d hash=%016x", sc.name, seed, r.Steps, r.RandCalls, r.TraceHash))
		}
	}
	return out
}

// TestGoldenTraceHashes pins the trace of a few seeds. TestDeterminismCanary
// only compares a seed with itself, so it cannot see a change in behaviour.
// This test can. When a change is meant to alter the traces, review why, then
// run: go test ./internal/verify/simulator/kafka -run TestGoldenTraceHashes -update-golden
func TestGoldenTraceHashes(t *testing.T) {
	got := strings.Join(goldenLines(), "\n") + "\n"
	if *updateGolden {
		if err := os.WriteFile(goldenFile, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf("%v (create it with -update-golden)", err)
	}
	if string(want) != got {
		t.Fatalf("trace hashes changed. If this is intended, review it and re-run with -update-golden.\nwant:\n%sgot:\n%s", want, got)
	}
}
