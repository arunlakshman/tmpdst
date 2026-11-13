package kafka

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/golden_hashes.txt from the current code")

const goldenFile = "testdata/golden_hashes.txt"

// goldenSeeds is how many seeds, from seed 1, are pinned.
const goldenSeeds = 5

// goldenLines runs the pinned seeds of the kafka scenario and returns one line
// per seed.
func goldenLines() []string {
	var out []string
	for seed := uint64(1); seed <= goldenSeeds; seed++ {
		r := Run(Config{Seed: seed})
		out = append(out, fmt.Sprintf("kafka seed=%d steps=%d rand=%d hash=%016x", seed, r.Steps, r.RandCalls, r.Hash))
	}
	return out
}

// TestGoldenTraceHashes pins the trace of a few seeds. TestDeterminismCanary
// only compares a seed with itself, so it cannot see a change in behaviour.
// This test can. When a change is meant to alter the traces, review why, then
// run: go test ./internal/verify/simulation/kafka -run TestGoldenTraceHashes -update-golden
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
