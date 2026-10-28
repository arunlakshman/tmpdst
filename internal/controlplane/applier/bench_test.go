package applier_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/controlplane/applier/pebblekv"
	"manifester/internal/core"
)

// benchSegment is a commit of parts distinct partitions (1-2 batches each) out
// of 4 topics x 10,000 partitions: the stage 1 target shape (40k partitions).
func benchSegment(rng *rand.Rand, n, parts int) core.CommitSegment {
	cs := core.CommitSegment{SegmentKey: fmt.Sprintf("seg/%032x", n)}
	seen := map[int]bool{}
	var off int64
	for len(seen) < parts {
		p := rng.Intn(40000)
		if seen[p] {
			continue
		}
		seen[p] = true
		for i := 1 + rng.Intn(2); i > 0; i-- {
			ln := int64(8000 + rng.Intn(24000))
			cs.Batches = append(cs.Batches, core.PendingBatch{
				TP:      core.TP(fmt.Sprintf("topic-%d", p/10000), int32(p%10000)),
				Records: int64(1 + rng.Intn(50)), Loc: core.SegmentRef{Key: cs.SegmentKey, Off: off, Len: ln}})
			off += ln
		}
	}
	return cs
}

// BenchmarkApply150 applies segments of 150 partitions, 8 commands per
// transaction, onto a store holding 2,000 segments already. The reported
// segs/s is the apply rate to compare with 430 segments/s (docs/applier-bakeoff.md).
func BenchmarkApply150(b *testing.B) {
	kv, err := pebblekv.Open(filepath.Join(b.TempDir(), "state"), pebblekv.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	a := applier.New(kv)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // G404: deterministic test/sim PRNG
	n := 0
	apply := func(k int) {
		for done := 0; done < k; done += 8 {
			var batch []applier.Command
			for j := 0; j < 8 && done+j < k; j++ {
				batch = append(batch, applier.Command{Slot: int64(n), Op: benchSegment(rng, n, 150)})
				n++
			}
			if _, err := a.CommitBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
	}
	apply(2000)
	b.ResetTimer()
	apply(b.N)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "segs/s")
}
