package metadb_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/pebble"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// benchSegment is a commit of parts distinct partitions (1-2 batches each) out
// of 4 topics x 10,000 partitions: the stage 1 target shape (40k partitions).
func benchSegment(rng *rand.Rand, n, parts int) contract.CommitSegment {
	cs := contract.CommitSegment{SegmentKey: fmt.Sprintf("seg/%032x", n)}
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
			cs.Batches = append(cs.Batches, contract.PendingBatch{
				TopicPartition: contract.NewTopicPartition(fmt.Sprintf("topic-%d", p/10000), int32(p%10000)),
				Records:        int64(1 + rng.Intn(50)), Loc: runtime.SegmentRef{Key: cs.SegmentKey, Off: off, Len: ln}})
			off += ln
		}
	}
	return cs
}

// BenchmarkApply150 applies segments of 150 partitions, one command per
// transaction, onto a store holding 2,000 segments already. The reported
// segs/s is the apply rate to compare with 430 segments/s (docs/applier-bakeoff.md).
func BenchmarkApply150(b *testing.B) {
	keyValue, err := pebble.Open(filepath.Join(b.TempDir(), "state"), pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = keyValue.Close() }()
	a := metadb.New(keyValue)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // G404: deterministic test/sim PRNG
	n := 0
	apply := func(k int) {
		for range k {
			if _, err := a.Write(int64(n), benchSegment(rng, n, 150)); err != nil {
				b.Fatal(err)
			}
			n++
		}
	}
	apply(2000)
	b.ResetTimer()
	apply(b.N)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "segs/s")
}
