package cptest

import (
	"testing"

	"manifester/core"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

// TestGeneratorCoverage guards the generator: the sequences must reach the
// interesting spec outcomes, or conformance proves little.
func TestGeneratorCoverage(t *testing.T) {
	seen := map[string]int{}
	for seed := uint64(1); seed <= 200; seed++ {
		r := veritastest.NewRand(seed)
		spec := veritas.NewControlPlane()
		for i := 0; i < 200; i++ {
			o := gen(r, i)
			if o.Kind == "create" || o.Kind == "init" && false {
				spec.CreateTopic(o.Name, o.Parts)
				continue
			}
			d := spec.Exec(core.CPCall{Op: cpOp(o)}).(core.CPDone)
			if o.Kind != "commit" {
				seen[o.Kind+"/"+d.Err.String()]++
				continue
			}
			seen["commit/"+d.Err.String()]++
			if d.Err == core.OK {
				for _, b := range d.Resp.(core.CommitSegmentResp).Assigned {
					switch {
					case b.Dup:
						seen["batch/dup"]++
					case b.Err != core.OK:
						seen["batch/"+b.Err.String()]++
					case b.Base >= 0:
						seen["batch/assigned"]++
					}
				}
			}
		}
	}
	t.Log(seen)
	for _, k := range []string{"commit/OK", "commit/ErrFenced", "commit/ErrConflict", "commit/ErrInvalid", "batch/dup", "batch/ErrOutOfOrder", "batch/ErrFenced", "batch/assigned"} {
		if seen[k] < 20 {
			t.Errorf("generator rarely reaches %s (%d)", k, seen[k])
		}
	}
}
