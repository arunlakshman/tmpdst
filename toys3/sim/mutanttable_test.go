package sim

import (
	"testing"

	"manifester/toys3/control"
)

// TestMutantTable pins who catches each mutant, the DST ones (a-f, d2) and
// the functional ones (g-j): the first seed and invariant of DST
// (DefaultInvariants, seeds 1..500), and the first seed and shrunk
// sequence length of conformance check C1 on each storage backend (seeds
// 1..c1MutantSeeds). It is the table in DESIGN.md ("Bugs DST catches"); if
// it changes, update the table.
//
// Only C1 is run: C2 and C3 check storage, not the controller, so no mutant
// can reach them, and C4 is C1 plus the oracle on the same controller. A
// mutant C1 cannot catch by design has a reason instead of a catch.

const (
	dstMutantSeeds = 500
	c1MutantSeeds  = 200
)

// c1Catch is where C1 first catches a mutant on one backend: the seed and
// the length of the shrunk sequence. Seed 0 means not caught.
type c1Catch struct {
	Seed uint64
	Ops  int
}

type mutantRow struct {
	dst     string  // invariant that first catches it; "" if DST does not
	dstSeed uint64  // its first seed
	c1      c1Catch // on veritas.FS
	c1Model c1Catch // on StorageModel (no faults)
	why     string  // why C1 misses it, where it does
}

var mutantTable = map[string]mutantRow{
	"a-skip-dir-fsync": {dst: "acked-is-durable", dstSeed: 1, c1Model: c1Catch{1, 1},
		why: "veritas.FS has no durable view; StorageModel's durable view catches it"},
	"b-skip-file-fsync": {dst: "obj-never-torn", dstSeed: 1, c1Model: c1Catch{1, 1},
		why: "veritas.FS has no durable view; StorageModel's durable view catches it"},
	"c-ack-before-rename": {dst: "acked-is-durable", dstSeed: 1,
		why: "C1 runs every storage command before comparing: only a crash or a concurrent reader sees the early ack"},
	"d-no-halt-on-error": {dst: "obj-never-torn", dstSeed: 1,
		why: "needs an I/O error; C1 is fault-free"},
	"d2-no-halt-on-dir-fsync-error": {dst: "linearizable", dstSeed: 132,
		why: "needs an I/O error; C1 is fault-free"},
	"e-skip-crc": {dst: "no-corrupt-reads", dstSeed: 18,
		why: "needs a corrupt read; C1 is fault-free"},
	"f-skip-temp-cleanup": {dst: "linearizable", dstSeed: 20, c1: c1Catch{1, 1}, c1Model: c1Catch{1, 1}},
	"g-copy-to-src":       {dst: "linearizable", dstSeed: 1, c1: c1Catch{1, 2}, c1Model: c1Catch{1, 2}},
	"h-delete-missing-ok": {dst: "linearizable", dstSeed: 1, c1: c1Catch{1, 1}, c1Model: c1Catch{1, 1}},
	// DST misses (i): its Puts are at most 40 bytes, under MaxObjectSize.
	"i-size-off-by-one":      {c1: c1Catch{1, 1}, c1Model: c1Catch{1, 1}},
	"j-recovery-deletes-obj": {dst: "linearizable", dstSeed: 1, c1: c1Catch{1, 2}, c1Model: c1Catch{1, 2}},
}

// firstC1Catch runs C1 with factory on newBackend's storage, seeds
// 1..c1MutantSeeds, and returns the first catch.
func firstC1Catch(t *testing.T, factory control.Factory, newBackend func() storeBackend) c1Catch {
	c := storeCheck(factory, newBackend, 0)
	for seed := uint64(1); seed <= c1MutantSeeds; seed++ {
		if f := c.RunSeed(seed); f != nil {
			t.Logf("%s", c.Report(f, ""))
			return c1Catch{seed, len(f.Ops)}
		}
	}
	return c1Catch{}
}

func TestMutantTable(t *testing.T) {
	all := append(append([]mutant(nil), mutants...), functionalMutants...)
	if len(all) != len(mutantTable) {
		t.Fatalf("%d mutants, %d rows in mutantTable", len(all), len(mutantTable))
	}
	for _, m := range all {
		t.Run(m.name, func(t *testing.T) {
			want, ok := mutantTable[m.name]
			if !ok {
				t.Fatalf("no row in mutantTable")
			}
			var got mutantRow
			var f Failure
			got.dstSeed, f = firstCatch(m.factory, DefaultInvariants, dstMutantSeeds)
			got.dst = f.Invariant
			got.c1 = firstC1Catch(t, m.factory, newVeritasBackend)
			got.c1Model = firstC1Catch(t, m.factory, newModelBackend)
			got.why = want.why
			t.Logf("DST: %s at seed %d; C1 veritasFS: %+v; C1 StorageModel: %+v; %s",
				f.Invariant, got.dstSeed, got.c1, got.c1Model, want.why)
			if got != want {
				t.Errorf("got  %+v\nwant %+v\n(update mutantTable and DESIGN.md's \"Bugs DST catches\")", got, want)
			}
			if (got.c1.Seed == 0 || got.c1Model.Seed == 0) && want.why == "" {
				t.Errorf("C1 misses it on a backend, but the row gives no reason")
			}
		})
	}
}
