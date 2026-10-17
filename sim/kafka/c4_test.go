package kafka_test

import (
	"go/parser"
	"go/token"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"manifester/core"
	"manifester/sim"
	"manifester/sim/kafka"
)

// C4 (veritas/doc.go): the Kafka oracle against the veritas.Partition and
// veritas.ControlPlane specs. The oracle never imports veritas (decision 21);
// this external test package may, because detcheck does not read _test.go
// files and only export_test.go (package kafka, no veritas) reaches into the
// oracle. Run: go test ./sim/kafka -run C4

func TestC4OracleDoesNotImportVeritas(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkgs {
		for name, f := range p.Files {
			for _, imp := range f.Imports {
				if strings.Contains(imp.Path.Value, "veritas") {
					t.Errorf("%s imports %s", name, imp.Path.Value)
				}
			}
		}
	}
}

// verdict runs a fresh oracle over the finished world: one Observe, AtEnd.
func verdict(w *world) (failures, sim.Invariant) {
	w.as = failures{}
	inv := kafka.NewOracle(w.opts)
	inv.Observe(w)
	inv.AtEnd(w)
	return w.as, inv
}

// sameAsSpec: the oracle's reconstructed log of every partition is exactly
// what veritas.Partition holds.
func sameAsSpec(t *testing.T, seed uint64, w *world, inv sim.Invariant) {
	t.Helper()
	for _, tp := range w.tps {
		recs, _ := w.parts[tp].Read(0, 0)
		want := make([]string, len(recs))
		for i, r := range recs {
			want[i] = string(r.Value)
		}
		if got := kafka.OracleLog(inv, tp); !slices.Equal(got, want) {
			t.Fatalf("seed %d %v: oracle log %v, Partition %v", seed, tp, got, want)
		}
	}
}

func TestC4AcceptsCorrectHistories(t *testing.T) {
	var tot stats
	var maxLog int
	for seed := uint64(1); seed <= 200; seed++ {
		// Incremental: the oracle (and trippedNeverAcks) observe every step.
		w := newWorld(seed, true)
		for _, inv := range w.invs {
			inv.AtEnd(w)
		}
		if len(w.as) > 0 {
			t.Fatalf("seed %d: live oracle rejected a correct history: %v", seed, w.as)
		}
		sameAsSpec(t, seed, w, w.invs[0])
		// One-shot: a fresh oracle sees only the finished truth.
		w2 := newWorld(seed, false)
		f, inv := verdict(w2)
		if len(f) > 0 {
			t.Fatalf("seed %d: one-shot oracle rejected a correct history: %v", seed, f)
		}
		sameAsSpec(t, seed, w2, inv)
		for _, tp := range w2.tps {
			maxLog = max(maxLog, int(w2.parts[tp].HighWatermark()))
		}
		s := w2.st
		tot = stats{tot.dups + s.dups, tot.indetLand + s.indetLand, tot.indetNo + s.indetNo, tot.rejected + s.rejected,
			tot.reinit + s.reinit, tot.bump + s.bump, tot.multi + s.multi, tot.nonIdem + s.nonIdem, tot.fenced + s.fenced}
	}
	t.Logf("coverage over 200 seeds: %+v, longest partition %d", tot, maxLog)
	for name, n := range map[string]int{"dup retries": tot.dups, "indeterminate landed": tot.indetLand, "indeterminate never landed": tot.indetNo,
		"rejected": tot.rejected, "pid re-init": tot.reinit, "epoch bump": tot.bump, "multi-batch segments": tot.multi,
		"non-idempotent": tot.nonIdem, "fenced brokers": tot.fenced} {
		if n == 0 {
			t.Errorf("generator never produced: %s", name)
		}
	}
}

func TestC4RejectsEveryFault(t *testing.T) {
	const wantApplied, maxSeeds = 25, 400
	for _, f := range faults() {
		t.Run(f.name, func(t *testing.T) {
			applied := 0
			for seed := uint64(1); seed <= maxSeeds && applied < wantApplied; seed++ {
				w := newWorld(seed, false)
				if !f.apply(w, rand.New(rand.NewPCG(seed, 0xfa17))) {
					continue
				}
				applied++
				w.rebuildTL()
				w.touch()
				got, _ := verdict(w)
				hit := 0
				for _, p := range f.want {
					if got[p] > 0 {
						hit++
					} else if !f.anyOf {
						t.Errorf("seed %d: oracle did not report %q (got %v)", seed, p, got)
					}
				}
				if f.anyOf && hit == 0 {
					t.Errorf("seed %d: oracle reported none of %q (got %v)", seed, f.want, got)
				}
			}
			if applied < wantApplied {
				t.Errorf("fault applicable in only %d of %d seeds", applied, maxSeeds)
			}
		})
	}
}

// fenceRun feeds acks and broker_fenced facts for 3 brokers to
// trippedNeverAcks, observing after every fact. With bad, a fenced broker acks
// again at the end. Reports the violations and whether a broker was fenced.
func fenceRun(seed uint64, bad bool) (failures, bool) {
	r := rand.New(rand.NewPCG(seed, 0xfe))
	w := &world{tl: sim.NewTimeline(), as: failures{}}
	inv := kafka.NewTripped()
	dead := map[int]bool{}
	emit := func(name string, n int) {
		w.emit(nodeRef("b"+strconv.Itoa(n)), core.Fact{Name: name})
		inv.Observe(w)
	}
	for i := 0; i < 40; i++ {
		n := r.IntN(3)
		switch {
		case dead[n]:
		case r.IntN(8) == 0 && len(dead) < 2:
			dead[n] = true
			emit("broker_fenced", n)
		default:
			emit("produce_acked", n)
		}
	}
	if bad {
		for n := range dead {
			emit("produce_acked", n)
			break
		}
	}
	return w.as, len(dead) > 0
}

func TestC4FencedBrokerNeverAcks(t *testing.T) {
	bad := 0
	for seed := uint64(1); seed <= 200; seed++ {
		if f, _ := fenceRun(seed, false); len(f) > 0 {
			t.Fatalf("seed %d: correct run rejected: %v", seed, f)
		}
		f, fenced := fenceRun(seed, true)
		if !fenced {
			continue
		}
		bad++
		if f[pFenced] == 0 {
			t.Errorf("seed %d: ack after broker_fenced not reported (got %v)", seed, f)
		}
	}
	if bad < 100 {
		t.Errorf("only %d seeds fenced a broker", bad)
	}
}
