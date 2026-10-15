package sim

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"manifester/toys3/control"
)

// Conformance check C5: porcupine agrees with the oracle. Porcupine
// (github.com/anishathalye/porcupine, a test-only dependency) is a second,
// independent linearizability checker. The client history the oracle
// records is translated into porcupine operations, checked against a
// nondeterministic model of the same register map, and the two verdicts
// must agree. The oracle is not changed and nothing here feeds back into a
// run, so TestGolden is unaffected.
//
// The translation mirrors the oracle's rules (Oracle, wop.apply):
//
//   - Call and Return are the history's InvSeq and RetSeq, the exact
//     logical order of invocations, replies and crashes.
//   - A Definite op takes effect once, with its replied result.
//   - A Failed (500), Abandoned (crash) or still Pending mutation may or may
//     not take effect, at one instant in [invoke, return]; Pending returns
//     at +inf, as the oracle's CheckFinal treats it. The model step offers
//     both outcomes ("applied" and "unchanged").
//   - Reads without a Definite reply, and 400s, are dropped. A GET 200 body
//     that does not decode (ValueWriter) is dropped too: NoCorruptReads
//     reports it, and the oracle fails it outright.
//   - Crashes erase nothing, so there is no crash step in the model.
//   - A PUT writes the value named by its own request id.
//
// What porcupine does not have is the oracle's widening to "unknown": it
// keeps every possible state, so where the oracle widens it is the stricter
// of the two.
//
// The history is partitioned by key (P-compositionality): union-find over
// keys, linked by COPY's src and dst, over the whole history. The oracle
// partitions per window instead, which porcupine cannot do.

var (
	flagPorcupineOut     = flag.String("porcupine-out", "", "TestC5*: directory for porcupine's HTML visualization of a disagreement (default: a new temp dir)")
	flagPorcupineTimeout = flag.Duration("porcupine-timeout", 60*time.Second, "TestC5*: porcupine time limit per partition (exceeded: Unknown)")
	flagC5Seeds          = flag.Int("c5-seeds", 20, "TestC5PorcupineAgreesWithOracle: seeds when neither -seeds nor -duration is given")
	flagC5MutantSeeds    = flag.Int("c5-mutant-seeds", 4, "TestC5PorcupineAgreesOnMutants: seeds 1..N per mutant (plus the mutant table's seed)")
)

// ---- translation ----

// pcInput is one porcupine operation's input.
type pcInput struct {
	Op       control.Op
	Req      control.ClientReq // the request; a PUT's value is named by it
	Optional bool              // Failed, Abandoned or Pending mutation
}

// pcOutput is what the client saw: the status (zero when Optional) and, for
// a GET 200, the writer named by the body.
type pcOutput struct {
	Status control.Status
	Reads  control.ClientReq
}

// pcState is one state of the register map: key → writer of its value; an
// absent key is not in the map. Immutable: steps return a new one. h is an
// order-independent hash of m, kept up to date incrementally.
type pcState struct {
	m map[string]control.ClientReq
	h uint64
}

func entryHash(k string, w control.ClientReq) uint64 { return Fold(hashString(k), uint64(w)) }

func (s pcState) with(k string, w control.ClientReq) pcState {
	n := pcState{m: make(map[string]control.ClientReq, len(s.m)+1), h: s.h}
	maps.Copy(n.m, s.m)
	if old, ok := s.m[k]; ok {
		n.h ^= entryHash(k, old)
	}
	n.m[k] = w
	n.h ^= entryHash(k, w)
	return n
}

func (s pcState) without(k string) pcState {
	old, ok := s.m[k]
	if !ok {
		return s
	}
	n := pcState{m: maps.Clone(s.m), h: s.h ^ entryHash(k, old)}
	delete(n.m, k)
	return n
}

func (s pcState) equal(o pcState) bool { return s.h == o.h && maps.Equal(s.m, o.m) }

func (s pcState) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range slices.Sorted(maps.Keys(s.m)) {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%d", k, s.m[k])
	}
	b.WriteByte('}')
	return b.String()
}

// pcStep mirrors wop.apply for known values (porcupine has no "unknown"):
// the possible next states, none if the replied result contradicts st.
func pcStep(st pcState, in pcInput, out pcOutput) []any {
	one := func(s pcState) []any { return []any{s} }
	switch op := in.Op.(type) {
	case control.Get: // always Definite: the others are dropped
		w, present := st.m[op.Key]
		if out.Status == control.StatusOK {
			if present && w == out.Reads {
				return one(st)
			}
			return nil
		}
		if !present { // 404
			return one(st)
		}
		return nil
	case control.Put:
		next := st.with(op.Key, in.Req)
		if in.Optional {
			return []any{st, next}
		}
		return one(next)
	case control.Delete:
		_, present := st.m[op.Key]
		if in.Optional {
			return []any{st, st.without(op.Key)}
		}
		if out.Status == control.StatusOK {
			if present {
				return one(st.without(op.Key))
			}
			return nil
		}
		if !present { // 404
			return one(st)
		}
		return nil
	case control.Copy:
		src, present := st.m[op.Src]
		if in.Optional {
			if !present {
				return one(st)
			}
			return []any{st, st.with(op.Dst, src)}
		}
		if out.Status == control.StatusOK {
			if present {
				return one(st.with(op.Dst, src))
			}
			return nil
		}
		if !present { // 404
			return one(st)
		}
		return nil
	}
	return nil
}

// pcSpec is the register map as a porcupine NondeterministicModel.
var pcSpec = porcupine.NondeterministicModel{
	Init: func() []any { return []any{pcState{}} },
	Step: func(st, in, out any) []any {
		return pcStep(st.(pcState), in.(pcInput), out.(pcOutput))
	},
	Equal: func(a, b any) bool { return a.(pcState).equal(b.(pcState)) },
	Hash:  func(st any) uint64 { return st.(pcState).h },
	DescribeOperation: func(in, out any) string {
		i, o := in.(pcInput), out.(pcOutput)
		s := fmt.Sprintf("req %d %s", i.Req, DescribeOp(i.Op))
		switch {
		case i.Optional:
			return s + " -> ? (500/abandoned/pending)"
		case o.Reads != 0:
			return fmt.Sprintf("%s -> 200 value-of-%d", s, o.Reads)
		}
		return fmt.Sprintf("%s -> %d", s, o.Status)
	},
	DescribeState: func(st any) string { return st.(pcState).String() },
}

// maxStateSet is the largest set of possible states porcupine carried in
// any search (the power-set construction's state), across all checks.
var maxStateSet atomic.Int64

// pcSet is a set of possible states, sorted by hash, with a hash of the set.
type pcSet struct {
	states []pcState
	h      uint64
}

// pcModel is pcSpec made deterministic by the power-set construction, as
// NondeterministicModel.ToModel does, but deduplicating by hash: ToModel
// merges with pairwise Equal, quadratic in the set size, and after a few
// crashes the set holds tens of thousands of states (C5 on 100 seeds: 5
// histories Unknown after 20s each, sets of up to 36864 states).
func pcModel() porcupine.Model {
	newSet := func(states []pcState) pcSet {
		slices.SortFunc(states, func(a, b pcState) int {
			switch {
			case a.h < b.h:
				return -1
			case a.h > b.h:
				return 1
			}
			return 0
		})
		var out pcSet
		for i, s := range states {
			dup := false
			for j := i - 1; j >= 0 && states[j].h == s.h; j-- {
				if states[j].equal(s) {
					dup = true
					break
				}
			}
			if !dup {
				out.states = append(out.states, s)
				out.h = Fold(out.h, s.h)
			}
		}
		return out
	}
	step := func(ctx context.Context, st, in, out any) (bool, any) {
		var next []pcState
		for i, s := range st.(pcSet).states {
			if i%1024 == 0 && ctx.Err() != nil {
				return false, nil
			}
			for _, n := range pcSpec.Step(s, in, out) {
				next = append(next, n.(pcState))
			}
		}
		if len(next) == 0 {
			return false, nil
		}
		set := newSet(next)
		n := int64(len(set.states))
		for cur := maxStateSet.Load(); n > cur && !maxStateSet.CompareAndSwap(cur, n); cur = maxStateSet.Load() {
		}
		return true, set
	}
	return porcupine.Model{
		Init:        func() any { return newSet([]pcState{{}}) },
		StepContext: step,
		Equal: func(a, b any) bool {
			x, y := a.(pcSet), b.(pcSet)
			return x.h == y.h && slices.EqualFunc(x.states, y.states, pcState.equal)
		},
		Hash:              func(st any) uint64 { return st.(pcSet).h },
		DescribeOperation: pcSpec.DescribeOperation,
		DescribeState: func(st any) string {
			var parts []string
			for _, s := range st.(pcSet).states {
				parts = append(parts, s.String())
			}
			return "{" + strings.Join(parts, ", ") + "}"
		},
	}
}

// toOperations translates a history (see the rules above).
func toOperations(h *History) []porcupine.Operation {
	var ops []porcupine.Operation
	for i := range h.Ops {
		o := &h.Ops[i]
		if o.Outcome == Definite && o.Status == control.StatusBadRequest {
			continue // rejected before touching anything
		}
		in := pcInput{Op: o.Op, Req: o.Req, Optional: o.Outcome != Definite}
		var out pcOutput
		if !in.Optional {
			out.Status = o.Status
		}
		if _, isGet := o.Op.(control.Get); isGet {
			if in.Optional {
				continue // a read with no reply constrains nothing
			}
			if o.Status == control.StatusOK {
				w, ok := ValueWriter(o.Body)
				if !ok {
					continue // garbage: NoCorruptReads' business
				}
				out.Reads = w
			}
		}
		ret := int64(o.RetSeq)
		if o.Outcome == Pending {
			ret = math.MaxInt64
		}
		ops = append(ops, porcupine.Operation{
			ClientId: o.Client, Input: in, Output: out,
			Call: int64(o.InvSeq), Return: ret, Metadata: i,
		})
	}
	return ops
}

// partition splits ops into independent groups of keys: union-find over
// keys, linked by COPY. Groups are ordered by their first op.
func partition(ops []porcupine.Operation) [][]porcupine.Operation {
	parent := map[string]string{}
	var find func(k string) string
	find = func(k string) string {
		p, ok := parent[k]
		if !ok || p == k {
			parent[k] = k
			return k
		}
		r := find(p)
		parent[k] = r
		return r
	}
	for _, op := range ops {
		ks := opKeys(op.Input.(pcInput).Op)
		for _, k := range ks[1:] {
			a, b := find(ks[0]), find(k)
			if a != b {
				parent[max(a, b)] = min(a, b)
			}
		}
	}
	index := map[string]int{}
	var parts [][]porcupine.Operation
	for _, op := range ops {
		r := find(opKeys(op.Input.(pcInput).Op)[0])
		i, ok := index[r]
		if !ok {
			i = len(parts)
			index[r] = i
			parts = append(parts, nil)
		}
		parts[i] = append(parts[i], op)
	}
	return parts
}

// pcResult is porcupine's verdict on one history.
type pcResult struct {
	Result     porcupine.CheckResult
	Ops, Parts int
	MaxPart    int           // ops in the largest partition
	Total      time.Duration // all partitions
	Illegal    []porcupine.Operation
}

// porcupineCheck checks h partition by partition (sequentially, to time
// each one), stopping at the first Illegal one.
func porcupineCheck(h *History, timeout time.Duration) pcResult {
	ops := toOperations(h)
	parts := partition(ops)
	r := pcResult{Result: porcupine.Ok, Ops: len(ops), Parts: len(parts)}
	m := pcModel()
	for _, p := range parts {
		r.MaxPart = max(r.MaxPart, len(p))
		start := time.Now()
		res := porcupine.CheckOperationsTimeout(m, p, timeout)
		r.Total += time.Since(start)
		switch res {
		case porcupine.Illegal:
			r.Result, r.Illegal = porcupine.Illegal, p
			return r
		case porcupine.Unknown:
			r.Result = porcupine.Unknown
		}
	}
	return r
}

var c5OutDir string

// visualize writes porcupine's HTML visualization of ops (one partition)
// and returns its path.
func visualize(t *testing.T, name string, ops []porcupine.Operation) string {
	t.Helper()
	if c5OutDir == "" {
		c5OutDir = *flagPorcupineOut
		if c5OutDir == "" {
			d, err := os.MkdirTemp("", "toys3-c5-")
			if err != nil {
				t.Fatal(err)
			}
			c5OutDir = d
		} else if err := os.MkdirAll(c5OutDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	m := pcModel()
	_, info := porcupine.CheckOperationsVerbose(m, ops, *flagPorcupineTimeout)
	path := filepath.Join(c5OutDir, name+".html")
	if err := porcupine.VisualizePath(m, info, path); err != nil {
		t.Errorf("visualize: %v", err)
		return ""
	}
	return path
}

// ---- the oracle's verdict ----

type oracleVerdict uint8

const (
	oracleOK      oracleVerdict = iota
	oracleIllegal               // no linearization (the WGL search failed)
	oracleOther                 // garbage read or checker budget: not a WGL verdict
)

func (v oracleVerdict) String() string {
	return [...]string{"ok", "illegal", "other"}[v]
}

// c5Run runs cfg and returns the run, the simulator (for its history) and
// the oracle's verdict on the whole recorded history. A run stops at the
// step of its first failure; if the oracle did not fail, it has not seen the
// tail yet, so CheckFinal checks it (Pending mutations as never returning).
func c5Run(cfg Config) (*Simulator, Result, oracleVerdict, string) {
	s := New(cfg)
	r := s.Run()
	var fs []Failure
	// Every invariant runs on the failing step, so the oracle may have
	// failed after another invariant did.
	if i := slices.IndexFunc(r.Failures, func(f Failure) bool { return f.Invariant == "linearizable" }); i >= 0 {
		fs = r.Failures[i : i+1]
	} else {
		fs = s.Oracle().CheckFinal()
	}
	if len(fs) == 0 {
		return s, r, oracleOK, ""
	}
	if strings.HasPrefix(fs[0].Msg, "no linearization") {
		return s, r, oracleIllegal, fs[0].Msg
	}
	return s, r, oracleOther, fs[0].Msg
}

// agree reports whether porcupine's result matches the oracle's verdict.
// Unknown (timeout) agrees with nothing and is counted separately.
func agree(v oracleVerdict, res porcupine.CheckResult) bool {
	return (v == oracleOK && res == porcupine.Ok) || (v == oracleIllegal && res == porcupine.Illegal)
}

// c5Seeds returns the seeds TestC5PorcupineAgreesWithOracle runs, honoring
// -seed, -duration and an explicit -seeds; else 1..-c5-seeds.
func c5Seeds() (one bool, n int) {
	if *flagSeed >= 0 {
		return true, 1
	}
	n = *flagC5Seeds
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "seeds" {
			n = *flagSeeds
		}
	})
	return false, n
}

// TestC5PorcupineAgreesWithOracle is conformance check C5 on the correct
// server: for every seed the oracle accepts, porcupine must say Ok.
// Flags: -seed, -seeds, -duration (as TestSeeds), -c5-seeds,
// -porcupine-timeout, -porcupine-out.
func TestC5PorcupineAgreesWithOracle(t *testing.T) {
	one, n := c5Seeds()
	start := time.Now()
	var (
		count          = map[porcupine.CheckResult]int{}
		times          []time.Duration
		maxPart, parts int
		slowest        uint64
		runs, skipped  int
		widened        int
		maxTime        time.Duration
	)
	maxStateSet.Store(0)
	for seed := uint64(1); ; seed++ {
		switch {
		case one:
			if runs == 1 {
				goto done
			}
			seed = uint64(*flagSeed)
		case *flagDuration > 0:
			if time.Since(start) >= *flagDuration {
				goto done
			}
		case seed > uint64(n):
			goto done
		}
		runs++
		s, r, v, msg := c5Run(DefaultConfig(seed))
		if v != oracleOK {
			// TestSeeds reports a correct-server failure; here it is only
			// counted, but porcupine is still asked for its opinion.
			skipped++
			t.Logf("seed %d: oracle %s (%.200s); failures %v", seed, v, msg, r.Failures)
		}
		pr := porcupineCheck(s.History(), *flagPorcupineTimeout)
		count[pr.Result]++
		times = append(times, pr.Total)
		if pr.Total >= maxTime {
			maxTime, slowest = pr.Total, seed
		}
		maxPart, parts = max(maxPart, pr.MaxPart), max(parts, pr.Parts)
		widened += r.Stats.Widenings
		switch {
		case pr.Result != porcupine.Unknown && pr.Total > time.Second:
			t.Logf("seed %d: porcupine %s took %v (%d ops, largest partition %d)", seed, pr.Result, pr.Total.Round(time.Millisecond), pr.Ops, pr.MaxPart)
		case pr.Result == porcupine.Unknown:
			t.Logf("seed %d: porcupine Unknown after %v (%d ops, largest partition %d)", seed, pr.Total, pr.Ops, pr.MaxPart)
		case v == oracleOK && pr.Result == porcupine.Illegal:
			path := visualize(t, fmt.Sprintf("seed-%d", seed), pr.Illegal)
			t.Errorf("seed %d: oracle accepts, porcupine Illegal (partition of %d ops; widenings %d)\nvisualization: %s\nrepro: go test ./toys3/sim -run TestC5PorcupineAgreesWithOracle -seed=%d -v",
				seed, len(pr.Illegal), r.Stats.Widenings, path, seed)
		case v == oracleIllegal && !agree(v, pr.Result):
			t.Errorf("seed %d: oracle Illegal, porcupine %s", seed, pr.Result)
		}
	}
done:
	slices.Sort(times)
	p99 := time.Duration(0)
	if len(times) > 0 {
		p99 = times[min(len(times)-1, len(times)*99/100)]
	}
	t.Logf("C5: %d seeds in %v (%d not accepted by the oracle): porcupine Ok=%d Illegal=%d Unknown=%d; "+
		"check time p50=%v p99=%v max=%v (seed %d); largest partition %d ops; most partitions %d; "+
		"largest possible-state set %d; oracle widenings %d",
		runs, time.Since(start).Round(time.Millisecond), skipped,
		count[porcupine.Ok], count[porcupine.Illegal], count[porcupine.Unknown],
		pct(times, 50), p99, pct(times, 100), slowest, maxPart, parts, maxStateSet.Load(), widened)
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, len(sorted)*p/100)].Round(time.Microsecond)
}

// TestC5PorcupineAgreesOnMutants checks the other direction: for every
// mutant whose catch in the mutant table is "linearizable", porcupine says
// Illegal on that seed; and on seeds 1..-c5-mutant-seeds of every mutant
// (black-box invariants, so the oracle sees as much history as it can), the
// two checkers' verdicts agree wherever the oracle gives one.
func TestC5PorcupineAgreesOnMutants(t *testing.T) {
	all := append(append([]mutant(nil), mutants...), functionalMutants...)
	for _, m := range all {
		t.Run(m.name, func(t *testing.T) {
			if row := mutantTable[m.name]; row.dst == "linearizable" {
				cfg := DefaultConfig(row.dstSeed)
				cfg.Factory, cfg.Invariants = m.factory, DefaultInvariants()
				s, _, v, msg := c5Run(cfg)
				pr := porcupineCheck(s.History(), *flagPorcupineTimeout)
				if v != oracleIllegal || pr.Result != porcupine.Illegal {
					t.Errorf("table seed %d: oracle %s, porcupine %s (%.300s)", row.dstSeed, v, pr.Result, msg)
				} else {
					t.Logf("table seed %d: both Illegal (porcupine %v, partition of %d ops)", row.dstSeed, pr.Total, len(pr.Illegal))
				}
			}
			tally := map[string]int{}
			for seed := uint64(1); seed <= uint64(*flagC5MutantSeeds); seed++ {
				cfg := DefaultConfig(seed)
				cfg.Factory, cfg.Invariants = m.factory, BlackBoxInvariants()
				s, r, v, msg := c5Run(cfg)
				pr := porcupineCheck(s.History(), *flagPorcupineTimeout)
				tally[fmt.Sprintf("oracle-%s/porcupine-%s", v, pr.Result)]++
				if pr.Total > time.Second {
					t.Logf("seed %d: porcupine %s took %v (%d ops, largest partition %d)", seed, pr.Result, pr.Total.Round(time.Millisecond), pr.Ops, pr.MaxPart)
				}
				if v == oracleOther || pr.Result == porcupine.Unknown || agree(v, pr.Result) {
					continue
				}
				var path string
				if pr.Illegal != nil {
					path = visualize(t, fmt.Sprintf("%s-seed-%d", m.name, seed), pr.Illegal)
				}
				t.Errorf("seed %d: oracle %s, porcupine %s (widenings %d; failures %v)\n%.500s\nvisualization: %s",
					seed, v, pr.Result, r.Stats.Widenings, r.Failures, msg, path)
			}
			keys := slices.Sorted(maps.Keys(tally))
			var b strings.Builder
			for _, k := range keys {
				fmt.Fprintf(&b, " %s=%d", k, tally[k])
			}
			t.Logf("seeds 1..%d:%s", *flagC5MutantSeeds, b.String())
		})
	}
}

// porcupineAgreesOnHist runs a hand-written oracle_test history through
// porcupine. A history with an undecodable GET body is skipped: porcupine
// drops such reads, the oracle rejects them outright. Unknown is logged,
// not failed.
func porcupineAgreesOnHist(t *testing.T, h *History, wantOK bool) {
	t.Helper()
	for i := range h.Ops {
		if h.Ops[i].Body != nil {
			if _, ok := ValueWriter(h.Ops[i].Body); !ok {
				return
			}
		}
	}
	pr := porcupineCheck(h, min(*flagPorcupineTimeout, time.Second))
	want := porcupine.Illegal
	if wantOK {
		want = porcupine.Ok
	}
	if pr.Result == porcupine.Unknown {
		// TestOracleWidensInsteadOfGivingUp: 2^14 possible states, then 13
		// concurrent COPYs. The oracle widens; porcupine has to enumerate.
		t.Logf("porcupine: Unknown after %v (%d ops)", pr.Total, pr.Ops)
		return
	}
	if pr.Result != want {
		t.Errorf("porcupine says %s, want %s (%d ops, %v)", pr.Result, want, pr.Ops, pr.Total)
	}
}
