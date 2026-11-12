package simulator

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
)

// Assertion kinds as recorded in AssertionStat.Kind. Frozen spellings.
const (
	kindMustAlwaysHold    = "always"
	kindMustHoldIfReached = "always_or_unreachable"
	kindMustHappenOnce    = "sometimes"
	kindMustBeReached     = "reachable"
	kindMustNotBeReached  = "unreachable"
)

// isSafety reports whether a kind is a violation-style assertion (Report.Always)
// as opposed to a coverage one (Report.Coverage).
func isSafety(kind string) bool {
	return kind == kindMustAlwaysHold || kind == kindMustHoldIfReached || kind == kindMustNotBeReached
}

type statKey struct {
	prop string
	kind string
}

// assertions implements machine.Assertions for one seed. It records and continues
// (lesson 11): a violated Always becomes a FailViolation (the first per
// property per seed; later ones only bump Checks) and the run goes on.
//
// Per-seed AssertionStat semantics: Checks counts evaluations, Passes the true
// ones. Seeds is {seed} when the stat is "interesting" (a safety property that
// was violated, or a coverage property that was satisfied) and FirstStep is the
// step of the first such evaluation.
type assertions struct {
	r       *run
	stats   []AssertionStat
	idx     map[statKey]int
	flagged map[statKey]bool
}

func newAssertions(r *run) *assertions {
	return &assertions{r: r, idx: map[statKey]int{}, flagged: map[statKey]bool{}}
}

func (a *assertions) record(kind, p string, ok bool, details []assert.Pair) {
	k := statKey{p, kind}
	i, found := a.idx[k]
	if !found {
		i = len(a.stats)
		a.idx[k] = i
		a.stats = append(a.stats, AssertionStat{Prop: p, Kind: kind})
	}
	s := &a.stats[i]
	s.Checks++
	if ok {
		s.Passes++
	}
	interesting := ok != isSafety(kind) // coverage satisfied, or safety violated
	if interesting && len(s.Seeds) == 0 {
		s.Seeds = []uint64{a.r.seed}
		s.FirstSeed = a.r.seed
		s.FirstStep = a.r.steps
	}
	if isSafety(kind) && !ok && !a.flagged[k] {
		a.flagged[k] = true
		a.r.fail(FailViolation, p, formatViolation(kind, details))
	}
}

func formatViolation(kind string, details []assert.Pair) string {
	var b strings.Builder
	b.WriteString(kind)
	b.WriteString(" violated")
	for _, d := range details {
		fmt.Fprintf(&b, " %s=%v", d.Key, d.Value)
	}
	return b.String()
}

func (a *assertions) Always(cond bool, p assert.Always, d ...assert.Pair) {
	a.record(kindMustAlwaysHold, p.Name(), cond, d)
}
func (a *assertions) AlwaysOrUnreachable(cond bool, p assert.AlwaysOrUnreachable, d ...assert.Pair) {
	a.record(kindMustHoldIfReached, p.Name(), cond, d)
}
func (a *assertions) Sometimes(cond bool, p assert.Sometimes) {
	a.record(kindMustHappenOnce, p.Name(), cond, nil)
}
func (a *assertions) Reachable(p assert.Reachable) { a.record(kindMustBeReached, p.Name(), true, nil) }
func (a *assertions) Unreachable(p assert.Unreachable, d ...assert.Pair) {
	a.record(kindMustNotBeReached, p.Name(), false, d)
}

var _ machine.Assertions = (*assertions)(nil)

// faultInjector implements machine.Faults. Activation is decided once per site per run
// from Derive("buggify/"+site): independent of the run stream, so adding the
// first site shifts no other draw. Firing draws from the run stream, and only
// for active sites.
type faultInjector struct{ r *run }

const faultInjectDefaultChance = 0.25

func (b *faultInjector) active(site fault.NamedSite) bool {
	if v, ok := b.r.sites[site]; ok {
		return v
	}
	v := b.r.rng.Derive("buggify/" + site.Name()).Chance(0.5)
	b.r.sites[site] = v
	return v
}

func (b *faultInjector) Maybe(site fault.NamedSite) bool {
	return b.MaybeP(site, faultInjectDefaultChance)
}

func (b *faultInjector) MaybeP(site fault.NamedSite, p float64) bool {
	return b.active(site) && b.r.rng.Chance(p)
}

func (b *faultInjector) Fault(site fault.NamedSite, p float64) bool {
	return b.r.phase == PhaseFaults && b.MaybeP(site, p)
}

var _ machine.Faults = (*faultInjector)(nil)

// env is the core.Environment every machine of a seed shares: the run stream, buggify
// and assertions.
type env struct {
	core.Rand
	machine.Faults
	machine.Assertions
}

// history is the simple History: ops in invoke order with logical sequence
// numbers, stamped with simulation time.
type history struct {
	ops []HistoryOperation
	idx map[machine.ID]int
	seq uint64
	now func() Time
	bad func(msg string)
}

func newHistory(now func() Time, bad func(string)) *history {
	return &history{idx: map[machine.ID]int{}, now: now, bad: bad}
}

func (h *history) Invoke(client machine.NodeInstance, id, value machine.ID, operation any) {
	if _, dup := h.idx[id]; dup {
		h.bad("history: duplicate invoke of an op id")
		return
	}
	h.seq++
	h.idx[id] = len(h.ops)
	h.ops = append(h.ops, HistoryOperation{ID: id, Value: value, Client: client, Op: operation, InvSeq: h.seq, Invoke: h.now(), Outcome: OpPending})
}

func (h *history) Return(id machine.ID, result any, o OpOutcome) bool {
	i, ok := h.idx[id]
	if !ok || h.ops[i].Outcome != OpPending {
		h.bad("history: return of an unknown or already completed op id")
		return false
	}
	h.seq++
	operation := &h.ops[i]
	operation.RetSeq, operation.Return, operation.Result, operation.Outcome = h.seq, h.now(), result, o
	return true
}

func (h *history) Ops() []HistoryOperation { return h.ops }

var _ History = (*history)(nil)
