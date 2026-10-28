package sim

import (
	"fmt"
	"strings"

	"manifester/internal/core"
)

// Assertion kinds as recorded in AssertionStat.Kind. Frozen spellings.
const (
	kindAlways     = "always"
	kindAlwaysOrUn = "always_or_unreachable"
	kindSometimes  = "sometimes"
	kindReachable  = "reachable"
	kindUnreach    = "unreachable"
)

// isSafety reports whether a kind is a violation-style assertion (Report.Always)
// as opposed to a coverage one (Report.Coverage).
func isSafety(kind string) bool {
	return kind == kindAlways || kind == kindAlwaysOrUn || kind == kindUnreach
}

type statKey struct {
	prop core.Prop
	kind string
}

// assertions implements core.Assertions for one seed. It records and continues
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

func (a *assertions) record(kind string, p core.Prop, ok bool, details []core.Detail) {
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

func formatViolation(kind string, details []core.Detail) string {
	var b strings.Builder
	b.WriteString(kind)
	b.WriteString(" violated")
	for _, d := range details {
		fmt.Fprintf(&b, " %s=%v", d.Key, d.Value)
	}
	return b.String()
}

func (a *assertions) Always(cond bool, p core.Prop, d ...core.Detail) {
	a.record(kindAlways, p, cond, d)
}
func (a *assertions) AlwaysOrUnreachable(cond bool, p core.Prop, d ...core.Detail) {
	a.record(kindAlwaysOrUn, p, cond, d)
}
func (a *assertions) Sometimes(cond bool, p core.Prop) { a.record(kindSometimes, p, cond, nil) }
func (a *assertions) Reachable(p core.Prop)            { a.record(kindReachable, p, true, nil) }
func (a *assertions) Unreachable(p core.Prop, d ...core.Detail) {
	a.record(kindUnreach, p, false, d)
}

var _ core.Assertions = (*assertions)(nil)

// buggify implements core.Buggify. Activation is decided once per site per run
// from Derive("buggify/"+site): independent of the run stream, so adding the
// first site shifts no other draw. Firing draws from the run stream, and only
// for active sites.
type buggify struct{ r *run }

const buggifyDefaultP = 0.25

func (b *buggify) active(site core.Site) bool {
	if v, ok := b.r.sites[site]; ok {
		return v
	}
	v := b.r.rng.Derive("buggify/" + string(site)).Chance(0.5)
	b.r.sites[site] = v
	return v
}

func (b *buggify) Maybe(site core.Site) bool { return b.MaybeP(site, buggifyDefaultP) }

func (b *buggify) MaybeP(site core.Site, p float64) bool {
	return b.active(site) && b.r.rng.Chance(p)
}

func (b *buggify) Fault(site core.Site, p float64) bool {
	return b.r.phase == PhaseChaos && b.MaybeP(site, p)
}

var _ core.Buggify = (*buggify)(nil)

// env is the core.Env every machine of a seed shares: the run stream, buggify
// and assertions.
type env struct {
	core.Rand
	core.Buggify
	core.Assertions
}

// history is the simple History: ops in invoke order with logical sequence
// numbers, stamped with simulation time.
type history struct {
	ops []HistOp
	idx map[core.ID]int
	seq uint64
	now func() Time
	bad func(msg string)
}

func newHistory(now func() Time, bad func(string)) *history {
	return &history{idx: map[core.ID]int{}, now: now, bad: bad}
}

func (h *history) Invoke(client core.NodeRef, id, value core.ID, op any) {
	if _, dup := h.idx[id]; dup {
		h.bad("history: duplicate invoke of an op id")
		return
	}
	h.seq++
	h.idx[id] = len(h.ops)
	h.ops = append(h.ops, HistOp{ID: id, Value: value, Client: client, Op: op, InvSeq: h.seq, Invoke: h.now(), Outcome: OpPending})
}

func (h *history) Return(id core.ID, result any, o OpOutcome) bool {
	i, ok := h.idx[id]
	if !ok || h.ops[i].Outcome != OpPending {
		h.bad("history: return of an unknown or already completed op id")
		return false
	}
	h.seq++
	op := &h.ops[i]
	op.RetSeq, op.Return, op.Result, op.Outcome = h.seq, h.now(), result, o
	return true
}

func (h *history) Ops() []HistOp { return h.ops }

var _ History = (*history)(nil)
