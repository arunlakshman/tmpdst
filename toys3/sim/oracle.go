package sim

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"manifester/toys3/control"
)

// Outcome is what the client learned about one request.
type Outcome uint8

const (
	Pending   Outcome = iota // no reply yet
	Definite                 // 200, 400 or 404: the op happened exactly as replied
	Failed                   // 500: the op may or may not have taken effect
	Abandoned                // the server crashed (or tripped) before replying
)

// HistOp is one client request as the client saw it.
type HistOp struct {
	Req    control.ClientReq
	Client int
	Op     control.Op
	Invoke control.Time // when the Request was delivered
	// Return is the reply time (Definite, Failed) or the crash time (Abandoned).
	Return  control.Time
	Outcome Outcome
	Status  control.Status
	Body    []byte // 200 GET only
	// InvSeq and RetSeq order Invoke, Return and crash events exactly
	// (several can share one virtual instant); the checker uses these.
	InvSeq, RetSeq uint64
}

// History is the complete client-visible history of a run.
type History struct {
	Ops     []HistOp       // in Invoke order
	Crashes []control.Time // crash (and Trip) instants, ascending
	// Completed lists indices into Ops in reply order (Definite and Failed).
	Completed []int
}

// Value is the model's view of one key: absent, or the bytes written by PUT
// Writer (COPY preserves Writer, since the bytes are identical).
type Value struct {
	Present bool
	Writer  control.ClientReq
}

// Oracle records the history and decides whether it is linearizable against a
// sequential model: a map from key to Value, starting empty. Nothing about
// the server's internals is consulted.
//
// The rules, which encode invariants (a) and (b) of DESIGN.md:
//
//   - A Definite op takes effect exactly once, at some instant in
//     [Invoke, Return], with the replied result (GET 200 returns the model's
//     value, GET/DELETE/COPY 404 means the key was absent, PUT/COPY/DELETE 200
//     changed the model).
//   - A Failed or Abandoned mutation takes effect at most once, at some
//     instant in [Invoke, Return] (Return = crash time when Abandoned), or
//     never. Failed and Abandoned reads impose nothing.
//   - Crashes erase nothing. So an acked mutation that disappears after a
//     restart, or an unacked one that a read observed and that later
//     vanishes, has no valid linearization and fails the check. (The latter
//     is why the server should Trip after an fsync error.)
//
// Checking is a Wing-Gong-Lowe search over each window between quiescent
// points (no op Pending), memoized on (set of linearized ops, model state).
// Windows are small because Workload.QuiesceEvery and crashes bound them.
type Oracle struct {
	hist History
	open map[control.ClientReq]int // Req → index into hist.Ops while Pending
	// checked is the index of the first op not yet in a checked window.
	checked int
	clock   uint64 // logical time for InvSeq / RetSeq
	// groups are the possible model states at the end of the last window,
	// factored into independent groups of keys: the possible states are
	// every combination of one state per group. A key in no group is
	// certainly absent.
	groups []group
	stats  OracleStats
	// Budget bounds the memo size of one window search (0: default).
	Budget int
}

// OracleStats describes the checker's work, for tuning window sizes.
type OracleStats struct {
	Windows      int
	MaxWindowOps int // ops in the largest window searched
	MaxVisited   int // memo entries in the largest search
	MaxStates    int // most possible states of one key group carried between windows
	Widenings    int // times the carried states were too many and keys were widened to "unknown"
}

// NewOracle returns an oracle with an empty history.
func NewOracle() *Oracle {
	return &Oracle{open: map[control.ClientReq]int{}}
}

func (o *Oracle) tick() uint64 {
	o.clock++
	return o.clock
}

// Invoke records a request delivered to the server at time at.
func (o *Oracle) Invoke(at control.Time, client int, req control.Request) {
	o.open[req.Client] = len(o.hist.Ops)
	o.hist.Ops = append(o.hist.Ops, HistOp{
		Req: req.Client, Client: client, Op: req.Op, Invoke: at, InvSeq: o.tick(),
	})
}

// Return records a reply. ok is false for an unknown or duplicate reply.
func (o *Oracle) Return(at control.Time, rep control.Reply) (ok bool) {
	i, ok := o.open[rep.Client]
	if !ok {
		return false
	}
	delete(o.open, rep.Client)
	op := &o.hist.Ops[i]
	op.Return, op.RetSeq, op.Status = at, o.tick(), rep.Status
	if rep.Status == control.StatusServerError {
		op.Outcome = Failed
	} else {
		op.Outcome = Definite
	}
	if _, isGet := op.Op.(control.Get); isGet && rep.Status == control.StatusOK {
		op.Body = slices.Clone(rep.Body)
		if op.Body == nil {
			op.Body = []byte{}
		}
	}
	o.hist.Completed = append(o.hist.Completed, i)
	return true
}

// Crash marks every Pending op Abandoned with Return = at.
func (o *Oracle) Crash(at control.Time) {
	seq := o.tick()
	for i := o.checked; i < len(o.hist.Ops); i++ {
		op := &o.hist.Ops[i]
		if op.Outcome == Pending {
			op.Outcome, op.Return, op.RetSeq = Abandoned, at, seq
			delete(o.open, op.Req)
		}
	}
	o.hist.Crashes = append(o.hist.Crashes, at)
}

// op returns the Pending op with id req, or nil.
func (o *Oracle) op(req control.ClientReq) *HistOp {
	i, ok := o.open[req]
	if !ok {
		return nil
	}
	return &o.hist.Ops[i]
}

// Quiescent reports whether no op is Pending.
func (o *Oracle) Quiescent() bool { return len(o.open) == 0 }

// Unchecked is the number of ops not yet in a checked window.
func (o *Oracle) Unchecked() int { return len(o.hist.Ops) - o.checked }

// Stats returns the checker's statistics.
func (o *Oracle) Stats() OracleStats { return o.stats }

// CheckQuiescent checks the window of ops completed since the last check. The
// simulator calls it at every quiescent point and at the end of the run; the
// model state at the end of a window must be carried as the set of possible
// states into the next one. It does nothing while an op is Pending.
func (o *Oracle) CheckQuiescent() []Failure {
	if !o.Quiescent() {
		return nil
	}
	return o.check()
}

// CheckFinal checks every unchecked op, treating a Pending mutation like an
// abandoned one that never returned (and ignoring Pending reads).
func (o *Oracle) CheckFinal() []Failure { return o.check() }

// History returns the recorded history (read-only).
func (o *Oracle) History() *History { return &o.hist }

// ---- the model ----

// kv is one present key in a model state.
type kv struct {
	key    string
	writer control.ClientReq
}

// modelState is the sequential model: the present keys, sorted. It is
// immutable once built (apply returns a new one).
type modelState []kv

func (st modelState) get(key string) (control.ClientReq, bool) {
	i, found := slices.BinarySearchFunc(st, key, func(e kv, k string) int { return strings.Compare(e.key, k) })
	if !found {
		return 0, false
	}
	return st[i].writer, true
}

func (st modelState) set(key string, w control.ClientReq, present bool) modelState {
	i, found := slices.BinarySearchFunc(st, key, func(e kv, k string) int { return strings.Compare(e.key, k) })
	out := make(modelState, 0, len(st)+1)
	out = append(out, st[:i]...)
	if present {
		out = append(out, kv{key, w})
	}
	if found {
		i++
	}
	return append(out, st[i:]...)
}

func (st modelState) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range st {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(e.key)
		b.WriteByte('=')
		b.WriteString(strconv.FormatUint(uint64(e.writer), 10))
	}
	b.WriteByte('}')
	return b.String()
}

// wop is one op in a window, prepared for the search.
type wop struct {
	h        *HistOp
	inv, ret uint64
	required bool              // Definite: must be linearized; else at most once
	reads    control.ClientReq // GET 200: the writer named by the body
}

// apply runs w on st. ok is false if w's replied result contradicts st.
func (w *wop) apply(st modelState) (modelState, bool) {
	status := w.h.Status
	switch op := w.h.Op.(type) {
	case control.Get:
		got, present := st.get(op.Key)
		if present && got == unknown {
			if status == control.StatusOK {
				return st.set(op.Key, w.reads, true), true // learned it
			}
			return st.set(op.Key, 0, false), true
		}
		if status == control.StatusOK {
			return st, present && got == w.reads
		}
		return st, !present // 404
	case control.Put:
		return st.set(op.Key, w.h.Req, true), true
	case control.Delete:
		got, present := st.get(op.Key)
		if !w.required || got == unknown {
			// Optional, or the key is unknown (it may have been either way).
			return st.set(op.Key, 0, false), true
		}
		if status == control.StatusOK {
			return st.set(op.Key, 0, false), present
		}
		return st, !present // 404
	case control.Copy:
		src, present := st.get(op.Src)
		if present && src == unknown {
			if !w.required || status == control.StatusOK {
				return st.set(op.Dst, unknown, true), true
			}
			return st.set(op.Src, 0, false), true // 404: src was absent
		}
		if !w.required {
			if !present {
				return st, true
			}
			return st.set(op.Dst, src, true), true
		}
		if status == control.StatusOK {
			return st.set(op.Dst, src, true), present
		}
		return st, !present // 404
	}
	return st, false
}

const never = ^uint64(0)

// unknown is a model value meaning "present or absent, with any value": the
// result of widening (see Oracle.widen). Any read of it is accepted, and the
// read's result then becomes the key's value. It only weakens the check, so
// it never causes a false failure.
const unknown = ^control.ClientReq(0)

// window builds the ops of the unchecked window. It returns a failure for a
// GET 200 whose bytes no PUT wrote (NoCorruptReads reports it too; the model
// cannot place it).
func (o *Oracle) window() ([]wop, []Failure) {
	var ws []wop
	for i := o.checked; i < len(o.hist.Ops); i++ {
		h := &o.hist.Ops[i]
		if h.Outcome == Definite && h.Status == control.StatusBadRequest {
			continue // rejected before touching anything
		}
		w := wop{h: h, inv: h.InvSeq, ret: h.RetSeq, required: h.Outcome == Definite}
		if h.Outcome == Pending {
			w.ret = never
		}
		if _, isGet := h.Op.(control.Get); isGet {
			if !w.required {
				continue // a failed or abandoned read constrains nothing
			}
			if h.Status == control.StatusOK {
				writer, ok := ValueWriter(h.Body)
				if !ok {
					return nil, []Failure{{Invariant: "linearizable", Msg: "GET " + describeOp(h) + " returned bytes no PUT wrote"}}
				}
				w.reads = writer
			}
		}
		ws = append(ws, w)
	}
	return ws, nil
}

// search is one Wing-Gong-Lowe search over a window, from several possible
// start states, collecting every possible end state.
type search struct {
	ws      []wop
	words   int
	visited map[string]struct{}
	budget  int
	over    bool
	finals  []modelState
	seen    map[string]struct{}
}

// group is a set of keys whose possible states are carried jointly
// (because a COPY tied them together, or a crash left correlated outcomes).
type group struct {
	keys   []string     // sorted
	states []modelState // restricted to keys; distinct
}

// opKeys returns the keys op touches, sorted.
func opKeys(op control.Op) []string {
	switch o := op.(type) {
	case control.Put:
		return []string{o.Key}
	case control.Get:
		return []string{o.Key}
	case control.Delete:
		return []string{o.Key}
	case control.Copy:
		if o.Src == o.Dst {
			return []string{o.Src}
		}
		ks := []string{o.Src, o.Dst}
		slices.Sort(ks)
		return ks
	}
	return nil
}

func intersects(a, b []string) bool {
	for _, k := range a {
		if _, ok := slices.BinarySearch(b, k); ok {
			return true
		}
	}
	return false
}

func union(a, b []string) []string {
	out := append(slices.Clone(a), b...)
	slices.Sort(out)
	return slices.Compact(out)
}

// maxStartStates bounds the possible start states of one component search.
const maxStartStates = 1 << 12

// check runs the WGL search over the unchecked window. By locality, ops on
// disjoint key sets are independent objects, so the window is split into
// connected components of keys (a COPY connects its src and dst; a carried
// group connects its keys) and each component is searched on its own.
func (o *Oracle) check() []Failure {
	ws, fails := o.window()
	o.checked = len(o.hist.Ops)
	if fails != nil || len(ws) == 0 {
		return fails
	}
	o.stats.Windows++
	o.stats.MaxWindowOps = max(o.stats.MaxWindowOps, len(ws))

	// Components: merge overlapping key sets until fixpoint.
	var comps [][]string
	add := func(ks []string) {
		for {
			merged := false
			for i := 0; i < len(comps); i++ {
				if intersects(ks, comps[i]) {
					ks = union(ks, comps[i])
					comps = append(comps[:i], comps[i+1:]...)
					merged = true
					break
				}
			}
			if !merged {
				comps = append(comps, ks)
				return
			}
		}
	}
	for i := range ws {
		add(opKeys(ws[i].h.Op))
	}
	pulled := make([]bool, len(o.groups))
	for changed := true; changed; {
		changed = false
		for i, g := range o.groups {
			if pulled[i] {
				continue
			}
			for _, c := range comps {
				if intersects(g.keys, c) {
					add(g.keys)
					pulled[i], changed = true, true
					break
				}
			}
		}
	}
	slices.SortFunc(comps, func(a, b []string) int { return strings.Compare(a[0], b[0]) })

	for _, comp := range comps {
		// Start states: the product of the carried groups inside comp.
		start := []modelState{nil}
		var rest []group
		for _, g := range o.groups {
			if !intersects(g.keys, comp) {
				rest = append(rest, g)
				continue
			}
			var next []modelState
			for _, a := range start {
				for _, b := range g.states {
					st := append(slices.Clone(a), b...)
					slices.SortFunc(st, func(x, y kv) int { return strings.Compare(x.key, y.key) })
					next = append(next, st)
				}
			}
			start = next
			if len(start) > maxStartStates {
				start = widenStates(comp, start)
				o.stats.Widenings++
			}
		}
		var cws []wop
		for i := range ws {
			if intersects(opKeys(ws[i].h.Op), comp) {
				cws = append(cws, ws[i])
			}
		}
		finals, f := o.search(start, cws)
		if f != nil {
			return f
		}
		if len(finals) > maxStartStates {
			finals = widenStates(comp, finals)
			o.stats.Widenings++
		}
		o.groups = append(rest, factor(comp, finals)...)
		slices.SortFunc(o.groups, func(a, b group) int { return strings.Compare(a.keys[0], b.keys[0]) })
		o.stats.MaxStates = max(o.stats.MaxStates, len(finals))
	}
	return nil
}

// widenStates over-approximates a too-large set of states over keys by one
// state: each key keeps its value if all states agree on it, else becomes
// unknown. Correlations between keys are forgotten.
func widenStates(keys []string, states []modelState) []modelState {
	var out modelState
	for _, k := range keys {
		w0, p0 := states[0].get(k)
		same := true
		for _, st := range states[1:] {
			if w, p := st.get(k); w != w0 || p != p0 {
				same = false
				break
			}
		}
		switch {
		case !same:
			out = append(out, kv{k, unknown})
		case p0:
			out = append(out, kv{k, w0})
		}
	}
	return []modelState{out}
}

// factor turns the possible end states of one component into groups: one
// group per key when the set is exactly the product of its per-key values
// (independent uncertainty, the common case), else one joint group. Keys
// that are certainly absent need no group.
func factor(keys []string, finals []modelState) []group {
	vals := make([][]control.ClientReq, len(keys)) // 0 = absent
	prod := 1
	for i, k := range keys {
		for _, st := range finals {
			w, _ := st.get(k)
			vals[i] = append(vals[i], w)
		}
		slices.Sort(vals[i])
		vals[i] = slices.Compact(vals[i])
		prod *= len(vals[i])
		if prod > len(finals) {
			break
		}
	}
	if prod != len(finals) {
		return []group{{keys: keys, states: finals}}
	}
	var gs []group
	for i, k := range keys {
		if len(vals[i]) == 1 && vals[i][0] == 0 {
			continue
		}
		g := group{keys: []string{k}}
		for _, w := range vals[i] {
			if w == 0 {
				g.states = append(g.states, nil)
			} else {
				g.states = append(g.states, modelState{{k, w}})
			}
		}
		gs = append(gs, g)
	}
	return gs
}

// search runs one WGL search from every start state and returns the
// distinct possible end states.
func (o *Oracle) search(start []modelState, ws []wop) ([]modelState, []Failure) {
	budget := o.Budget
	if budget <= 0 {
		budget = 1 << 20
	}
	s := &search{
		ws: ws, words: (len(ws) + 63) / 64, budget: budget,
		visited: map[string]struct{}{}, seen: map[string]struct{}{},
	}
	for _, st := range start {
		s.dfs(make([]uint64, s.words), 0, st)
	}
	o.stats.MaxVisited = max(o.stats.MaxVisited, len(s.visited))
	if s.over {
		return nil, []Failure{{Invariant: "linearizable", Msg: fmt.Sprintf(
			"checker budget exceeded (%d memo entries) on %d ops: shrink QuiesceEvery", budget, len(ws))}}
	}
	if len(s.finals) == 0 {
		return nil, []Failure{{Invariant: "linearizable", Msg: describeWindow(start, ws)}}
	}
	return s.finals, nil
}

// dfs explores linearizations extending set (the linearized ops), whose last
// linearization point is at logical time at, with model state st.
func (s *search) dfs(set []uint64, at uint64, st modelState) {
	if s.over {
		return
	}
	key := memoKey(set, st)
	if _, done := s.visited[key]; done {
		return
	}
	if len(s.visited) >= s.budget {
		s.over = true
		return
	}
	s.visited[key] = struct{}{}

	// The earliest return among unlinearized required ops (and the second
	// earliest, for when that op itself is the candidate).
	min1, min2, minIdx := never, never, -1
	for i := range s.ws {
		if set[i/64]&(1<<(i%64)) != 0 || !s.ws[i].required {
			continue
		}
		r := s.ws[i].ret
		if r < min1 {
			min1, min2, minIdx = r, min1, i
		} else if r < min2 {
			min2 = r
		}
	}
	if minIdx < 0 {
		sk := st.String()
		if _, dup := s.seen[sk]; !dup {
			s.seen[sk] = struct{}{}
			s.finals = append(s.finals, st)
		}
	}
	for i := range s.ws {
		if set[i/64]&(1<<(i%64)) != 0 {
			continue
		}
		w := &s.ws[i]
		p := max(at, w.inv)
		if p > w.ret {
			continue // too late for this op (an optional one is now "never")
		}
		bound := min1
		if i == minIdx {
			bound = min2
		}
		if p > bound {
			continue // a required op would have to return before this one starts
		}
		next, ok := w.apply(st)
		if !ok {
			continue
		}
		set2 := slices.Clone(set)
		set2[i/64] |= 1 << (i % 64)
		s.dfs(set2, p, next)
	}
}

func memoKey(set []uint64, st modelState) string {
	var b strings.Builder
	for _, w := range set {
		b.WriteString(strconv.FormatUint(w, 36))
		b.WriteByte('.')
	}
	b.WriteString(st.String())
	return b.String()
}

func describeWindow(start []modelState, ws []wop) string {
	var b strings.Builder
	fmt.Fprintf(&b, "no linearization of %d ops from any of %d possible start states", len(ws), len(start))
	for i, st := range start {
		if i == 8 {
			fmt.Fprintf(&b, " ...")
			break
		}
		fmt.Fprintf(&b, " %s", st)
	}
	b.WriteString("; window:")
	for i := range ws {
		if i == 60 {
			b.WriteString("\n  ...")
			break
		}
		b.WriteString("\n  ")
		b.WriteString(describeOp(ws[i].h))
	}
	return b.String()
}

func (o Outcome) String() string {
	switch o {
	case Pending:
		return "pending"
	case Definite:
		return "definite"
	case Failed:
		return "failed"
	case Abandoned:
		return "abandoned"
	}
	return "outcome(?)"
}

// describeOp renders one history op for failure messages and logs.
func describeOp(h *HistOp) string {
	res := h.Outcome.String()
	if h.Outcome == Definite || h.Outcome == Failed {
		res = strconv.Itoa(int(h.Status))
	}
	if w, ok := ValueWriter(h.Body); ok {
		res += fmt.Sprintf(" value-of-%d", w)
	} else if h.Body != nil {
		res += fmt.Sprintf(" garbage(%d bytes)", len(h.Body))
	}
	return fmt.Sprintf("req %d client %d %s [%d..%d @%v..%v] -> %s",
		h.Req, h.Client, DescribeOp(h.Op), h.InvSeq, h.RetSeq, h.Invoke, h.Return, res)
}
