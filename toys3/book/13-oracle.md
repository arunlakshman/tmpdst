# Chapter 13: The Oracle: Linearizability

> *"A concurrent object is correct if every history it produces could have been produced by some sequential execution that respects real time."* (Herlihy and Wing, paraphrased)

The invariants of Chapter 14 that peek at the fake disk (`AckedIsDurable`, `ObjNeverTorn`) are powerful but *white-box*: they know where the server keeps its files and what a correct file looks like. This chapter is about the one check that knows nothing about the server at all. The **oracle** (`toys3/sim/oracle.go`) sees only what clients saw: which requests were sent, when, and what came back. From that alone it decides whether the server behaved like a correct key-value store that never loses acknowledged data.

That decision is a linearizability check, and it is the hardest piece of code in the repository. We will build it up from first principles, walk through the implementation line by line, work every test in `oracle_test.go` by hand, and then dissect three real failures from the mutation suite.

---

## 13.1 What "correct" means for a concurrent store

### Histories

A client's interaction with the server is an **operation** with two events: an **invocation** (the request is sent) and a **response** (the reply arrives). A **history** is the sequence of all invocation and response events of all clients, in real-time order.

```
 real time ──────────────────────────────────────────────────────►
 client A   [ PUT k=v1 ─────────────── 200 ]
 client B          [ GET k ─── 200 v1 ]
 client C                                  [ GET k ── 404 ]
```

If all operations were instantaneous and one client ran at a time, "correct" would be easy: replay them in order against a sequential map and compare results. Concurrency breaks that: A's PUT and B's GET overlap, so it is not obvious whether B "should" have seen `v1`.

### Linearizability

Herlihy and Wing (TOPLAS 1990) gave the standard answer. A history is **linearizable** if we can pick, for every operation, a single instant (its **linearization point**) somewhere between its invocation and its response, such that:

1. executing the operations one at a time, in the order of their linearization points, against the **sequential specification** gives exactly the results the clients observed, and
2. (implied by choosing points inside the intervals) if operation X returned before operation Y was invoked, X's point precedes Y's point. This is **real-time order**.

In the history above, PUT's point can be placed before B's GET's point (both are inside their overlapping intervals), so B seeing `v1` is fine. C's GET was invoked after PUT returned, so C's point is after PUT's, and C must see `v1`. C saw 404. **Not linearizable.** (This is exactly `TestOracleRejectsReadNotYetPossible`'s shape, §13.7.)

### The sequential specification of toys3

The model is a map from key to value, starting empty. Operations:

| Op | Sequential effect | Result |
|---|---|---|
| `PUT k v` | `m[k] = v` | 200 |
| `GET k` | none | 200 with `m[k]`, or 404 if absent |
| `DELETE k` | `delete(m, k)` if present | 200 if it was present, else 404 |
| `COPY s → d` | `m[d] = m[s]` if `s` present | 200 if `s` present, else 404 (and `d` untouched) |

Thanks to self-describing values (Chapter 12), "value" never means bytes. A value is identified by the **writer**: the request id of the PUT whose bytes they are. COPY moves the writer id from `s` to `d` unchanged. So the model state is a map from key to `core.ClientReq`.

### Locality

Herlihy and Wing proved that linearizability is **local** (compositional): a history over many objects is linearizable iff its restriction to each object is linearizable. Each key is an object... *except* that COPY touches two keys atomically, so a COPY ties its source and destination into one object for the duration. toys3's oracle exploits locality aggressively (§13.5), splitting each window into connected components of keys.

## 13.2 Crashes and errors: what does a reply promise?

Classical linearizability assumes every operation eventually responds. A storage server under DST has three other possibilities, and each must be given a precise meaning. This is the most important design decision in the oracle, so here it is in full:

```go
// toys3/sim/oracle.go:12-20
type Outcome uint8

const (
	Pending   Outcome = iota // no reply yet
	Definite                 // 200, 400 or 404: the op happened exactly as replied
	Failed                   // 500: the op may or may not have taken effect
	Abandoned                // the server crashed (or halted) before replying
)
```

and the rules, from the `Oracle` doc comment:

```go
// toys3/sim/oracle.go:57-69
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
//     is why the server should Halt after an fsync error.)
```

Three things in these rules deserve emphasis.

**1. "Crashes erase nothing."** The model has no crash operation. A crash is just an instant at which some operations stop (become Abandoned). The model state after the crash is whatever it was before. So if PUT k=v1 returned 200, and after a crash a GET returns 404, there is no sequential explanation: the history is rejected. This one sentence is the black-box statement of durability: *acknowledged means durable* (invariant (b) in `DESIGN.md:144`).

**2. Indeterminate mutations are optional, but bounded.** A Failed or Abandoned PUT may or may not have happened, but if it happened, it happened *inside its interval*. For an Abandoned op, the interval ends at the crash. It cannot take effect after the crash, because the process that would have done it is dead. For a Failed op (500), the interval ends at the reply.

That second bound is a strong assumption, and it is not the one Jepsen makes. In Jepsen, an indeterminate (`:info`) operation is treated as possibly taking effect at *any* time after its invocation, forever, because a timed-out network request may still be sitting in a queue somewhere. toys3 can afford the stronger assumption because of its error policy (Chapter 7): every error after which a mutation may have touched `obj/` is a 500 **plus Halt**, and the simulator processes the Halt as a crash *in the same step* as the 500. So the effect, if any, is resolved at the instant of the reply. Errors before anything can touch `obj/` are a plain 500 with nothing changed. In both cases, "at most once, within [Invoke, Return]" is exactly right for a correct server.

And that is what lets the oracle catch mutant d2 (§13.9): a server that replies 500 but *keeps running* lets the half-applied change stay visible after the reply and then be undone by a later crash. In model terms, the effect would have to happen inside the interval (because a read saw it) and then un-happen later (because a later read did not). Neither "applied" nor "never" explains the history.

**3. Failed and Abandoned reads impose nothing.** A GET that got 500 or no reply tells us nothing about the state, so it is simply dropped from the history the checker sees.

## 13.3 Recording the history

The oracle records every operation as a `HistOp`:

```go
// toys3/sim/oracle.go:22-44
type HistOp struct {
	Req    core.ClientReq
	Client int
	Op     core.Op
	Invoke core.Time // when the Request was delivered
	// Return is the reply time (Definite, Failed) or the crash time (Abandoned).
	Return  core.Time
	Outcome Outcome
	Status  core.Status
	Body    []byte // 200 GET only
	// InvSeq and RetSeq order Invoke, Return and crash events exactly
	// (several can share one virtual instant); the checker uses these.
	InvSeq, RetSeq uint64
}

type History struct {
	Ops     []HistOp    // in Invoke order
	Crashes []core.Time // crash (and Halt) instants, ascending
	// Completed lists indices into Ops in reply order (Definite and Failed).
	Completed []int
}
```

### Logical time, not virtual time

Look at `InvSeq` and `RetSeq`. The simulator has a virtual clock, but many events can happen at the same virtual instant (a reply, a client wake with zero think time, a request delivery, a crash). If the checker used `Invoke` and `Return` times, two operations that share an instant would be ambiguous: did X return before Y was invoked? So the oracle keeps its own logical clock, ticked on every invocation, return and crash:

```go
// toys3/sim/oracle.go:104-115
func (o *Oracle) tick() uint64 {
	o.clock++
	return o.clock
}

// Invoke records a request delivered to the server at time at.
func (o *Oracle) Invoke(at core.Time, client int, req core.Request) {
	o.open[req.Client] = len(o.hist.Ops)
	o.hist.Ops = append(o.hist.Ops, HistOp{
		Req: req.Client, Client: client, Op: req.Op, Invoke: at, InvSeq: o.tick(),
	})
}
```

The sequence numbers order events exactly as the single-threaded simulator processed them, which is the real-time order of the simulated world. The virtual times are kept only for humans (they appear in failure messages as `@82642556..84991187`).

A small documentation inaccuracy worth knowing: the comments say "when the Request was delivered", but `Simulator.wake` calls `oracle.Invoke` when the client *issues* the request, and only then schedules its delivery (`simulator.go:248-252`). The delivery may happen later in the same instant, or never (if a crash intervenes). Recording the invocation early only *widens* the operation's interval, which can only make more histories acceptable, never fewer. It is safe, but the comment is slightly off.

### Returns and crashes

```go
// toys3/sim/oracle.go:117-152
func (o *Oracle) Return(at core.Time, rep core.Reply) (ok bool) {
	i, ok := o.open[rep.Client]
	if !ok {
		return false
	}
	delete(o.open, rep.Client)
	op := &o.hist.Ops[i]
	op.Return, op.RetSeq, op.Status = at, o.tick(), rep.Status
	if rep.Status == core.StatusServerError {
		op.Outcome = Failed
	} else {
		op.Outcome = Definite
	}
	if _, isGet := op.Op.(core.Get); isGet && rep.Status == core.StatusOK {
		op.Body = slices.Clone(rep.Body)
		if op.Body == nil {
			op.Body = []byte{}
		}
	}
	o.hist.Completed = append(o.hist.Completed, i)
	return true
}

func (o *Oracle) Crash(at core.Time) {
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
```

Details:

- `open` maps a pending request id to its index. A reply for an id not in `open` is a duplicate or invented reply: `Return` returns false and the simulator reports a `protocol` failure.
- Only a 500 is `Failed`. 200, 400 and 404 are all `Definite`.
- The body is cloned (the server's reply slice must not be retained; "byte slices are immutable once sent" is a convention, and cloning makes the oracle independent of it). A nil body on a 200 GET becomes an empty slice, so "200 with empty body" and "no body" are distinguishable later.
- A crash ticks the clock **once** and gives every abandoned operation the same `RetSeq`. All of them "returned" at the crash instant.
- The loop starts at `o.checked`: operations in already-checked windows cannot be pending (windows are checked only at quiescence).

## 13.4 The model and how operations apply

The model state is a sorted slice:

```go
// toys3/sim/oracle.go:192-221
type kv struct {
	key    string
	writer core.ClientReq
}

type modelState []kv

func (st modelState) get(key string) (core.ClientReq, bool) { ...binary search... }

func (st modelState) set(key string, w core.ClientReq, present bool) modelState {
	i, found := slices.BinarySearchFunc(st, key, ...)
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
```

Two design choices:

- **Immutable**: `set` always builds a new slice. The search explores many branches from the same state; sharing is safe because nobody mutates.
- **Sorted slice, not a map**: deterministic iteration (detcheck bans ranging over maps in this package, Chapter 8) and a canonical `String()` (`{k0=10 k1=26}`) that doubles as a memo key.

### The special value `unknown`

```go
// toys3/sim/oracle.go:296-302
const never = ^uint64(0)

// unknown is a model value meaning "present or absent, with any value": the
// result of widening (see Oracle.widen). Any read of it is accepted, and the
// read's result then becomes the key's value. It only weakens the check, so
// it never causes a false failure.
const unknown = ^core.ClientReq(0)
```

`unknown` is the oracle's escape valve when it is tracking too many possibilities (§13.6). A key whose value is `unknown` is "present" in the slice but stands for "anything, including absent". (The comment mentions `Oracle.widen`; the function is actually named `widenStates`.)

### apply, case by case

Each operation in a window is prepared as a `wop`:

```go
// toys3/sim/oracle.go:238-244
type wop struct {
	h        *HistOp
	inv, ret uint64
	required bool           // Definite: must be linearized; else at most once
	reads    core.ClientReq // GET 200: the writer named by the body
}
```

and `apply` runs one against a state, returning the new state and whether the operation's observed result is consistent with it:

```go
// toys3/sim/oracle.go:246-294
func (w *wop) apply(st modelState) (modelState, bool) {
	status := w.h.Status
	switch op := w.h.Op.(type) {
	case core.Get:
		got, present := st.get(op.Key)
		if present && got == unknown {
			if status == core.StatusOK {
				return st.set(op.Key, w.reads, true), true // learned it
			}
			return st.set(op.Key, 0, false), true
		}
		if status == core.StatusOK {
			return st, present && got == w.reads
		}
		return st, !present // 404
	case core.Put:
		return st.set(op.Key, w.h.Req, true), true
	case core.Delete:
		got, present := st.get(op.Key)
		if !w.required || got == unknown {
			// Optional, or the key is unknown (it may have been either way).
			return st.set(op.Key, 0, false), true
		}
		if status == core.StatusOK {
			return st.set(op.Key, 0, false), present
		}
		return st, !present // 404
	case core.Copy:
		src, present := st.get(op.Src)
		if present && src == unknown {
			if !w.required || status == core.StatusOK {
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
		if status == core.StatusOK {
			return st.set(op.Dst, src, true), present
		}
		return st, !present // 404
	}
	return st, false
}
```

As a table (here "opt" means Failed or Abandoned, i.e. `!required`, and only mutations can be optional because failed reads are dropped earlier):

| Op | Known state | Result | New state | Consistent iff |
|---|---|---|---|---|
| GET | k = w | 200 reads r | same | w = r |
| GET | k absent | 200 | same | never |
| GET | k = w | 404 | same | never |
| GET | k absent | 404 | same | always |
| GET | k = unknown | 200 reads r | k = r ("learned") | always |
| GET | k = unknown | 404 | k absent ("learned") | always |
| PUT | any | 200 or opt | k = this PUT's req | always |
| DELETE | any | opt | k absent | always |
| DELETE | k = unknown | any | k absent | always |
| DELETE | k present | 200 | k absent | always |
| DELETE | k absent | 200 | k absent | never |
| DELETE | k present | 404 | same | never |
| DELETE | k absent | 404 | same | always |
| COPY | s = unknown | 200 or opt | d = unknown | always |
| COPY | s = unknown | 404 | s absent | always |
| COPY | s = w | opt | d = w | always |
| COPY | s absent | opt | same | always |
| COPY | s = w | 200 | d = w | always |
| COPY | s absent | 200 | (d = 0) | never |
| COPY | s present | 404 | same | never |
| COPY | s absent | 404 | same | always |

A few non-obvious points:

- **An optional op is either applied here or skipped entirely.** The search (next section) tries both. `apply` is only called when the search decides to linearize the op.
- **An optional DELETE always "succeeds" when applied.** A Failed DELETE got 500, so we do not know whether the key was present. If it is applied, the key ends absent.
- **A PUT never contradicts anything.** A 200 PUT and an optional PUT have the same effect when applied; the difference is only that the 200 PUT *must* be applied.
- **A 200 COPY from an absent source is inconsistent**: `st.set(op.Dst, src, true)` would set `d` to writer 0, but the `present` flag returned is false, so the branch is pruned anyway.

## 13.5 Building and splitting a window

### window(): which operations count

```go
// toys3/sim/oracle.go:304-333
func (o *Oracle) window() ([]wop, []Failure) {
	var ws []wop
	for i := o.checked; i < len(o.hist.Ops); i++ {
		h := &o.hist.Ops[i]
		if h.Outcome == Definite && h.Status == core.StatusBadRequest {
			continue // rejected before touching anything
		}
		w := wop{h: h, inv: h.InvSeq, ret: h.RetSeq, required: h.Outcome == Definite}
		if h.Outcome == Pending {
			w.ret = never
		}
		if _, isGet := h.Op.(core.Get); isGet {
			if !w.required {
				continue // a failed or abandoned read constrains nothing
			}
			if h.Status == core.StatusOK {
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
```

The window is every operation invoked since the last check. Filtering:

- **400s are dropped.** They were rejected before touching any state (the server validates before queuing, `server.go:259-284`).
- **Pending ops** can only appear here through `CheckFinal` (end of run). They get `ret = never`: they may take effect at any time after invocation. Pending GETs are dropped by the next rule.
- **Failed and Abandoned (and Pending) GETs are dropped.**
- **A 200 GET whose body is not a valid value** (per `ValueWriter`) fails immediately: the model cannot place bytes that no PUT wrote. This duplicates `NoCorruptReads` (Chapter 14), which is why mutant e's failure reports both invariants at once (§13.9).

### check(): components, carried groups, and searching

The heart of the oracle is `check`, called at quiescent points (`CheckQuiescent`) and at the end (`CheckFinal`).

```go
// toys3/sim/oracle.go:392-404
func (o *Oracle) check() []Failure {
	ws, fails := o.window()
	o.checked = len(o.hist.Ops)
	if fails != nil || len(ws) == 0 {
		return fails
	}
	o.stats.Windows++
	o.stats.MaxWindowOps = max(o.stats.MaxWindowOps, len(ws))
```

Note that `o.checked` advances *before* searching. Whatever the outcome, these operations are never revisited.

#### Step 1: components by locality

Two operations interact only if they touch a common key. COPY touches two keys. So the operations of a window form a graph, and its connected components are independent sub-histories. Horn and Kroening (FORTE 2015) called this property **P-compositionality**: if a specification can be partitioned so that each part is checked independently, the expensive search runs on small pieces. Porcupine (a Go linearizability checker by Anish Athalye) exposes it as a user-supplied partition function. toys3 computes the partition itself:

```go
// toys3/sim/oracle.go:405-426
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
```

`add` is a simple union of key sets: merge the new key set with any component it intersects, repeat until no intersection remains. O(n²), fine for windows of a few dozen operations.

#### Step 2: pull in carried groups

The state at the start of this window is not a single map. It is a *set* of possible maps, because previous windows contained optional operations (and crashes) whose effects are uncertain. That set is stored compactly as **groups**:

```go
// toys3/sim/oracle.go:347-352
// group is a set of keys whose possible states are carried jointly
// (because a COPY tied them together, or a crash left correlated outcomes).
type group struct {
	keys   []string     // sorted
	states []modelState // restricted to keys; distinct
}
```

The possible full states are the Cartesian product of one state per group. A key in no group is certainly absent. So if `o.groups` is `[{keys:[a], states:[{}, {a=1}]}, {keys:[b], states:[{}, {b=2}]}]`, the possible states are the four combinations.

A group that shares a key with a component must be searched with it (the component needs to know that key's possible values), and a group can *connect* two components (if it ties `a` and `b` together from a previous COPY, then this window's ops on `a` and on `b` are no longer independent). So:

```go
// toys3/sim/oracle.go:427-443
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
```

Fixpoint: keep merging groups' key sets into components until nothing changes. Then sort components by first key for deterministic processing order.

#### Step 3: per component, build start states and search

```go
// toys3/sim/oracle.go:445-486
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
					slices.SortFunc(st, ...)
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
		slices.SortFunc(o.groups, ...)
		o.stats.MaxStates = max(o.stats.MaxStates, len(finals))
	}
	return nil
}
```

For each component:

1. **Start states** are the product of the groups inside it, starting from `[nil]` (the empty map, meaning every key of the component is absent unless a group says otherwise). If the product exceeds `maxStartStates = 1 << 12 = 4096` (`oracle.go:390`), widen.
2. **Select the operations** touching the component.
3. **Search** from every start state; collect every possible **end state** (`finals`). Zero end states means no linearization exists: return the failure.
4. Widen the end states if there are too many.
5. **Factor** the end states back into groups, replacing the groups that were consumed (the others are kept in `rest`).

Note that the first failing component returns immediately; later components are not checked. That is fine because the simulator stops the run at the first failure (`simulator.go:364-374`).

## 13.6 Keeping uncertainty small: factor and widen

### factor: undo the product when possible

After a crash that abandons PUTs on `a` and `b`, the end states are `{}`, `{a=1}`, `{b=2}`, `{a=1 b=2}`. Carrying them as one joint group of 4 states would be wasteful: the uncertainty about `a` is independent of the uncertainty about `b`. `factor` detects that:

```go
// toys3/sim/oracle.go:513-551
func factor(keys []string, finals []modelState) []group {
	vals := make([][]core.ClientReq, len(keys)) // 0 = absent
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
```

The test is elegant: compute, for each key, the set of distinct values it takes across the end states. If the product of those set sizes equals the number of (distinct) end states, then the end states *are* the full Cartesian product (a set of N distinct tuples drawn from a product space of size N is the whole space). Then each key gets its own group. If not, the keys are correlated (for example, a COPY made `b` equal to `a` in every end state) and the component stays one joint group.

Keys that are absent in every end state need no group at all ("a key in no group is certainly absent").

### widen: give up precision, never soundness

If a component would start from more than 4096 states (or end with more than 4096), the oracle **widens**:

```go
// toys3/sim/oracle.go:489-511
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
```

One state replaces many: each key that all states agree on keeps its value; each key they disagree on becomes `unknown`. Correlations are forgotten. Because `unknown` accepts any read (and then *learns* the value read), widening can only make the checker accept more histories. It can never cause a false failure. It can cause a missed bug, for a while, on the widened keys; a fresh PUT re-establishes precision. The `widened=` statistic counts how often this happened. In every stats line in this book it is 0: the default campaign never needs it. `TestOracleWidensInsteadOfGivingUp` (§13.7) forces it.

### The memo budget

The search itself has a separate safety limit, `Budget` (default `1 << 20` memo entries, `oracle.go:556-559`). Exceeding it is reported as a `linearizable` failure with the message `checker budget exceeded (N memo entries) on M ops: shrink QuiesceEvery`. Unlike widening, this is a *tooling* failure that could be a false alarm: it says "I could not decide", not "this history is wrong". The message is distinct, so you can tell them apart; in the default configuration it does not occur.

## 13.7 The search: Wing–Gong–Lowe, line by line

Wing and Gong (1993) gave the basic algorithm for testing linearizability: depth-first search over the choice of which pending operation to linearize next, backtracking when the sequential specification disagrees with an observed result. Lowe (2017) added the crucial optimization of **memoizing** configurations already explored (the set of linearized operations plus the model state), which turns many exponential cases into polynomial ones in practice. Knossos (Kingsbury's Clojure checker used by Jepsen) and Porcupine both implement variants. toys3's version adds optional operations and multiple start states.

### The entry point

```go
// toys3/sim/oracle.go:553-576
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
		return nil, []Failure{{... "checker budget exceeded" ...}}
	}
	if len(s.finals) == 0 {
		return nil, []Failure{{Invariant: "linearizable", Msg: describeWindow(start, ws)}}
	}
	return s.finals, nil
}
```

The set of linearized operations is a bitset (`[]uint64`, one bit per op in the component's window). The memo (`visited`) and the set of collected end states (`seen`) are shared across all start states, so a configuration reached from two different start states is explored once.

### The DFS

```go
// toys3/sim/oracle.go:578-639
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
```

Walk through it.

**State of the search.** A node is `(set, at, st)`: which operations have been linearized, the logical time of the latest linearization point, and the model state after applying them in order.

**Memo.** If this `(set, st)` was seen before, return. If the memo is full, stop and flag `over`.

**min1 / min2.** Among the *required* operations not yet linearized, find the earliest `ret` (`min1`, at index `minIdx`) and the second earliest (`min2`). Optional operations do not participate: they may be skipped forever, so they never force anything.

**Record an end state.** If no required operation remains (`minIdx < 0`), the current state is a valid end state of the window. Record it (deduplicated by its string form). Then *keep going*: optional operations that have not been linearized yet may still be linearized later within their intervals, producing more end states. Every node with no required operation left is an end state, so the set of finals covers every subset of optional ops that can be consistently placed.

**Try each unlinearized op as the next one.** For candidate `i`:

- Its linearization point is `p = max(at, w.inv)`: as early as possible, but not before the previous point (points are non-decreasing) and not before the op was invoked.
- If `p > w.ret`, it is too late to place this op. For a required op, this branch dies (and since all orders are tried, if every branch dies, the history is rejected). For an optional op, it means "this op never happened", which is allowed: we simply do not pick it.
- **The bound.** If `p` is later than the return of some *other* required unlinearized op, then that other op would have to be linearized after `p`, beyond its own return. Impossible. So prune. `bound` is `min1` unless the candidate *is* the min op, in which case it is `min2` (the candidate does not bound itself). This is the WGL "only minimal operations can go next" rule, expressed with logical times.
- **Apply.** If the model disagrees with the observed result, prune.
- **Recurse** with the op added to the set, the new point, and the new state.

Choosing the *earliest possible* point `p` is not a restriction. Any valid linearization can be shifted so that each point is as early as its constraints allow without changing the order, and an earlier point never removes options for later operations.

### Why memoizing on (set, state) without `at` is sound

The memo key is `memoKey(set, st)`; it does not include `at`. That looks like a bug: two paths could reach the same set and state with different `at`, and the one with the larger `at` has fewer options (more ops would be "too late"). If the larger-`at` path were explored first and memoized, the smaller-`at` path would be pruned and a valid linearization lost.

It is not a bug, because **`at` is a function of `set`**. Claim: at every node, `at = max{ inv_j : j ∈ set }` (and 0 for the empty set).

*Proof by induction.* At the root, `set = ∅` and `at = 0`. Suppose the claim holds at a node. A child adds op `i` with `p = max(at, inv_i) = max(max_{j∈set} inv_j, inv_i) = max_{j∈set∪{i}} inv_j`. The child's `at` is `p`, so the claim holds for the child. ∎

So two nodes with the same `set` have the same `at`, and `(set, st)` identifies the node completely. (The `min1`/`min2` bounds are also functions of `set`.) This is a nice property of using *logical invocation times* as linearization points: in a version using real-valued points chosen freely inside intervals, the memo key would need the point too.

### Termination and complexity

Each recursion adds one bit to the set, so depth is at most the window size. The memo bounds the total number of nodes by (number of subsets reachable) × (number of distinct states). Without memoization the search is O(n!) in the worst case; with it, it is bounded by 2ⁿ × |states|, and in practice by far less, because the real-time constraints (`bound`) prune most orders. With windows of at most ~23 operations (the largest in 200 seeds), and usually under 15, the search is fast.

## 13.8 Worked examples: every test in oracle_test.go

The tests build histories by hand with a small helper (`toys3/sim/oracle_test.go:9-51`): `inv(req, op)` invokes, `ret(req, status, body)` replies, `crash()` crashes, and each call advances a fake clock by one. `check(t, wantOK)` calls `CheckQuiescent` and then `CheckFinal`, and asserts acceptance or rejection. `put(key, req)` builds a PUT with `MakeValue(req, 20)`, and `val(req)` is that value. In the timelines below, the numbers are the oracle's logical sequence numbers (`InvSeq`/`RetSeq`), which here match the fake clock.

### Test 1: TestOracleAcceptsLinearizableConcurrency (accept)

```go
// toys3/sim/oracle_test.go:53-70
h.inv(1, put("a", 1))
h.inv(2, core.Get{Key: "a"}) // overlaps the PUT: may see absent...
h.inv(3, core.Get{Key: "a"}) // ...or the new value
h.ret(2, core.StatusOK, val(1))
h.ret(3, core.StatusNotFound, nil)
h.ret(1, core.StatusOK, nil)
h.inv(4, core.Copy{Src: "a", Dst: "b"}); h.ret(4, core.StatusOK, nil)
h.inv(5, core.Get{Key: "b"});            h.ret(5, core.StatusOK, val(1))
h.inv(6, core.Delete{Key: "a"});         h.ret(6, core.StatusOK, nil)
h.inv(7, core.Delete{Key: "a"});         h.ret(7, core.StatusNotFound, nil)
```

```
 seq:  1    2    3    4    5    6    7    8    9   10   11   12   13   14
 PUT1  [============================]                      a := 1
 GET2       [==========]                                     sees a=1
 GET3            [==========]                                sees absent
 COPY4                               [====]                  b := a
 GET5                                          [====]        sees b=1
 DEL6                                                [====]  200
 DEL7                                                          [====] 404
```

All operations are Definite; one window (no quiescence until the end). Components: COPY ties `a` and `b`, so one component `{a, b}`, starting from `[{}]`.

Search: the first point must handle GET3 (absent) before PUT1, and GET2 (a=1) after PUT1. Candidates at the root: PUT1 (inv 1), GET2 (inv 2), GET3 (inv 3). `min1` = 4 (GET2 returns at 4), `min2` = 5.

- Try GET2 first: p = 2, apply on `{}`: 200 with a absent → inconsistent. Prune.
- Try PUT1 first: p = 1 ≤ bound 4. State `{a=1}`. Now GET3 at p = max(1,3)=3: 404 on `{a=1}` → inconsistent. GET2 at p=2: OK. Then GET3 at p=3: still inconsistent. Dead end.
- Try GET3 first: p = 3 ≤ bound 4 (min1 is GET2's ret 4). 404 on `{}` → OK. Then PUT1 at p = max(3,1) = 3 ≤ 4. State `{a=1}`. Then GET2 at p = max(3,2) = 3 ≤ GET2's ret 4: 200 a=1 → OK. Then COPY4 (p=7), GET5 (p=9, b=1 ✓), DEL6 (p=11, a present ✓ → absent), DEL7 (p=13, absent → 404 ✓).

Linearization: GET3, PUT1, GET2, COPY4, GET5, DEL6, DEL7. Notice that GET2 *returned* (seq 4) before GET3 *returned* (seq 5), yet GET3 is linearized first. That is fine: they overlap. **Accepted**, end state `{b=1}`.

### Test 2: TestOracleRejectsReadNotYetPossible (reject)

```go
// toys3/sim/oracle_test.go:72-83
h.inv(1, put("a", 1)); h.ret(1, core.StatusOK, nil)
h.inv(2, core.Get{Key: "a"}); h.ret(2, core.StatusOK, val(1))
h.inv(3, core.Get{Key: "a"}); h.ret(3, core.StatusNotFound, nil)
```

```
 seq: 1  2  3  4  5  6
 PUT1 [==]              a := 1
 GET2       [==]        sees 1
 GET3             [==]  sees absent   ✗
```

No overlaps, so the only order is PUT1, GET2, GET3. GET3 on `{a=1}` returns 404: inconsistent. **Rejected.** (The test's name is a little misleading: the problem is not a read "not yet possible" but a value that vanished. The failure message is `no linearization of 3 ops from any of 1 possible start states {}`.)

### Test 3: TestOracleRejectsStaleRead (reject)

```
 seq: 1  2  3  4  5  6
 PUT1 [==]              a := 1
 PUT2       [==]        a := 2
 GET3             [==]  sees 1   ✗ (stale)
```

Sequential order forced; GET3 on `{a=2}` expects writer 2, observed writer 1. **Rejected.**

### Test 4: TestOracleRejectsAckedThenLost (reject)

```go
// toys3/sim/oracle_test.go:96-107
h.inv(1, put("a", 1)); h.ret(1, core.StatusOK, nil)
h.crash()
if f := h.o.CheckQuiescent(); f != nil { t.Fatal(f[0].Msg) }
h.inv(2, core.Get{Key: "a"}) // after the restart, in the next window
h.ret(2, core.StatusNotFound, nil)
h.check(t, false)
```

```
 seq:  1  2  3      4  5
 PUT1  [==]   ⚡     .         a := 1 (acked)
              crash  .
 GET2               [==]       sees absent  ✗
 window 1: {PUT1}   │ window 2: {GET2}
 end states {a=1} ──┘ carried as start state
```

Window 1 contains just PUT1 (Definite). End states `[{a=1}]`; `factor` makes one group `{a: [{a=1}]}`. The crash abandons nothing (nothing was pending) and, crucially, *changes nothing in the model*. Window 2 starts from `{a=1}`; GET2's 404 is inconsistent. **Rejected.** This is the black-box statement of durability.

### Test 5: TestOracleFailedMutationMayOrMayNotApply (accept, four variants)

First variant family:

```go
// toys3/sim/oracle_test.go:110-124
h.inv(1, put("a", 1)); h.ret(1, core.StatusOK, nil)
h.inv(2, put("a", 2)); h.ret(2, core.StatusServerError, nil)
h.crash()
h.inv(3, core.Get{Key: "a"})
// applied:  h.ret(3, 200, val(2));  not applied: h.ret(3, 200, val(1))
```

```
 seq:  1  2  3  4  5      6  7
 PUT1  [==]                          a := 1
 PUT2        [==] 500                optional: a := 2 within [3,4], or never
                     ⚡ crash
 GET3                     [==]       sees 2  (or 1)
```

There is no intermediate check in this test, so everything is one window. PUT2 is optional with `ret = 4`. Branches: PUT1 then PUT2 (p=3) then GET3 (p=6) sees 2 ✓; or PUT1, skip PUT2, GET3 sees 1 ✓. Both histories **accepted**. Note that PUT2 cannot be placed *after* GET3: GET3's inv is 6 > PUT2's ret 4.

Second family: an **abandoned** DELETE.

```
 seq:  1  2  3      4      5  6
 PUT1  [==]                          a := 1
 DEL2        [=====⚡                abandoned: may delete within [3,4]
                    crash (4)
 GET3                      [==]      sees absent (or 1)
```

`h.crash()` marks DEL2 Abandoned with `RetSeq` = 4 (the crash tick). Both outcomes are explainable. **Accepted.**

### Test 6: TestOracleRejectsObservedThenVanished (reject)

```go
// toys3/sim/oracle_test.go:142-156
h.inv(1, put("a", 1)); h.ret(1, core.StatusOK, nil)
h.inv(2, put("a", 2)); h.ret(2, core.StatusServerError, nil)
h.inv(3, core.Get{Key: "a"}); h.ret(3, core.StatusOK, val(2))
h.crash()
h.inv(4, core.Get{Key: "a"}); h.ret(4, core.StatusOK, val(1))
```

```
 seq:  1  2  3  4  5  6     7      8  9
 PUT1  [==]                                a := 1
 PUT2        [==] 500                      optional, interval [3,4]
 GET3              [==]                    sees 2  → PUT2 must have applied
                         ⚡ crash
 GET4                          [==]        sees 1  ✗ (crashes erase nothing)
```

GET3 forces PUT2 to be applied (in [3,4]). The model then holds `a=2` forever; nothing after the crash can bring back `a=1`. Skipping PUT2 makes GET3 inconsistent. **Rejected.** This is the abstract form of mutant d2 and of the story in Chapter 1, §1.1.

### Test 7: TestOracleRejectsGarbageAndMustRespectRealTime (reject, twice)

Part 1: GET returns `"not a value at all"`. `window()` calls `ValueWriter`, which fails, and returns the failure `GET ... returned bytes no PUT wrote` without searching. **Rejected.**

Part 2:

```
 seq: 1  2  3  4
 GET1 [==]          sees value-of-2
 PUT2       [==]    a := 2   (invoked after GET1 returned)
```

GET1 needs `a=2`, so PUT2 must come first. Trying PUT2 first: `p = max(0, 3) = 3`, but `bound = min1 = 2` (GET1 is required and returns at 2), and 3 > 2: pruned. Trying GET1 first: inconsistent on `{}`. **Rejected.** A read cannot observe a write from its future.

### Test 8: TestOracleCarriesUncertaintyAcrossWindows (accept, accept, accept, reject)

```go
// toys3/sim/oracle_test.go:173-191
h.inv(1, put("a", 1))
h.inv(2, put("b", 2))
h.crash()
h.check(t, true)
h.inv(3, core.Get{Key: "a"}); h.ret(3, core.StatusOK, val(1)); h.check(t, true)
h.inv(4, core.Get{Key: "b"}); h.ret(4, core.StatusNotFound, nil); h.check(t, true)
h.inv(5, core.Get{Key: "a"}); h.ret(5, core.StatusNotFound, nil); h.check(t, false)
```

Trace the carried groups through each window:

```
 window 1: PUT1 a (abandoned), PUT2 b (abandoned)
   components: {a}, {b}   (no op touches both)
   {a}: finals [{}, {a=1}]  → factor → group a: [∅, {a=1}]
   {b}: finals [{}, {b=2}]  → factor → group b: [∅, {b=2}]
   (possible global states: 4, stored as 2 + 2)

 window 2: GET3 a → 200 value-of-1
   component {a}, start states [∅, {a=1}]
   from ∅: inconsistent; from {a=1}: ✓   finals [{a=1}]
   group a: [{a=1}]          group b unchanged

 window 3: GET4 b → 404
   component {b}, start [∅, {b=2}]  → finals [∅]
   factor: b absent in every state → no group for b (certainly absent)

 window 4: GET5 a → 404
   component {a}, start [{a=1}]  → 404 inconsistent    ✗
```

This test is the clearest demonstration of *carried uncertainty*: after a crash, the oracle does not guess; it keeps every possibility, and later reads collapse them, one key at a time, until a contradiction appears.

### Test 9: TestOracleWidensInsteadOfGivingUp (accept with widening, then reject)

```go
// toys3/sim/oracle_test.go:193-227 (abridged)
// 14 keys a..n: one abandoned PUT each
for _, k := range keys { req++; h.inv(req, put(k, req)) }
h.crash(); h.check(t, true)
// 13 COPYs chaining a→b→c→...→n, all invoked, then all return 200
for i := 1; i < len(keys); i++ { req++; h.inv(req, core.Copy{Src: keys[i-1], Dst: keys[i]}) }
for i := 1; i < len(keys); i++ { h.ret(core.ClientReq(len(keys)+i), core.StatusOK, nil) }
h.check(t, true)                             // must widen, must not fail
if h.o.Stats().Widenings == 0 { t.Fatal("expected a widening") }
// then: PUT a=28 (200), PUT a=29 (200), GET a → value-of-28  ✗
```

Window 1 leaves 14 independent groups, each `[∅, {k=req}]`. Window 2's COPY chain connects all 14 keys into one component. Building start states multiplies the groups in key order: after 12 groups there are 2¹² = 4096 states (not more than `maxStartStates`); after the 13th, 8192 > 4096, so `widenStates` collapses them into a single state in which keys `a` through `m` are `unknown` (they disagree across states) and `n` is absent (not yet multiplied in, so absent in all states and omitted). Multiplying the 14th group (for `n`) gives 2 start states. The 13 COPYs then run over `unknown` sources, which `apply` accepts and propagates as `unknown` destinations. The window is **accepted**, with `Widenings ≥ 1`.

The final part checks that widening is temporary: two fresh 200 PUTs on `a` give it a known value again (writer 29), so a GET returning `value-of-28` is stale. **Rejected.** Precision is regained by writing.

## 13.9 Dissecting real failures

Now the payoff: three failure messages from the mutation suite (Chapter 15), decoded with everything above. The failure message format comes from `describeWindow` (`oracle.go:651-671`) and `describeOp` (`oracle.go:687-700`):

```
req <id> client <n> <OP> [<InvSeq>..<RetSeq> @<Invoke ns>..<Return ns>] -> <status or outcome> [value-of-<w> | garbage(<n> bytes)]
```

### Mutant d2, seed 132: an observed delete that came back

```
FAIL linearizable step 380: no linearization of 1 ops from any of 2 possible start states {} {k1=52}; window:
  req 55 client 0 GET "k1" [102..104 @82642556..84991187] -> 200 value-of-26
```

Read it literally: in the final window, the component containing `k1` had one operation, a GET of `k1` that returned the value written by request 26. The carried start states for that component were `{}` (k1 absent) and `{k1=52}` (k1 holds request 52's value). Neither contains `value-of-26`. So no linearization exists.

Now reconstruct *why* those were the start states, using the event log of the run (`./tracer -seed 132 -mutant d2-no-halt-on-dir-fsync-error`, Chapter 16):

```
#277  Request{c42 COPY "k1" -> "k1"}     ...  Committed{c42 "k1"}  (k1 = value-of-26)
#283  Request{c45 DELETE "k1"}
#305  Request{c47 DELETE "k1"}
#310  quiesce
#317  land FSUnlink{io68 /data/obj/k1} -> OK              ← c45's unlink: k1 gone (visibly)
#323  land FSFsyncDir{io70 /data/obj} -> EIO              ← the dir fsync fails
#324  FSFsyncDirDone{io70 EIO} => Reply{c45 500} FSUnlink{io71 /data/obj/k1}
                                                          ← mutant: 500 but NO Halt; c47 starts
#327  FSUnlinkDone{io71 ENOENT} => Reply{c47 404}         ← window 1 closes here (all idle)
#332  Request{c49 GET "k1"}
#337  Request{c51 GET "k1"}
#339  FSReadDone{io73 ENOENT} => Reply{c49 404}
#342  Request{c52 PUT "k1" value-of-52}
#343  FSFsyncDone{io74 EIO} => Reply{c48 500} Halt{fsync temp: EIO}
#343  CRASH (halt: fsync temp: EIO): kept=1 lost=2 ...    ← window 2 closes (c51, c52 abandoned)
                                                          ← crash REVERTS the un-synced unlink of k1
#356  Request{c55 GET "k1"}
#365  FSReadDone{io5 OK obj(value-of-26)} => Reply{c55 200 value-of-26}
#380  ... quiescent → window 3 checked → FAILURE
```

In model terms:

```
 window 1 (ends #327)                     window 2 (ends at crash #343)       window 3
 k1 = 26 (from c42 COPY, earlier)
 DEL c45 ── 500 (optional)                GET c49 → 404 (definite)            GET c55 → value-of-26  ✗
 DEL c47 ── 404 (definite)                GET c51 abandoned (ignored)
   c47's 404 requires k1 absent           PUT c52 abandoned (optional)
   ⇒ c45 MUST have applied                ⇒ finals: {} or {k1=52}
 finals: {}  (k1 absent)
```

Window 1 is accepted, but only by linearizing c45's failed DELETE (otherwise c47's 404 is impossible). The model now says "k1 is gone". Window 2 is consistent with that. The crash reverts the FakeFS's un-synced unlink, so the server reads `value-of-26` again in window 3. The model cannot un-delete. The client saw a key disappear (two 404s) and then reappear with its old value. That is the d2 bug, and note what was needed to see it: an EIO on exactly the dir fsync, two subsequent reads of the same key observing the visible-but-not-durable state, and a crash before anything else made the unlink durable. With 2 keys (seed 132's swarm shape), the reads land on `k1` quickly.

Also note *when* it was reported: the bad GET replied at step 365, but the failure is at step 380, the next quiescent point. The oracle only checks complete windows; per-step invariants (Chapter 14) report at the exact step, the oracle at the end of the window.

### Mutant f, seed 20: a value from the wrong key

```
FAIL linearizable step 1750: no linearization of 1 ops from any of 1 possible start states {k5=221}; window:
  req 248 client 3 GET "k5" [471..475 @255595778..256780566] -> 200 value-of-231
```

Here the model is certain: `k5` holds request 221's value (one start state). The GET returned request 231's value. From the log:

```
#1463  inc=12  Request{c221 PUT "k5" value-of-221}
#1468  inc=12  FSListDone{io1 OK [0000000000000004 ... 000000000000004b]}   ← leftovers NOT removed (mutant f)
#1472  inc=12  ... FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-221)}  ← writes into the EXISTING tmp/…04
#1489  inc=12  FSRename{io11 /data/tmp/0000000000000004 -> /data/obj/k5} -> OK
#1497  inc=12  Committed{c221 "k5"} Reply{c221 200}
#1518  inc=12  CRASH (injected crash)              ← tmp/…04's removal was never durable: the name comes back,
                                                     pointing at k5's inode (a hard link)
#1530  inc=13  FSListDone{io1 OK [0000000000000004 ...]}   ← still not removed
#1527  inc=13  Request{c231 PUT "k25" value-of-231}
#1534  inc=13  ... FSWrite{io4 /data/tmp/0000000000000004 obj(value-of-231)}  ← IOIDs restart at 1 each boot:
                                                     same temp name; O_TRUNC-style write into the SHARED inode
#1546  inc=13  FSRename{io10 /data/tmp/0000000000000004 -> /data/obj/k25}
#1669  inc=13  Request{c248 GET "k5"} ... FSReadDone{io52 OK obj(value-of-231)}   ← k5 now holds k25's value
```

Black-box, the oracle sees only: `k5` was acked as 221, nothing wrote `k5` since, and a GET of `k5` returned the value of a PUT to `k25`. No COPY moved 231 into `k5`. Unexplainable. `NoCorruptReads` cannot see this: the bytes are a perfectly valid value, just from the wrong key. Only a model-based check can. Chapter 11 explains the FakeFS mechanics (durable vs current directory entries) that make the hard link possible, and Chapter 15 discusses why this mutant needs many keys to be visible.

### Mutant e, seed 18: garbage

```
FAIL no-corrupt-reads step 657: 200 GET returned bytes no PUT wrote: req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes)
FAIL linearizable step 657: GET req 98 client 0 GET "k3" [184..185 @153865676..154136154] -> 200 garbage(13 bytes) returned bytes no PUT wrote
```

Two invariants fire at the same step. `NoCorruptReads` checks every completed GET immediately. The step also happened to complete a window (`[184..185]`: invoked and returned back to back, nothing else pending), so `Linearizable` ran too, and `window()` rejected the body before searching. The run stops after the step (both failures are recorded because the simulator finishes running the remaining invariants of the step before it checks `halted`). The 13 bytes are the payload of a 29-byte object file (16-byte header + 13) in which the FakeFS flipped one bit on read; the mutant skipped the CRC check, so the server served it.

## 13.10 Statistics: windows, maxwin, widened

Three oracle statistics appear in every stats line:

```go
// toys3/sim/oracle.go:90-97
type OracleStats struct {
	Windows      int
	MaxWindowOps int // ops in the largest window searched
	MaxVisited   int // memo entries in the largest search
	MaxStates    int // most possible states of one key group carried between windows
	Widenings    int // times the carried states were too many and keys were widened to "unknown"
}
```

The simulator copies `Windows`, `MaxWindowOps` and `Widenings` into `Stats` (`simulator.go:117-120`) as `windows=`, `maxwin=` and `widened=`. `MaxVisited` and `MaxStates` are kept by the oracle but not surfaced in the stats line; they are useful when tuning (an exercise below surfaces them).

How to read them:

- **`windows`** should be comparable to the number of quiesces plus crashes. If it drops to a handful, quiescence is not happening and windows are huge.
- **`maxwin`** is the best predictor of oracle cost. In the 200-seed run, `maxwin=23`. If you change the workload and it climbs past ~40, expect slowdowns and eventually `checker budget exceeded`.
- **`widened`** should be 0 in normal campaigns. A nonzero value means the check was weakened somewhere. Not wrong, but worth knowing: a bug on a widened key could slip through until the key is rewritten.

## 13.11 How this compares to other checkers

- **Knossos** (Clojure, used by Jepsen) implements WGL-style search and a "linear" algorithm over configurations, with indeterminate (`:info`) operations that may linearize any time after invocation. It is general (any model with a step function) and has to be, because Jepsen tests systems it cannot pause or observe internally.
- **Porcupine** (Go, Anish Athalye) implements the P-compositional WGL search with bitsets and a state cache, and is fast enough to check large histories. It requires the user to supply the partition function and the model.
- **Elle** (Kingsbury and Alvaro, VLDB 2020) takes a different approach for transactional systems: rather than searching orders, it infers dependency graphs from carefully designed workloads (append-only lists, unique values) and finds cycles. Its insistence on values that reveal their history is a cousin of `MakeValue`.

toys3's oracle is specialized in three ways that the general tools are not: it controls the workload (so it can force quiescence and make every value self-describing), it knows exactly when crashes happen (so indeterminate operations have a bounded interval, and "crashes erase nothing" is a model rule), and it carries possible states across windows, factored per key. Those specializations are what make a few hundred lines sufficient.

## Key takeaways

- Linearizability: every operation takes effect at one instant inside its interval, and the resulting sequential execution explains every observed result. Real-time order is respected by construction.
- The oracle gives each reply a precise meaning: Definite (must happen, exactly as replied), Failed and Abandoned mutations (at most once, inside the interval, which ends at the reply or crash), failed reads (nothing). **Crashes erase nothing.**
- History uses logical sequence numbers, not virtual times, so same-instant events stay ordered.
- The check runs per window (between quiescent points), per connected component of keys (locality), from a set of carried start states factored into per-key groups.
- The WGL DFS places each op at the earliest point `max(at, inv)`, prunes with the earliest required return, and memoizes on (set, state), which is sound because `at` is the max invocation over the set.
- Widening to `unknown` trades precision for tractability and never causes false failures; the memo budget is a separate, tooling-level failure.
- The oracle sees bugs that no per-response check can: values that vanish (d2) and values from the wrong key (f).

## Exercises

1. **Predict, then test.** Add to `toys3/sim/oracle_test.go` a history where two overlapping PUTs (`a=1`, `a=2`) both return 200, followed by two sequential GETs that see 2 then 1. Predict accept or reject, then run `go test ./toys3/sim -run TestOracle -v`. Then make the second GET *overlap* the first. Does your answer change? Why not?
2. **A failed COPY.** Write a history: `PUT a=1` (200), `COPY a→b` (500), crash, `GET b` → `value-of-1`. Predict. Then change the GET's result to 404. Predict again. Use the `apply` table in §13.4 to justify both.
3. **Break the memo.** Change `memoKey` to ignore `st` (key only on the set). Find a history in `oracle_test.go` that is now wrongly accepted or rejected, and explain why using §13.7.
4. **Break the bound.** Delete the `if p > bound { continue }` check (`oracle.go:628-630`). Which test fails? (Prediction first. Hint: think about real-time order with no overlap.) Is the oracle now unsound (accepting bad histories) or incomplete (rejecting good ones)?
5. **Relax Failed to Jepsen semantics.** In `window()`, set `w.ret = never` for Failed ops (as if a 500 could take effect at any later time). Re-run `go test ./toys3/sim -run 'TestCatchesBugs/d2' -v`. Predict whether d2 is still caught, and explain in terms of §13.2.
6. **Surface the hidden stats.** Add `MaxVisited` and `MaxStates` to `sim.Stats` and its `String()`. Run `go test ./toys3/sim -run 'TestSeeds$' -seeds=200 -v` and report the largest memo size. How close is it to the `1 << 20` budget?
7. **Force widening in a real run.** Find a configuration (more keys, more crashes, more clients) under which `widened` becomes nonzero in some seed. Then check whether the mutants are still caught at the same seeds.
8. **Fix the comments.** The comments at `oracle.go:27` ("when the Request was delivered") and `oracle.go:299` (`Oracle.widen`) are slightly inaccurate (§13.3, §13.4). Write the corrected comments (do this on a branch).

## Further reading

- Maurice Herlihy and Jeannette Wing, *"Linearizability: A Correctness Condition for Concurrent Objects"*, ACM TOPLAS 12(3), 1990. Definition, locality theorem.
- Jeannette Wing and Chun Gong, *"Testing and Verifying Concurrent Objects"*, Journal of Parallel and Distributed Computing, 1993. The search algorithm.
- Gavin Lowe, *"Testing for Linearizability"*, Concurrency and Computation: Practice and Experience, 2017. Memoization and the improved WGL algorithm.
- Alex Horn and Daniel Kroening, *"Faster Linearizability Checking via P-Compositionality"*, FORTE 2015. Partitioning histories by object.
- Knossos (https://github.com/jepsen-io/knossos), Jepsen's linearizability checker.
- Porcupine (https://github.com/anishathalye/porcupine), a fast linearizability checker in Go.
- Kyle Kingsbury and Peter Alvaro, *"Elle: Inferring Isolation Anomalies from Experimental Observations"*, VLDB 2020.
