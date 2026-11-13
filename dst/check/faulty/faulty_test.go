package faulty_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// The tests use a tiny spec: a map from a name to a number. A "set" op stores a
// number and a "get" op reads it. The caller node issues the requests and keeps
// every answer it gets. Nothing here is specific to a product.

const ms = machine.Duration(1_000_000)

// request is one call to the service.
type request struct {
	id    machine.RequestID
	key   string
	set   bool // true: store val under key. false: read key.
	val   int64
	limit bool // the test limit refuses this request
	quiet bool // a set that changes the state, but whose answer says "no change"
}

func (r request) Fold(h *trace.Hash) {
	h.Uint64(1)
	h.Uint64(uint64(r.id))
	h.String(r.key)
	h.Bool(r.set)
	h.Int64(r.val)
	h.Bool(r.limit)
	h.Bool(r.quiet)
}

// answer is what the caller gets back.
type answer struct {
	id      machine.RequestID
	val     int64
	changed bool // the spec changed its state
	failure faulty.Failure
}

// input is the input of the caller node: a command to issue a request, or an
// answer from the service.
type input struct {
	issue bool
	req   request
	ans   answer
}

func (i input) Fold(h *trace.Hash) {
	h.Uint64(2)
	h.Bool(i.issue)
	i.req.Fold(h)
	h.Uint64(uint64(i.ans.id))
	h.Int64(i.ans.val)
	h.Bool(i.ans.changed)
	h.Uint64(uint64(i.ans.failure))
}

// spec is the fault-free map.
type spec struct{ m map[string]int64 }

func (s *spec) Apply(r request) answer {
	if r.set {
		old, had := s.m[r.key]
		s.m[r.key] = r.val
		return answer{id: r.id, val: r.val, changed: (!had || old != r.val) && !r.quiet}
	}
	return answer{id: r.id, val: s.m[r.key]}
}

func (s *spec) StateHash() uint64 {
	keys := make([]string, 0, len(s.m))
	//determinismcheck:ok the keys are sorted below
	for k := range s.m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	h := trace.New()
	for _, k := range keys {
		h.String(k)
		h.Int64(s.m[k])
	}
	return h.Sum()
}

// The assertions of the test service.
var (
	testAsserts = faulty.Asserts{ //determinismcheck:ok catalog handles, set once at start
		Outage:      assert.NewReachable("test outage started"),
		Unavailable: assert.NewReachable("test 503 injected"),
		Timeout:     assert.NewReachable("test timeout injected"),
		Lost:        assert.NewReachable("test lost response injected"),
		Applied:     assert.NewSometimes("test timed-out op applied"),
		NotApplied:  assert.NewSometimes("test timed-out op not applied"),
	}
	testFlipped = assert.NewReachable("test answer corrupted") //determinismcheck:ok catalog handle, set once at start
)

// Which extras a test turns on.
type extras struct {
	corruptProb float64
	limit       bool
}

func shape(e extras) faulty.Shape[request, input, request, answer] {
	s := faulty.Shape[request, input, request, answer]{
		Name:      "testservice",
		Prefix:    "test",
		Asserts:   testAsserts,
		Op:        func(r request) request { return r },
		RequestID: func(r request) machine.RequestID { return r.id },
		Mutating:  func(r request) bool { return r.set },
		Changed:   func(a answer) bool { return a.changed },
		Reply:     func(_ request, a answer) input { return input{ans: a} },
		Fail: func(r request, f faulty.Failure) input {
			return input{ans: answer{id: r.id, failure: f}}
		},
	}
	if e.limit {
		s.Limit = func(r request) (answer, bool) {
			if r.limit {
				return answer{id: r.id, failure: faulty.Unavailable}, true
			}
			return answer{}, false
		}
	}
	if e.corruptProb > 0 {
		s.Corrupt = &faulty.Corruption[request, answer]{
			Prob:    e.corruptProb,
			Targets: func(r request) bool { return !r.set },
			Flip: func(a answer, bits uint64) (answer, uint64, bool) {
				bit := bits % 8
				a.val ^= 1 << bit
				return a, bit, true
			},
			Kind:    "test_corrupt",
			Reached: testFlipped,
		}
	}
	return s
}

// delivery is one answer that reached the caller.
type delivery struct {
	at  kernel.Time
	ans answer
}

// caller is the machine of the caller node.
type caller struct{ t *testWorld }

func (c caller) Step(_ machine.Now, in input) []request {
	if in.issue {
		return []request{in.req}
	}
	c.t.got = append(c.t.got, delivery{at: c.t.w.Now(), ans: in.ans})
	return nil
}
func (caller) StateHash() uint64 { return 0 }

// testWorld is a kernel world with one caller node "a" and the service.
type testWorld struct {
	tb   testing.TB
	w    *kernel.World
	a    kernel.Addr[input]
	sp   *spec
	svc  *faulty.Service[request, input, request, answer]
	got  []delivery
	next machine.RequestID
}

func newWorld(tb testing.TB, seed uint64, f faulty.Faults, e extras) *testWorld {
	tb.Helper()
	t := &testWorld{tb: tb, w: kernel.New(kernel.Config{Seed: seed}), sp: &spec{m: map[string]int64{}}}
	t.svc = faulty.New(t.w, faulty.Spec[request, answer](t.sp), f, shape(e))
	addr := kernel.AddService[request, input](t.w, t.svc)
	t.a = kernel.Spawn(t.w, kernel.NodeSpec[input, request]{
		Name:  "a",
		New:   func(machine.Env, machine.NodeInstance) machine.Machine[input, request] { return caller{t} },
		Route: func(r *kernel.Router[input], out request) { kernel.Call(r, addr, out) },
	})
	t.w.Start()
	tb.Cleanup(func() {
		for _, f := range t.w.Finish().Failures {
			tb.Errorf("run failure: %s: %s", f.Property, f.Detail)
		}
	})
	return t
}

// base is a service with timing and no faults.
func base() faulty.Faults {
	ts := kernel.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return faulty.Faults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

// do makes the caller issue r after a delay. It gives r the next id if it has none.
func (t *testWorld) do(after machine.Duration, r request) {
	if r.id == 0 {
		t.next++
		r.id = t.next
	}
	kernel.Inject(t.w, t.a, after, input{issue: true, req: r})
}

func (t *testWorld) set(id machine.RequestID, key string, val int64) {
	t.do(0, request{id: id, key: key, set: true, val: val})
}

func (t *testWorld) get(id machine.RequestID, key string) { t.do(0, request{id: id, key: key}) }

func (t *testWorld) run() { t.w.RunUntil(nil) }

func (t *testWorld) runTo(at kernel.Time) {
	reached := false
	t.w.At(at, kernel.NoLane, func() { reached = true })
	t.w.RunUntil(func() bool { return reached })
}

// step takes exactly one queue entry.
func (t *testWorld) step() bool {
	start := t.w.Steps()
	t.w.RunUntil(func() bool { return t.w.Steps() > start })
	return t.w.Steps() > start
}

func (t *testWorld) recover() { t.w.SetPhase(kernel.PhaseRecovering) }

// find returns the answer to request id (false if none came).
func (t *testWorld) find(id machine.RequestID) (delivery, bool) {
	for _, d := range t.got {
		if d.ans.id == id {
			return d, true
		}
	}
	return delivery{}, false
}

// faults lists the kinds of the injected faults, in order.
func (t *testWorld) faults() []string {
	var kinds []string
	for _, f := range t.w.Timeline().Snapshot("sim_fault") {
		if k, ok := f.Fact.Field("kind").(machine.StringValue); ok {
			kinds = append(kinds, string(k))
		}
	}
	return kinds
}

// hit reports whether the assertion passed at least once.
func (t *testWorld) hit(name string) bool {
	c, ok := t.w.Assert().Coverage()[name]
	return ok && c.Passes > 0
}

func (t *testWorld) value(key string) (int64, bool) {
	v, ok := t.sp.m[key]
	return v, ok
}

// Without faults the service gives the spec answers, applies each change once,
// and answers after the apply time and the respond time.
func TestNoFaultsGivesTheSpecAnswers(t *testing.T) {
	w := newWorld(t, 1, base(), extras{})
	w.set(1, "k", 7)
	w.run()
	w.get(2, "k")
	w.get(3, "nope")
	w.run()
	if d, _ := w.find(1); d.ans.failure != 0 || !d.ans.changed {
		t.Fatal(d)
	}
	if d, _ := w.find(2); d.ans.val != 7 {
		t.Fatal(d)
	}
	if d, _ := w.find(3); d.ans.val != 0 {
		t.Fatal(d)
	}
	if w.svc.Name() != "testservice" {
		t.Fatal(w.svc.Name())
	}
	if w.svc.Gen() != 1 {
		t.Fatal("a change and two reads must count one change, got", w.svc.Gen())
	}
	if got := w.faults(); len(got) != 0 {
		t.Fatal("no fault was asked for:", got)
	}
}

// Slow reply: with the slow tail always on, the answer comes after one slow
// apply time and one slow respond time.
func TestSlowReply(t *testing.T) {
	f := base()
	f.ApplyLatency.SlowProb, f.RespondLatency.SlowProb = 1, 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 1)
	w.run()
	d, ok := w.find(1)
	lo := kernel.Time(f.ApplyLatency.SlowMin + f.RespondLatency.SlowMin)
	hi := kernel.Time(f.ApplyLatency.SlowMax + f.RespondLatency.SlowMax)
	if !ok || d.at < lo || d.at > hi {
		t.Fatal("answer time", d.at, "want in", lo, hi)
	}
	// And with the tail off, the answer is fast.
	g := newWorld(t, 1, base(), extras{})
	g.set(1, "k", 1)
	g.run()
	if d, _ := g.find(1); d.at > kernel.Time(base().ApplyLatency.FastMax+base().RespondLatency.FastMax) {
		t.Fatal("fast answer came late:", d.at)
	}
}

// Error with nothing applied: the call is refused, the spec never sees it.
func TestRefusedAppliesNothing(t *testing.T) {
	f := base()
	f.Unavailable503 = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 7)
	w.run()
	d, ok := w.find(1)
	if !ok || d.ans.failure != faulty.Unavailable || len(w.got) != 1 {
		t.Fatal(w.got)
	}
	if _, has := w.value("k"); has || w.svc.Gen() != 0 {
		t.Fatal("a refused call was applied")
	}
	if !slices.Contains(w.faults(), "test_503") || !w.hit("test 503 injected") {
		t.Fatal(w.faults())
	}
}

// Timeout: the caller sees TimedOut after CallerTimeout, and the op is applied
// in some runs and not in others.
func TestTimeoutAppliesOrNot(t *testing.T) {
	f := base()
	f.Timeout = 1
	applied, notApplied := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		w := newWorld(t, seed, f, extras{})
		w.set(1, "k", 7)
		w.run()
		d, ok := w.find(1)
		if !ok || d.ans.failure != faulty.TimedOut || len(w.got) != 1 || d.at != kernel.Time(f.CallerTimeout) {
			t.Fatal(w.got)
		}
		if _, has := w.value("k"); has {
			applied++
		} else {
			notApplied++
		}
		if !slices.Contains(w.faults(), "test_timeout") {
			t.Fatal(w.faults())
		}
	}
	if applied == 0 || notApplied == 0 {
		t.Fatal("the coin must give both:", applied, notApplied)
	}
}

// Applied, but the reply is lost: the caller sees TimedOut, the op is applied
// every time, and a lost read still answers exactly once.
func TestLostReplyStillApplies(t *testing.T) {
	f := base()
	f.LostResponse = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 7)
	w.run()
	d, ok := w.find(1)
	if !ok || d.ans.failure != faulty.TimedOut || d.at != kernel.Time(f.CallerTimeout) {
		t.Fatal(w.got)
	}
	if v, has := w.value("k"); !has || v != 7 {
		t.Fatal("the lost call was not applied")
	}
	w.get(2, "k")
	w.run()
	n := 0
	for _, g := range w.got {
		if g.ans.id == 2 {
			n++
			if g.ans.failure != faulty.TimedOut {
				t.Fatal(g)
			}
		}
	}
	if n != 1 || !slices.Contains(w.faults(), "test_lost_response") || !w.hit("test lost response injected") {
		t.Fatal(n, w.faults())
	}
}

// The retry after a lost reply: the op was applied, the caller retries the same
// op in recovery, and the spec answers as for a replay. The service adds no
// effect of its own, so the retry is exactly as safe as the spec says.
func TestRetryAfterLostReply(t *testing.T) {
	f := base()
	f.LostResponse = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 7)
	w.run() // applied, the caller saw TimedOut
	w.recover()
	w.set(2, "k", 7) // the retry
	w.run()
	d, ok := w.find(2)
	if !ok || d.ans.failure != 0 || d.ans.val != 7 || d.ans.changed {
		t.Fatal("the retry must see the stored value and no change:", d)
	}
	if w.svc.Gen() != 1 {
		t.Fatal("the retry counted as a change:", w.svc.Gen())
	}
}

// Outage window: while it lasts, every call is refused. Only one window starts.
func TestOutageWindow(t *testing.T) {
	f := base()
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]machine.Duration{100 * ms, 200 * ms}}
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 1)
	w.runTo(50 * kernel.Time(ms)) // still inside the window
	w.set(2, "k", 1)
	w.run()
	for _, d := range w.got {
		if d.ans.failure != faulty.Unavailable {
			t.Fatal(d)
		}
	}
	faults := w.faults()
	if n := slices.Index(faults, "test_outage"); n < 0 || slices.Index(faults[n+1:], "test_outage") >= 0 {
		t.Fatal("want exactly one window", faults)
	}
	if _, has := w.value("k"); has {
		t.Fatal("a call was applied in the outage")
	}
	if !w.hit("test outage started") {
		t.Fatal("outage not recorded")
	}
	w.recover()
	w.runTo(10_000 * kernel.Time(ms))
	w.set(3, "k", 1)
	w.run()
	if d, _ := w.find(3); d.ans.failure != 0 {
		t.Fatal(d)
	}
}

// Corruption: one bit of the answer is flipped. The stored value stays whole.
func TestCorruptedAnswer(t *testing.T) {
	w := newWorld(t, 1, base(), extras{corruptProb: 1})
	w.set(1, "k", 100)
	w.run()
	w.get(2, "k")
	w.run()
	d, _ := w.find(2)
	if d.ans.val == 100 || bitsOf(d.ans.val^100) != 1 {
		t.Fatal("want one flipped bit:", d)
	}
	if v, _ := w.value("k"); v != 100 {
		t.Fatal("the stored value was damaged")
	}
	if !slices.Contains(w.faults(), "test_corrupt") || !w.hit("test answer corrupted") {
		t.Fatal(w.faults())
	}
	// A set is not a target: its answer stays whole.
	if d, _ := w.find(1); d.ans.val != 100 {
		t.Fatal("a set was corrupted:", d)
	}
}

func bitsOf(x int64) int {
	n := 0
	for ; x != 0; x &= x - 1 {
		n++
	}
	return n
}

// Limit: an op that the limit refuses never reaches the spec, and changes nothing.
func TestLimitRefusesBeforeTheSpec(t *testing.T) {
	w := newWorld(t, 1, base(), extras{limit: true})
	w.do(0, request{id: 1, key: "k", set: true, val: 1, limit: true})
	w.set(2, "j", 2)
	w.run()
	if d, _ := w.find(1); d.ans.failure != faulty.Unavailable {
		t.Fatal(d)
	}
	if _, has := w.value("k"); has {
		t.Fatal("a refused op reached the spec")
	}
	if d, _ := w.find(2); d.ans.failure != 0 || w.svc.Gen() != 1 {
		t.Fatal(d, w.svc.Gen())
	}
}

// The caller dies after it issued the call: the effect lands, and nothing is
// delivered to the dead caller.
func TestLandsAfterCallerCrash(t *testing.T) {
	w := newWorld(t, 1, base(), extras{})
	w.set(1, "k", 7)
	w.step() // the caller issues the call
	w.w.Crash(w.a, "test")
	w.run()
	if len(w.got) != 0 {
		t.Fatal("delivered to a dead node")
	}
	if v, ok := w.value("k"); !ok || v != 7 {
		t.Fatal("the call did not land")
	}
}

// The caller dies between the landing and the reply: the effect stands, and the
// reply goes neither to the dead incarnation nor to the next one.
func TestCrashBetweenLandingAndReply(t *testing.T) {
	w := newWorld(t, 2, base(), extras{})
	w.set(1, "k", 7)
	w.step()
	for _, ok := w.value("k"); !ok; _, ok = w.value("k") {
		if !w.step() {
			t.Fatal("the call never landed")
		}
	}
	w.w.Crash(w.a, "test")
	w.w.Boot(w.a) // a new incarnation must not get the answer of the old one
	w.run()
	if len(w.got) != 0 {
		t.Fatal("delivered to another incarnation")
	}
}

// A caller that died before the landing gets no answer, and the corruption step
// does not run for it: no bit is flipped and no fault is recorded.
func TestNoCorruptionForADeadCaller(t *testing.T) {
	w := newWorld(t, 1, base(), extras{corruptProb: 1})
	w.set(1, "k", 100)
	w.run()
	w.get(2, "k")
	w.step() // the caller issues the read
	w.w.Crash(w.a, "test")
	w.run()
	if len(w.got) != 1 {
		t.Fatal("the dead caller got an answer:", w.got)
	}
	if slices.Contains(w.faults(), "test_corrupt") {
		t.Fatal("a bit was flipped for a caller that is gone")
	}
}

// After EnterRecovery no new fault is drawn, and the corruption coin too.
func TestRecoveryStopsFaults(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 1, 1, 1
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]machine.Duration{ms, 2 * ms}}
	w := newWorld(t, 1, f, extras{corruptProb: 1})
	w.set(1, "k", 100) // faulted
	w.run()
	w.recover()
	w.set(2, "j", 100)
	w.run()
	if d, _ := w.find(1); d.ans.failure != faulty.Unavailable {
		t.Fatal(d)
	}
	if d, _ := w.find(2); d.ans.failure != 0 {
		t.Fatal(d)
	}
	w.get(3, "j")
	w.run()
	if d, _ := w.find(3); d.ans.val != 100 {
		t.Fatal("corrupted in recovery:", d)
	}
}

// A plan made before EnterRecovery stands. It is not drawn again.
func TestPlanInFlightStandsAfterRecovery(t *testing.T) {
	f := base()
	f.Unavailable503 = 1
	w := newWorld(t, 1, f, extras{})
	w.set(1, "k", 1)
	w.step() // issued: the plan is a refusal
	w.recover()
	w.set(2, "j", 2)
	w.run()
	if d, _ := w.find(1); d.ans.failure != faulty.Unavailable {
		t.Fatal("the plan in flight must stand:", d)
	}
	if d, _ := w.find(2); d.ans.failure != 0 {
		t.Fatal(d)
	}
}

// The cached spec hash is never stale, after any mix of calls and faults.
func TestSpecHashCacheIsNeverStale(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	w := newWorld(t, 5, f, extras{})
	for i := range 200 {
		key := string(rune('a' + i%5))
		switch {
		case i%3 == 2:
			w.get(machine.RequestID(i+1), key)
		case i%4 == 0:
			// A change that the answer does not name: only Mutating can clear the cache.
			w.do(0, request{id: machine.RequestID(i + 1), key: key, set: true, val: int64(i), quiet: true})
		default:
			w.set(machine.RequestID(i+1), key, int64(i%4))
		}
		w.runTo(w.w.Now() + kernel.Time(ms))
		if got := w.svc.SpecStateHash(); got != w.sp.StateHash() {
			t.Fatalf("op %d: cached spec hash is stale", i)
		}
	}
	w.run()
	if w.svc.SpecStateHash() != w.sp.StateHash() {
		t.Fatal("final: cached spec hash is stale")
	}
	// A change made outside a call needs Bump.
	w.sp.m["x"] = 1
	w.svc.Bump()
	if w.svc.SpecStateHash() != w.sp.StateHash() {
		t.Fatal("Bump did not clear the cached hash")
	}
}

// The same seed gives the same run, a different seed gives another, and the calls
// in flight are part of the state hash.
func TestDeterminism(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	f.Outage = faulty.Outage{Prob: 0.05, Dur: [2]machine.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) ([]delivery, uint64, uint64) {
		w := newWorld(t, seed, f, extras{corruptProb: 0.3})
		for i := range 60 {
			key := string(rune('a' + i%5))
			if i%3 == 0 {
				w.do(machine.Duration(i)*ms, request{id: machine.RequestID(i + 1), key: key, set: true, val: int64(i)})
			} else {
				w.do(machine.Duration(i)*ms, request{id: machine.RequestID(i + 1), key: key})
			}
		}
		w.runTo(30 * kernel.Time(ms))
		mid := w.svc.StateHash()
		w.run()
		return w.got, w.svc.StateHash(), mid
	}
	g1, h1, m1 := run(7)
	g2, h2, m2 := run(7)
	if !slices.Equal(g1, g2) || h1 != h2 || m1 != m2 {
		t.Fatal("the same seed diverged")
	}
	if g3, _, _ := run(8); slices.Equal(g1, g3) {
		t.Fatal("two seeds gave one run")
	}
	if m1 == h1 {
		t.Fatal("the calls in flight are not in the state hash")
	}
}
