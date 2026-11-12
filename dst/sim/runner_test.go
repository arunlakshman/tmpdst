package sim_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/dst/eng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
)

func TestSameSeedSameHash(t *testing.T) {
	run := func(seed uint64) uint64 {
		r := sim.New(sim.Config{Seed: seed})
		r.Spawn(&echoProc{id: "p"})
		r.Inject("p", 0, "ping")
		res, err := r.Run()
		if err != nil {
			t.Fatal(err)
		}
		return res.Hash
	}
	first, second := run(7), run(7)
	if first != second {
		t.Fatal("hash drift")
	}
}

type echoProc struct{ id sim.ProcessID }

func (p *echoProc) ID() sim.ProcessID { return p.id }
func (p *echoProc) StateHash() uint64 { return 1 }
func (p *echoProc) Step(_ sim.Context, inbox any) []any {
	if inbox == "ping" {
		return []any{sim.Send{To: p.id, Body: "pong"}}
	}
	return nil
}

func TestUnhandledOutbound(t *testing.T) {
	r := sim.New(sim.Config{Seed: 1})
	r.Spawn(&outProc{id: "p", out: []any{struct{ X int }{1}}})
	r.Inject("p", 0, true)
	_, err := r.Run()
	if !errors.Is(err, sim.ErrProtocol) {
		t.Fatalf("got %v, want ErrProtocol", err)
	}
}

type outProc struct {
	id  sim.ProcessID
	out []any
}

func (p *outProc) ID() sim.ProcessID           { return p.id }
func (p *outProc) StateHash() uint64           { return 0 }
func (p *outProc) Step(sim.Context, any) []any { return p.out }

func TestCrashAndRestart(t *testing.T) {
	r := sim.New(sim.Config{Seed: 3})
	n := &countProc{id: "n"}
	sup := &outProc{id: "sup", out: nil}
	r.Spawn(n)
	r.Spawn(sup)

	// Crash the node via a kill message, then restart with a boot inbox.
	r.Spawn(&outProc{id: "killer", out: []any{sim.Send{To: "n", Body: "kill"}}})
	r.Inject("killer", 0, true)

	// After kill is processed, inject restart from supervisor.
	r.Inject("sup", sched.TimeNs(1), true)
	sup.out = []any{sim.Restart{Who: "n", Boot: "boot"}}

	res, err := r.Run()
	if err != nil {
		t.Fatal(err)
	}
	if n.kills != 1 {
		t.Fatalf("kills=%d want 1", n.kills)
	}
	if n.boots != 1 {
		t.Fatalf("boots=%d want 1", n.boots)
	}
	if res.Steps == 0 {
		t.Fatal("no steps")
	}
}

type countProc struct {
	id           sim.ProcessID
	kills, boots int
}

func (p *countProc) ID() sim.ProcessID { return p.id }
func (p *countProc) StateHash() uint64 { return uint64(p.kills + p.boots) }
func (p *countProc) Step(_ sim.Context, inbox any) []any {
	switch inbox {
	case "kill":
		p.kills++
		return []any{sim.Crash{}}
	case "boot":
		p.boots++
		return nil
	default:
		return nil
	}
}

func TestErrNotMineRouting(t *testing.T) {
	r := sim.New(sim.Config{Seed: 5})
	first := &skipEngine{id: "skip"}
	second := &takeEngine{id: "take"}
	r.Register(first)
	r.Register(second)
	r.Spawn(&outProc{id: "p", out: []any{takeAct{}}})
	r.Inject("p", 0, true)
	_, err := r.Run()
	if err != nil {
		t.Fatal(err)
	}
	if !first.seen {
		t.Fatal("first engine was not tried")
	}
	if !second.hit {
		t.Fatal("second engine did not handle")
	}
}

type takeAct struct{}

type skipEngine struct {
	id   eng.ID
	seen bool
}

func (e *skipEngine) ID() eng.ID        { return e.id }
func (e *skipEngine) StateHash() uint64 { return 0 }
func (e *skipEngine) Handle(_ eng.Context, _ any) error {
	e.seen = true
	return eng.ErrNotMine
}
func (e *skipEngine) OnSelf(eng.Context, any) {}

type takeEngine struct {
	id  eng.ID
	hit bool
}

func (e *takeEngine) ID() eng.ID        { return e.id }
func (e *takeEngine) StateHash() uint64 { return 0 }
func (e *takeEngine) Handle(_ eng.Context, act any) error {
	if _, ok := act.(takeAct); ok {
		e.hit = true
		return nil
	}
	return eng.ErrNotMine
}
func (e *takeEngine) OnSelf(eng.Context, any) {}

func TestEngineSelfAndDeliver(t *testing.T) {
	r := sim.New(sim.Config{Seed: 9})
	e := &relayEngine{id: "relay", to: "p"}
	p := &inboxProc{id: "p"}
	r.Register(e)
	r.Spawn(p)
	r.Spawn(&outProc{id: "c", out: []any{relayAct{}}})
	r.Inject("c", 0, true)
	_, err := r.Run()
	if err != nil {
		t.Fatal(err)
	}
	if !e.selfHit {
		t.Fatal("OnSelf not called")
	}
	if !p.got {
		t.Fatal("Deliver did not reach process")
	}
}

type relayAct struct{}

type relayEngine struct {
	id      eng.ID
	to      eng.ActorID
	selfHit bool
}

func (e *relayEngine) ID() eng.ID        { return e.id }
func (e *relayEngine) StateHash() uint64 { return 0 }
func (e *relayEngine) Handle(cx eng.Context, act any) error {
	if _, ok := act.(relayAct); !ok {
		return eng.ErrNotMine
	}
	cx.Self(0, "wake")
	return nil
}
func (e *relayEngine) OnSelf(cx eng.Context, body any) {
	e.selfHit = body == "wake"
	cx.Deliver(e.to, 0, "hello")
}

type inboxProc struct {
	id  sim.ProcessID
	got bool
}

func (p *inboxProc) ID() sim.ProcessID { return p.id }
func (p *inboxProc) StateHash() uint64 { return 0 }
func (p *inboxProc) Step(_ sim.Context, inbox any) []any {
	if inbox == "hello" {
		p.got = true
	}
	return nil
}

func TestWallIsWallStartPlusNow(t *testing.T) {
	var got []sched.TimeNs
	p := &fnProc{id: "p", step: func(cx sim.Context, inbox any) []any {
		got = append(got, cx.Now(), cx.Wall())
		return nil
	}}
	r := sim.New(sim.Config{Seed: 1, WallStart: 1_000_000})
	r.Spawn(p)
	r.Inject("p", 5, "a")
	if _, err := r.Run(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 5 || got[1] != 1_000_005 {
		t.Fatalf("now/wall = %v", got)
	}
}

// fnProc is a Process whose Step is a function.
type fnProc struct {
	id   sim.ProcessID
	step func(cx sim.Context, inbox any) []any
}

func (p *fnProc) ID() sim.ProcessID { return p.id }
func (p *fnProc) StateHash() uint64 { return 0 }
func (p *fnProc) Step(cx sim.Context, inbox any) []any {
	return p.step(cx, inbox)
}

func TestCrashWhoAndExit(t *testing.T) {
	steps := map[sim.ProcessID]int{}
	mk := func(id sim.ProcessID, out ...any) *fnProc {
		return &fnProc{id: id, step: func(_ sim.Context, _ any) []any { steps[id]++; return out }}
	}
	r := sim.New(sim.Config{Seed: 1})
	r.Spawn(mk("victim"))
	r.Spawn(mk("killer", sim.Crash{Who: "victim", Cause: "test"}))
	r.Spawn(mk("quitter", sim.Exit{}))
	r.Inject("killer", 0, "go")
	r.Inject("quitter", 0, "go")
	r.Inject("victim", 10, "late")
	r.Inject("quitter", 10, "late")
	if _, err := r.Run(); err != nil {
		t.Fatal(err)
	}
	if steps["victim"] != 0 || steps["quitter"] != 1 {
		t.Fatalf("steps = %v", steps)
	}
	if !r.Crashed("victim") || r.Exited("victim") || !r.Exited("quitter") {
		t.Fatal("victim must be crashed, quitter must have exited")
	}
	r.Spawn(mk("quitter"))
	if r.Exited("quitter") || r.Crashed("quitter") {
		t.Fatal("Spawn must clear exit")
	}
}

// callEngine answers an "io" call after 10ns with "io-done" to the caller.
type callEngine struct{}

func (callEngine) ID() eng.ID              { return "io" }
func (callEngine) StateHash() uint64       { return 0 }
func (callEngine) OnSelf(eng.Context, any) {}
func (callEngine) Handle(cx eng.Context, act any) error {
	if act != "io" {
		return eng.ErrNotMine
	}
	cx.Deliver(cx.From(), 10, "io-done")
	return nil
}

func TestRestartDropsStaleTimerAndReply(t *testing.T) {
	var seen []string
	newProc := func(name string) *fnProc {
		return &fnProc{id: "n", step: func(_ sim.Context, inbox any) []any {
			seen = append(seen, name+":"+inbox.(string))
			if inbox == "boot" && name == "old" {
				return []any{sim.SetTimer{ID: 1, After: 20, Body: "timer"}, "io"}
			}
			return nil
		}}
	}
	r := sim.New(sim.Config{Seed: 1})
	r.Register(callEngine{})
	r.Spawn(newProc("old"))
	// "sup" restarts n with a new process at t=5: after the old boot sent its
	// call and armed its timer, before the reply (t=10) and the timer (t=20).
	r.Spawn(&fnProc{id: "sup", step: func(sim.Context, any) []any {
		return []any{sim.Restart{Who: "n", Boot: "boot", New: newProc("new")}}
	}})
	r.Inject("n", 0, "boot")
	r.Inject("sup", 5, "restart")
	if _, err := r.Run(); err != nil {
		t.Fatal(err)
	}
	for _, s := range seen {
		if s == "new:io-done" || s == "new:timer" {
			t.Fatalf("new incarnation saw a stale value: %v", seen)
		}
	}
	want := []string{"old:boot", "new:boot"}
	if len(seen) != 2 || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("seen = %v, want %v", seen, want)
	}
}

func TestCurrentIncarnationStillGetsItsReplyAndTimer(t *testing.T) {
	var seen []string
	p := &fnProc{id: "n", step: func(_ sim.Context, inbox any) []any {
		seen = append(seen, inbox.(string))
		if inbox == "boot" {
			return []any{sim.SetTimer{ID: 1, After: 20, Body: "timer"}, "io"}
		}
		return nil
	}}
	r := sim.New(sim.Config{Seed: 1})
	r.Register(callEngine{})
	r.Spawn(p)
	r.Inject("n", 0, "boot")
	if _, err := r.Run(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 || seen[1] != "io-done" || seen[2] != "timer" {
		t.Fatalf("seen = %v", seen)
	}
}

func TestOnStepSeesStateBeforeOutIsRouted(t *testing.T) {
	var log []string
	state := 0
	p := &fnProc{id: "p", step: func(_ sim.Context, inbox any) []any {
		state++
		if inbox == "a" {
			return []any{sim.Send{To: "p", Body: "b"}}
		}
		return nil
	}}
	r := sim.New(sim.Config{Seed: 1, OnStep: func(s sim.StepInfo) {
		log = append(log, fmt.Sprintf("%s:%v:%d:%d", s.Who, s.Inbox, len(s.Out), state))
	}})
	r.Spawn(p)
	r.Inject("p", 0, "a")
	if _, err := r.Run(); err != nil {
		t.Fatal(err)
	}
	if len(log) != 2 || log[0] != "p:a:1:1" || log[1] != "p:b:0:2" {
		t.Fatalf("log = %v", log)
	}
}

func TestRunUntilStopsOnPredicate(t *testing.T) {
	n := 0
	p := &fnProc{id: "p", step: func(sim.Context, any) []any {
		n++
		return []any{sim.Send{To: "p", Body: "again"}} // never ends by itself
	}}
	r := sim.New(sim.Config{Seed: 1})
	r.Spawn(p)
	r.Inject("p", 0, "go")
	if drained := r.RunUntil(func() bool { return r.Steps() >= 5 }); drained || n != 5 {
		t.Fatalf("drained=%v n=%d", drained, n)
	}
}

// A timer that was replaced must never fire, even after the new timer has fired
// and a third one was armed with the same ID.
func TestReplacedTimerNeverFiresAfterIDIsReused(t *testing.T) {
	var seen []string
	p := &fnProc{id: "p", step: func(_ sim.Context, inbox any) []any {
		seen = append(seen, inbox.(string))
		switch inbox {
		case "boot":
			return []any{
				sim.SetTimer{ID: 1, After: 100, Body: "A"}, // replaced at once, must not fire
				sim.SetTimer{ID: 1, After: 10, Body: "B"},
			}
		case "B":
			return []any{sim.SetTimer{ID: 1, After: 500, Body: "C"}} // reuses the ID after B fired
		}
		return nil
	}}
	r := sim.New(sim.Config{Seed: 1})
	r.Spawn(p)
	r.Inject("p", 0, "boot")
	if _, err := r.Run(); err != nil {
		t.Fatal(err)
	}
	want := []string{"boot", "B", "C"}
	if len(seen) != len(want) {
		t.Fatalf("seen = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("seen = %v, want %v", seen, want)
		}
	}
}
