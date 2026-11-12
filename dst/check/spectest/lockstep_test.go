package spectest

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestLockstepPassesOnTheRightSubject(t *testing.T) {
	setLockstep("").Check(t)
}

func TestStepsRunsInsideACallerRapidCheck(t *testing.T) {
	l := setLockstep("")
	rapid.Check(t, l.Steps)
}

// TestPlantedBugIsCaughtAndShrunk is the test the T1.3 card asks for: a
// planted bug in a tiny example spec is caught, and rapid shrinks it to at
// most 3 ops.
func TestPlantedBugIsCaughtAndShrunk(t *testing.T) {
	var found *Failure[setOp]
	RequireCaught(t, []Bug{bugRemoveLast}, 20, func(bug Bug, seed uint64) bool {
		found = setLockstep(bug).RunSeed(seed)
		return found != nil
	})
	if found == nil {
		t.Fatal("no failure")
	}
	if len(found.Steps) > 3 {
		t.Fatalf("shrunk to %d steps, want 3 or fewer:\n%s", len(found.Steps), setLockstep("").GoCode(found.Steps))
	}
	last := found.Steps[len(found.Steps)-1]
	if _, isRemove := last.Op.(Remove); last.Kind != KindOp || !isRemove {
		t.Fatalf("the last step is %+v, want a Remove", last)
	}
	RequireClean(t, 20, func(seed uint64) bool { return setLockstep("").RunSeed(seed) != nil })
}

func TestCrashBugIsCaughtWithADirtyReboot(t *testing.T) {
	var found *Failure[setOp]
	RequireCaught(t, []Bug{bugCrashNoLog}, 50, func(bug Bug, seed uint64) bool {
		found = setLockstep(bug).RunSeed(seed)
		return found != nil
	})
	if found == nil {
		t.Fatal("no failure")
	}
	last := found.Steps[len(found.Steps)-1]
	if last.Kind != KindDirtyReboot || len(found.Steps) > 3 {
		t.Fatalf("want a short list that ends with a dirty reboot, got:\n%s", setLockstep("").GoCode(found.Steps))
	}
	if !strings.Contains(found.Message, "after a crash") {
		t.Fatalf("message = %q", found.Message)
	}
}

func TestHasFailedAllowsNoAnswer(t *testing.T) {
	l := setLockstep(bugNoAnswerAdd)
	l.NewSubject = func(*rapid.T) *setSubject { return &setSubject{bug: bugNoAnswerAdd, injected: true} }
	RequireCaught(t, []Bug{bugNoAnswerAdd}, 20, func(_ Bug, seed uint64) bool { return l.RunSeed(seed) != nil })

	l.HasFailed = func(s *setSubject) bool { return s.injected }
	RequireClean(t, 20, func(seed uint64) bool { return l.RunSeed(seed) != nil })
}

func TestInvariantIsCheckedAfterEveryStep(t *testing.T) {
	l := setLockstep("")
	l.Invariant = func(s *setSubject) error {
		if s.mem&1 != 0 {
			return errors.New("0 is in the set")
		}
		return nil
	}
	f := l.RunSeed(1)
	for seed := uint64(2); f == nil && seed < 50; seed++ {
		f = l.RunSeed(seed)
	}
	if f == nil || !strings.Contains(f.Message, "invariant broken: 0 is in the set") {
		t.Fatalf("failure = %+v", f)
	}
	if len(f.Steps) != 1 || f.Steps[0].Op != (Add{Key: 0}) {
		t.Fatalf("shrunk steps = %+v, want [Add{0}]", f.Steps)
	}
}

func TestRunSeedIsRepeatable(t *testing.T) {
	a := setLockstep(bugRemoveLast).RunSeed(3)
	b := setLockstep(bugRemoveLast).RunSeed(3)
	if (a == nil) != (b == nil) || (a != nil && fmt.Sprint(a.Steps) != fmt.Sprint(b.Steps)) {
		t.Fatalf("seed 3 gave two results: %+v and %+v", a, b)
	}
}

func TestRunSeedPutsTheFlagsBack(t *testing.T) {
	before := flag.Lookup("rapid.seed").Value.String()
	setLockstep(bugRemoveLast).RunSeed(9)
	if after := flag.Lookup("rapid.seed").Value.String(); after != before {
		t.Fatalf("rapid.seed is %s, was %s", after, before)
	}
}

// fakeTB is a rapid.TB that keeps the Fatalf message and does not stop.
type fakeTB struct {
	rapid.TB
	name  string
	fatal string
}

func (f *fakeTB) Helper()                           {}
func (f *fakeTB) Name() string                      { return f.name }
func (f *fakeTB) Logf(string, ...any)               {}
func (f *fakeTB) Log(...any)                        {}
func (f *fakeTB) Fatalf(format string, args ...any) { f.fatal = fmt.Sprintf(format, args...) }

func TestCheckPrintsSeedGoCodeAndCommand(t *testing.T) {
	tb := &fakeTB{name: "TestBox/one"}
	l := setLockstep(bugRemoveLast)
	for seed := uint64(1); tb.fatal == "" && seed <= 20; seed++ {
		restore, err := setRapidFlags(seed, true)
		if err != nil {
			t.Fatal(err)
		}
		l.check(tb)
		restore()
	}
	if tb.fatal == "" {
		t.Fatal("no seed from 1 to 20 caught the bug")
	}
	for _, want := range []string{
		"lockstep failed: step ",
		"seed: ",
		"as Go code:\n[]spectest.setOp{\n",
		"spectest.Remove{Key:",
		"run it again (in the package folder): P_SEED=",
		" go test -run '^TestBox$/^one$' -count=1 -v .",
	} {
		if !strings.Contains(tb.fatal, want) {
			t.Errorf("report has no %q:\n%s", want, tb.fatal)
		}
	}
}

func TestCheckRunsTheSeedFromTheVariable(t *testing.T) {
	t.Setenv("P_SEED", "7")
	setLockstep("").Check(t)
	if v := flag.Lookup("rapid.seed").Value.String(); v == "7" {
		t.Fatalf("rapid.seed was not put back")
	}
}

func TestGoCodeShowsRebootsAsComments(t *testing.T) {
	got := setLockstep("").GoCode([]Step[setOp]{
		{Kind: KindOp, Op: Add{Key: 1}},
		{Kind: KindCheckpoint},
		{Kind: KindDirtyReboot, Keep: 2},
		{Kind: KindReboot},
		{Kind: KindOp, Op: Has{Key: 1}},
	})
	want := "[]spectest.setOp{\n\tspectest.Add{Key:1},\n\t// Checkpoint()\n\t// DirtyReboot(2)\n\t// Reboot()\n\tspectest.Has{Key:1},\n}"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	l := setLockstep("")
	l.Name, l.Format = "set.Op", func(op setOp) string { return fmt.Sprintf("op(%v)", op) }
	if got := l.GoCode([]Step[setOp]{{Op: Add{Key: 2}}}); got != "[]set.Op{\n\top({2}),\n}" {
		t.Fatalf("got %q", got)
	}
}

func TestReportWithoutSeed(t *testing.T) {
	got := setLockstep("").Report(&Failure[setOp]{Message: "boom"}, "TestX")
	if !strings.Contains(got, "seed: not known") || strings.Contains(got, "run it again") {
		t.Fatalf("got %q", got)
	}
}

func TestSeedOf(t *testing.T) {
	cases := map[string]uint64{
		`To reproduce, specify -run="X" -rapid.seed=123`:                   123,
		`-rapid.failfile="a.fail" (or -rapid.seed=99)`:                     99,
		`To reproduce, specify -run="X" -rapid.failfile="testdata/x.fail"`: 0,
		`-rapid.seed=`: 0,
	}
	for msg, want := range cases {
		if got := seedOf(msg); got != want {
			t.Errorf("seedOf(%q) = %d, want %d", msg, got, want)
		}
	}
}

func TestSimplestFirstShrinksToTheFirstGenerator(t *testing.T) {
	gen := SimplestFirst(rapid.Just("read"), rapid.Just("write"), rapid.Just("crash"))
	l := Lockstep[string, string, echo]{
		NewSpec:    func() Spec[string, string] { return echoSpec{} },
		NewSubject: func(*rapid.T) echo { return echo{} },
		Ops:        gen,
		Equal: func(want, got string) error {
			if got == "write" {
				return errors.New("any write fails")
			}
			return nil
		},
	}
	var f *Failure[string]
	for seed := uint64(1); f == nil && seed < 50; seed++ {
		f = l.RunSeed(seed)
	}
	if f == nil || len(f.Steps) != 1 || f.Steps[0].Op != "write" {
		t.Fatalf("failure = %+v, want one write", f)
	}
}

type echoSpec struct{}

func (echoSpec) Apply(op string) string { return op }
func (echoSpec) StateHash() uint64      { return 0 }

type echo struct{}

func (echo) Apply(op string) (string, error) { return op, nil }
