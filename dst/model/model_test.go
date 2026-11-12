package model_test

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/dst/model"
)

type counter struct {
	n int
}

func (c *counter) Apply(op string) (int, error) {
	switch op {
	case "inc":
		c.n++
		return c.n, nil
	case "get":
		return c.n, nil
	case "boom":
		return c.n, errors.New("boom")
	default:
		return 0, fmt.Errorf("unknown op %q", op)
	}
}

func eqInt(want, got int) error {
	if want != got {
		return fmt.Errorf("want %d got %d", want, got)
	}
	return nil
}

func TestCheckLockstepCounter(t *testing.T) {
	m, s := &counter{}, &counter{}
	step := model.Step[string, int]{
		ModelApply:   m.Apply,
		SubjectApply: s.Apply,
		Equal:        eqInt,
		Invariant: func() error {
			if m.n != s.n {
				return fmt.Errorf("diverged model=%d subject=%d", m.n, s.n)
			}
			return nil
		},
	}
	for _, op := range []string{"get", "inc", "inc", "get"} {
		if err := step.Check(op); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
	}
	if m.n != 2 || s.n != 2 {
		t.Fatalf("n model=%d subject=%d", m.n, s.n)
	}
}

func TestCheckDetectsMismatch(t *testing.T) {
	m, s := &counter{}, &counter{n: 1}
	step := model.Step[string, int]{
		ModelApply:   m.Apply,
		SubjectApply: s.Apply,
		Equal:        eqInt,
	}
	if err := step.Check("get"); err == nil {
		t.Fatal("want mismatch error")
	}
}

func TestCheckAllowFailed(t *testing.T) {
	m := &counter{}
	s := &counter{}
	step := model.Step[string, int]{
		ModelApply: m.Apply,
		SubjectApply: func(op string) (int, error) {
			if op == "inc" {
				return 0, errors.New("crashed")
			}
			return s.Apply(op)
		},
		Equal: eqInt,
	}
	if err := step.CheckAllowFailed("inc", false); err == nil {
		t.Fatal("want subject error when failed=false")
	}
	if err := step.CheckAllowFailed("inc", true); err != nil {
		t.Fatalf("failed=true: %v", err)
	}
}

func TestDirtyReboot(t *testing.T) {
	store := map[string]int{"a": 1, "b": 2}
	answered := []model.Write[string, int]{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
	}
	eq := eqInt
	if err := model.DirtyReboot(answered, func(k string, v int) bool {
		return store[k] == v
	}, func() (int, error) { return 3, nil }, func() (int, error) { return 3, nil }, eq); err != nil {
		t.Fatalf("ok: %v", err)
	}
	delete(store, "b")
	if err := model.DirtyReboot(answered, func(k string, v int) bool {
		return store[k] == v
	}, func() (int, error) { return 3, nil }, func() (int, error) { return 3, nil }, eq); err == nil {
		t.Fatal("want persistence error")
	}
	store["b"] = 2
	if err := model.DirtyReboot(answered, func(k string, v int) bool {
		return store[k] == v
	}, func() (int, error) { return 3, nil }, func() (int, error) { return 1, nil }, eq); err == nil {
		t.Fatal("want forward progress error")
	}
}

func TestDirtyRebootHelpers(t *testing.T) {
	store := map[string]int{"a": 1, "b": 2}
	answered := []model.Write[string, int]{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
	}
	if err := model.CheckPersistence(answered, func(k string, v int) bool {
		return store[k] == v
	}); err != nil {
		t.Fatalf("persistence: %v", err)
	}
	delete(store, "b")
	if err := model.CheckPersistence(answered, func(k string, v int) bool {
		return store[k] == v
	}); err == nil {
		t.Fatal("want persistence error")
	}

	modelClean := func() (int, error) { return 3, nil }
	subjectOK := func() (int, error) { return 3, nil }
	subjectBad := func() (int, error) { return 1, nil }
	if err := model.CheckForwardProgress(modelClean, subjectOK, eqInt); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if err := model.CheckForwardProgress(modelClean, subjectBad, eqInt); err == nil {
		t.Fatal("want forward progress error")
	}
}

func TestModelSystemCheckGen(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m, s := &counter{}, &counter{}
		step := model.NewStep[string, int](m, s, eqInt)
		hasFailed := false
		gen := model.SimplestFirst(
			rapid.Just("get"),
			rapid.Just("inc"),
		)
		step.CheckGen(rt, gen, func() bool { return hasFailed })
		if m.n != s.n {
			rt.Fatalf("diverged model=%d subject=%d", m.n, s.n)
		}
	})
}

func TestCheckGenAllowFailed(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := &counter{}
		step := model.Step[string, int]{
			ModelApply: m.Apply,
			SubjectApply: func(op string) (int, error) {
				if op == "inc" {
					return 0, errors.New("crashed")
				}
				return 0, nil
			},
			Equal: eqInt,
		}
		gen := model.SimplestFirst(rapid.Just("inc"))
		step.CheckGen(rt, gen, func() bool { return true })
	})
}

func TestSimplestFirstShrinksToEarlier(t *testing.T) {
	if err := flag.Set("rapid.nofailfile", "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set("rapid.nofailfile", "false") })

	tb := &captureTB{name: t.Name()}
	rapid.Check(tb, func(rt *rapid.T) {
		op := model.SimplestFirst(
			rapid.Just("simple"),
			rapid.Just("complex"),
		).Draw(rt, "op")
		rt.Fatalf("fail on %s", op)
	})
	if !tb.failed {
		t.Fatal("want rapid failure")
	}
	out := tb.buf.String()
	if !strings.Contains(out, "fail on simple") {
		t.Fatalf("want shrink to simple, got %q", out)
	}
}

type captureTB struct {
	name   string
	buf    bytes.Buffer
	failed bool
}

func (c *captureTB) Helper()      {}
func (c *captureTB) Name() string { return c.name }
func (c *captureTB) Logf(format string, args ...any) {
	fmt.Fprintf(&c.buf, format, args...)
}
func (c *captureTB) Log(args ...any)      { fmt.Fprint(&c.buf, args...) }
func (c *captureTB) Skipf(string, ...any) { panic("skip") }
func (c *captureTB) Skip(...any)          { panic("skip") }
func (c *captureTB) SkipNow()             { panic("skip") }
func (c *captureTB) Errorf(format string, args ...any) {
	c.failed = true
	fmt.Fprintf(&c.buf, format, args...)
}
func (c *captureTB) Error(args ...any) { c.failed = true; fmt.Fprint(&c.buf, args...) }
func (c *captureTB) Fatalf(format string, args ...any) {
	c.failed = true
	fmt.Fprintf(&c.buf, format, args...)
}
func (c *captureTB) Fatal(args ...any) {
	c.failed = true
	fmt.Fprint(&c.buf, args...)
}
func (c *captureTB) FailNow()     { c.failed = true }
func (c *captureTB) Fail()        { c.failed = true }
func (c *captureTB) Failed() bool { return c.failed }
