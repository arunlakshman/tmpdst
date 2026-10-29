package veritastest

import (
	"errors"
	"strconv"
	"testing"
)

// counter has a planted bug: an op >= 50 after an op >= 30 fails.
func counter() SpecChecker[*int, int] {
	return SpecChecker[*int, int]{
		Name: "int",
		New:  func() *int { return new(int) },
		Gen:  func(r *Rand, i int) int { return r.Intn(100) },
		Step: func(armed *int, op int) error {
			if *armed > 0 && op >= 50 {
				return errors.New("planted bug")
			}
			if op >= 30 {
				*armed = 1
			}
			return nil
		},
		Shrink: func(op int) []int {
			if op == 0 {
				return nil
			}
			return []int{0, op / 2, op - 1}
		},
		Format: strconv.Itoa,
		Ops:    50,
	}
}

func TestShrinkFindsMinimal(t *testing.T) {
	c := counter()
	f := c.RunSeed(1)
	if f == nil {
		t.Fatal("planted bug not found")
	}
	if got, want := c.Literal(f.Ops), "[]int{\n\t30,\n\t50,\n}"; got != want {
		t.Fatalf("shrunk to\n%s\nwant\n%s", got, want)
	}
	if g := c.RunSeed(1); c.Literal(g.Ops) != c.Literal(f.Ops) || g.Runs != f.Runs {
		t.Fatal("shrinking is not deterministic")
	}
}

func TestInvariantAndPanicFail(t *testing.T) {
	c := SpecChecker[*int, int]{
		Name:      "int",
		New:       func() *int { return new(int) },
		Gen:       func(r *Rand, i int) int { return i },
		Step:      func(s *int, op int) error { *s += op; return nil },
		Invariant: func(s *int) error { return nil },
		Format:    strconv.Itoa,
		Ops:       5,
	}
	if f := c.RunSeed(1); f != nil {
		t.Fatalf("unexpected failure: %v", f.Err)
	}
	c.Invariant = func(s *int) error {
		if *s >= 3 {
			return errors.New("too big")
		}
		return nil
	}
	f := c.RunSeed(1)
	if f == nil || f.Err.Error() != "invariant: too big" {
		t.Fatalf("invariant not reported: %+v", f)
	}
	c.Invariant = nil
	c.Step = func(s *int, op int) error { panic("boom") }
	if f := c.RunSeed(1); f == nil || len(f.Ops) != 1 || f.Err.Error() != "panic: boom" {
		t.Fatalf("panic not reported: %+v", f)
	}
}

func TestShrinkAll(t *testing.T) {
	// Fails when the first two ops sum to >= 14, so lowering all together
	// walks the sequence down to the boundary.
	c := SpecChecker[*[]int, int]{
		Name: "int",
		New:  func() *[]int { return new([]int) },
		Gen:  func(r *Rand, i int) int { return 20 },
		Step: func(s *[]int, op int) error {
			*s = append(*s, op)
			if len(*s) == 2 && (*s)[0]+(*s)[1] >= 14 {
				return errors.New("bad")
			}
			return nil
		},
		ShrinkAll: func(ops []int) [][]int {
			down := make([]int, len(ops))
			for i, o := range ops {
				down[i] = o - 1
			}
			return [][]int{down}
		},
		Format: strconv.Itoa,
		Ops:    2,
	}
	f := c.RunSeed(3)
	if f == nil || c.Literal(f.Ops) != "[]int{\n\t7,\n\t7,\n}" {
		t.Fatalf("got %+v", f)
	}
}

type fakeReporter struct{ errs, logs int }

func (*fakeReporter) Helper()                 {}
func (f *fakeReporter) Errorf(string, ...any) { f.errs++ }
func (f *fakeReporter) Logf(string, ...any)   { f.logs++ }

func TestRun(t *testing.T) {
	var tb fakeReporter
	Run(&tb, counter(), Options{Seeds: 5, Seed: -1})
	if tb.errs != 1 {
		t.Fatalf("errs = %d", tb.errs)
	}
	tb = fakeReporter{}
	Run(&tb, counter(), Options{Seed: 1})
	if tb.errs != 1 {
		t.Fatalf("single seed errs = %d", tb.errs)
	}
	tb = fakeReporter{}
	n := 0
	ok := counter()
	ok.Step = func(*int, int) error { return nil }
	Run(&tb, ok, Options{Seed: -1, Stop: func() bool { n++; return n > 3 }})
	if tb.errs != 0 || tb.logs != 1 {
		t.Fatalf("errs=%d logs=%d", tb.errs, tb.logs)
	}
}

func TestRand(t *testing.T) {
	a, b := NewRand(7), NewRand(7)
	for range 100 {
		if x, y := a.Uint64(), b.Uint64(); x != y {
			t.Fatal("not reproducible")
		}
		if n := a.Intn(10); n < 0 || n >= 10 {
			t.Fatalf("Intn out of range: %d", n)
		}
		b.Intn(10)
	}
	if got := NewRand(0).Uint64(); got != 0xe220a8397b1dcdaf {
		t.Fatalf("SplitMix64(0) first output = %#x", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Intn(0) did not panic")
		}
	}()
	a.Intn(0)
}
