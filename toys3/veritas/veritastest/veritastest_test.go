package veritastest

import (
	"errors"
	"fmt"
	"testing"

	"manifester/toys3/control"
	"manifester/toys3/veritas"
)

// counter is a toy check over ints with a planted bug: an op >= 50 that
// comes after an op >= 30 fails.
func counter() Check[*int, int] {
	return Check[*int, int]{
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
			var out []int
			for _, m := range shorter(op) {
				out = append(out, m)
			}
			return out
		},
		Format: func(op int) string { return fmt.Sprint(op) },
		Ops:    50,
	}
}

func TestShrinkFindsMinimal(t *testing.T) {
	c := counter()
	f := c.RunSeed(1)
	if f == nil {
		t.Fatal("planted bug not found")
	}
	// Halving 30 or 50 passes, and n-1 of them fails, so the shrinker walks
	// both down to the boundary.
	if got, want := c.Literal(f.Ops), "[]int{\n\t30,\n\t50,\n}"; got != want {
		t.Fatalf("shrunk to\n%s\nwant\n%s", got, want)
	}
	if g := c.RunSeed(1); c.Literal(g.Ops) != c.Literal(f.Ops) || g.Runs != f.Runs {
		t.Fatal("shrinking is not deterministic")
	}
}

// leakyUnlink is veritas.FS with a planted bug: unlink reports success but
// keeps the file.
type leakyUnlink struct{ *veritas.FS }

func (l leakyUnlink) Exec(a control.Actuation) control.Feedback {
	if u, ok := a.(control.FSUnlink); ok {
		if _, exists := l.ReadFile(u.Path); exists {
			return control.FSUnlinkDone{IO: u.IO}
		}
	}
	return l.FS.Exec(a)
}

func TestFSCheckShrinksPlantedBug(t *testing.T) {
	c := FSCheck(func() FSUnderTest { return leakyUnlink{veritas.NewFS(FSDirs()...)} })
	var f *Failure[control.Actuation]
	for seed := uint64(1); f == nil && seed <= 20; seed++ {
		f = c.RunSeed(seed)
	}
	if f == nil {
		t.Fatal("planted bug not found in 20 seeds")
	}
	if len(f.Ops) != 2 {
		t.Fatalf("want 2 operations (write, unlink), got:\n%s", c.Report(f, ""))
	}
	w, ok1 := f.Ops[0].(control.FSWrite)
	u, ok2 := f.Ops[1].(control.FSUnlink)
	if !ok1 || !ok2 || w.Path != "/d/obj/a" || u.Path != "/d/obj/a" || len(w.Data) != 0 {
		t.Fatalf("not the simplest sequence:\n%s", c.Report(f, ""))
	}
	t.Logf("example report:\n%s", c.Report(f, "go test ./toys3/veritas/veritastest -run TestFSCheckShrinksPlantedBug"))
}

func TestFSCheckVeritasAgainstItself(t *testing.T) {
	c := FSCheck(func() FSUnderTest { return veritas.NewFS(FSDirs()...) })
	for seed := uint64(1); seed <= 50; seed++ {
		if f := c.RunSeed(seed); f != nil {
			t.Fatal(c.Report(f, ""))
		}
	}
}

func TestPattern(t *testing.T) {
	if got := string(Pattern(5, 'y')); got != "yzabc" {
		t.Fatalf("Pattern(5, 'y') = %q", got)
	}
	if Pattern(0, 'a') != nil {
		t.Fatal("Pattern(0) not nil")
	}
}

func TestReplayFS(t *testing.T) {
	// The shape of a pasted regression test: rename onto itself, rename over
	// an existing file, and a write that truncates.
	ReplayFS(t, func() FSUnderTest { return veritas.NewFS(FSDirs()...) }, []control.Actuation{
		control.FSWrite{IO: 1, Path: "/d/obj/a", Data: Pattern(4097, 'q')},
		control.FSRename{IO: 2, From: "/d/obj/a", To: "/d/obj/a"},
		control.FSWrite{IO: 3, Path: "/d/tmp/1", Data: []byte("x")},
		control.FSRename{IO: 4, From: "/d/tmp/1", To: "/d/obj/a"},
		control.FSWrite{IO: 5, Path: "/d/obj/a"},
		control.FSRead{IO: 6, Path: "/d/obj/a"},
	})
}

func TestShrinkFSEveryKind(t *testing.T) {
	last, lastDir := FSPaths()[4], FSDirs()[1]
	ops := []control.Actuation{
		control.FSRead{IO: 1, Path: last},
		control.FSList{IO: 2, Dir: lastDir},
		control.FSWrite{IO: 3, Path: last, Data: Pattern(10, 'q')},
		control.FSFsync{IO: 4, Path: last},
		control.FSFsyncDir{IO: 5, Dir: lastDir},
		control.FSUnlink{IO: 6, Path: last},
		control.FSRename{IO: 7, From: last, To: last},
	}
	for _, a := range ops {
		if got := shrinkFS(a); len(got) == 0 {
			t.Errorf("%T: no shrinks", a)
		}
	}
	if got := shrinkFS(control.FSRename{From: last, To: last}); len(got) != 8 {
		t.Errorf("rename shrinks = %d, want 8", len(got))
	}
	if got := shrinkFS(control.FSRead{Path: FSPaths()[0]}); len(got) != 0 {
		t.Errorf("simplest read shrinks: %v", got)
	}
	if got := shrinkFS(nil); got != nil {
		t.Errorf("nil shrinks: %v", got)
	}
}

func TestFormatFS(t *testing.T) {
	cases := map[string]control.Actuation{
		`control.FSWrite{IO: 1, Path: "/d/obj/a"}`:                                    control.FSWrite{IO: 1, Path: "/d/obj/a"},
		`control.FSWrite{IO: 1, Path: "/d/obj/a", Data: veritastest.Pattern(3, 'c')}`: control.FSWrite{IO: 1, Path: "/d/obj/a", Data: Pattern(3, 'c')},
		`control.FSWrite{IO: 1, Path: "/d/obj/a", Data: []byte("xx")}`:                control.FSWrite{IO: 1, Path: "/d/obj/a", Data: []byte("xx")},
		`control.FSFsync{IO: 2, Path: "/p"}`:                                          control.FSFsync{IO: 2, Path: "/p"},
		`control.FSRename{IO: 3, From: "/a", To: "/b"}`:                               control.FSRename{IO: 3, From: "/a", To: "/b"},
		`control.FSFsyncDir{IO: 4, Dir: "/d"}`:                                        control.FSFsyncDir{IO: 4, Dir: "/d"},
		`control.FSRead{IO: 5, Path: "/p"}`:                                           control.FSRead{IO: 5, Path: "/p"},
		`control.FSUnlink{IO: 6, Path: "/p"}`:                                         control.FSUnlink{IO: 6, Path: "/p"},
		`control.FSList{IO: 7, Dir: "/d"}`:                                            control.FSList{IO: 7, Dir: "/d"},
	}
	for want, a := range cases {
		if got := formatFS(a); got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
	if got := formatFS(nil); got != "<nil>" {
		t.Errorf("nil: %s", got)
	}
}

type recTB struct{ errs, logs []string }

func (r *recTB) Helper()                   {}
func (r *recTB) Errorf(f string, a ...any) { r.errs = append(r.errs, fmt.Sprintf(f, a...)) }
func (r *recTB) Logf(f string, a ...any)   { r.logs = append(r.logs, fmt.Sprintf(f, a...)) }

func TestRunModes(t *testing.T) {
	good := Check[*int, int]{
		Name: "int", New: func() *int { return new(int) },
		Gen:  func(r *Rand, i int) int { return i },
		Step: func(*int, int) error { return nil }, Format: func(op int) string { return fmt.Sprint(op) }, Ops: 3,
	}
	var r recTB
	Run(&r, good, Options{Seed: 5})
	Run(&r, good, Options{Seed: -1, Seeds: 4})
	calls := 0
	Run(&r, good, Options{Seed: -1, Stop: func() bool { calls++; return calls > 2 }})
	if len(r.errs) != 0 || len(r.logs) != 3 {
		t.Fatalf("errs %v logs %v", r.errs, r.logs)
	}
	Run(&r, counter(), Options{Seed: -1, Seeds: 200, Replay: func(s uint64) string { return "replay" }})
	Run(&r, counter(), Options{Seed: 1})
	if len(r.errs) == 0 {
		t.Fatal("planted bug not reported")
	}
}
