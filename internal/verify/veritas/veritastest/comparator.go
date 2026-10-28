package veritastest

import (
	"fmt"
	"slices"
	"strings"
)

// Reporter is the small part of testing.TB that Run needs. *testing.T
// satisfies it. Keeping it small lets this package avoid importing testing.
type Reporter interface {
	Helper()
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

// SpecComparator compares an implementation with its spec after every step.
// S holds a fresh implementation paired with its spec. Op is one operation.
// The fields say how to build S, how to make operations, and how to run one
// operation on both sides.
type SpecComparator[S, Op any] struct {
	// Name is the Go type of Op as written in a regression test, for
	// example "core.IOCall". It is the first line of the printed sequence.
	Name string
	// New returns a fresh implementation and a fresh spec, both empty.
	New func() S
	// Gen returns operation number i of a sequence. It must take all random
	// values from r, so the same seed always gives the same sequence.
	Gen func(r *Rand, i int) Op
	// Step applies op to the implementation and to the spec. It returns an
	// error that describes any difference between the two results.
	Step func(s S, op Op) error
	// Invariant, if set, runs after every Step and checks a rule that must
	// always hold.
	Invariant func(s S) error
	// Shrink, if set, returns simpler versions of op. The simplest comes first.
	Shrink func(op Op) []Op
	// ShrinkAll, if set, returns simpler versions of a whole sequence. The
	// simplest comes first. Use it for a change that keeps the failure only
	// if it is made in every operation, such as renaming one path everywhere.
	// Each candidate must differ from ops. Otherwise shrinking never ends.
	ShrinkAll func(ops []Op) [][]Op
	// Format returns op as Go source text.
	Format func(op Op) string
	// Ops is the number of operations in each generated sequence.
	Ops int
	// MaxShrinkRuns is the most sequences shrinking may run. Zero means 5000.
	MaxShrinkRuns int
}

// Failure is a failing sequence after shrinking.
type Failure[Op any] struct {
	Seed     uint64
	Ops      []Op  // the shrunk sequence; its last operation fails
	Err      error // what went wrong at the last operation
	Original int   // length of the generated sequence up to the first failure
	Runs     int   // number of sequences run while shrinking
}

// Generate returns the operations for seed. The same seed gives the same list.
func (c SpecComparator[S, Op]) Generate(seed uint64) []Op {
	r := NewRand(seed)
	ops := make([]Op, c.Ops)
	for i := range ops {
		ops[i] = c.Gen(r, i)
	}
	return ops
}

// Replay runs ops on a fresh pair. It returns the index of the first failing
// operation and its error, or -1 and nil. A panic in Step or Invariant counts
// as a failure of that operation.
func (c SpecComparator[S, Op]) Replay(ops []Op) (int, error) {
	s := c.New()
	for i, op := range ops {
		if err := c.step(s, op); err != nil {
			return i, err
		}
	}
	return -1, nil
}

func (c SpecComparator[S, Op]) step(s S, op Op) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	if err := c.Step(s, op); err != nil {
		return err
	}
	if c.Invariant != nil {
		if err := c.Invariant(s); err != nil {
			return fmt.Errorf("invariant: %w", err)
		}
	}
	return nil
}

// RunSeed makes the sequence for seed and runs it. If it fails, RunSeed
// shrinks the sequence and returns the failure. If it passes, RunSeed returns
// nil.
func (c SpecComparator[S, Op]) RunSeed(seed uint64) *Failure[Op] {
	ops := c.Generate(seed)
	i, err := c.Replay(ops)
	if err == nil {
		return nil
	}
	f := &Failure[Op]{Seed: seed, Original: i + 1}
	f.Ops, f.Runs, f.Err = c.shrink(ops[:i+1], err)
	return f
}

// shrink makes the failing case smaller and returns it with its error. ops
// must fail at its last operation. It repeats these passes until none helps:
//
//  1. drop chunks: halves, then quarters, down to single operations;
//  2. replace each operation with the first Shrink candidate that still
//     fails, and repeat;
//  3. replace the whole sequence with the first ShrinkAll candidate that
//     still fails.
//
// After each success it cuts the sequence just after the new failing
// operation. Every choice follows a fixed order, so the result is the same
// on every run. Any failure is accepted, not only the first one, because a
// different failure is also a real bug.
func (c SpecComparator[S, Op]) shrink(ops []Op, err error) ([]Op, int, error) {
	budget := c.MaxShrinkRuns
	if budget == 0 {
		budget = 5000
	}
	s := shrinkState[Op]{ops: ops, err: err, budget: budget, replay: c.Replay}
	for progress := true; progress && s.runs < budget; {
		progress = s.dropChunks()
		if s.shrinkOne(c.Shrink) {
			progress = true
		}
		if s.shrinkAll(c.ShrinkAll) {
			progress = true
		}
	}
	return s.ops, s.runs, s.err
}

// shrinkState holds the failing sequence while it shrinks. try reports whether
// cand fails. If it does, cand becomes the new sequence.
type shrinkState[Op any] struct {
	ops    []Op
	err    error
	runs   int
	budget int
	replay func([]Op) (int, error)
}

func (s *shrinkState[Op]) try(cand []Op) bool {
	if s.runs >= s.budget {
		return false
	}
	s.runs++
	i, e := s.replay(cand)
	if e == nil {
		return false
	}
	s.ops, s.err = cand[:i+1], e
	return true
}

func (s *shrinkState[Op]) dropChunks() bool {
	progress := false
	for size := max(len(s.ops)/2, 1); size >= 1; size /= 2 {
		for i := 0; i+size <= len(s.ops); {
			cand := slices.Delete(slices.Clone(s.ops), i, i+size)
			if s.try(cand) {
				progress = true
			} else {
				i += size
			}
		}
	}
	return progress
}

func (s *shrinkState[Op]) shrinkOne(shrink func(Op) []Op) bool {
	if shrink == nil {
		return false
	}
	progress := false
	for i := 0; i < len(s.ops); i++ {
		for again := true; again && i < len(s.ops); {
			again = false
			for _, simpler := range shrink(s.ops[i]) {
				cand := slices.Clone(s.ops)
				cand[i] = simpler
				if s.try(cand) {
					progress, again = true, true
					break
				}
			}
		}
	}
	return progress
}

func (s *shrinkState[Op]) shrinkAll(shrinkAll func([]Op) [][]Op) bool {
	if shrinkAll == nil {
		return false
	}
	return slices.ContainsFunc(shrinkAll(s.ops), s.try)
}

// Literal returns ops as Go source text of type []c.Name.
func (c SpecComparator[S, Op]) Literal(ops []Op) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[]%s{\n", c.Name)
	for _, op := range ops {
		fmt.Fprintf(&b, "\t%s,\n", c.Format(op))
	}
	b.WriteString("}")
	return b.String()
}

// Report describes f: the seed, how to replay it, and the shrunk sequence.
// It always prints a replay line. If replay is empty, the line says that no
// replay command is configured.
func (c SpecComparator[S, Op]) Report(f *Failure[Op], replay string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %d: operation %d of the shrunk sequence failed: %v\n", f.Seed, len(f.Ops), f.Err)
	fmt.Fprintf(&b, "(shrunk from %d operations to %d in %d runs)\n", f.Original, len(f.Ops), f.Runs)
	if replay == "" {
		replay = "not configured (set Options.Replay)"
	}
	fmt.Fprintf(&b, "replay: %s\n", replay)
	fmt.Fprintf(&b, "shrunk sequence:\n%s", c.Literal(f.Ops))
	return b.String()
}

// Options chooses the seeds that Run uses. The caller maps its own test flags
// onto these fields.
type Options struct {
	// Seeds runs the seeds 1 to Seeds.
	Seeds int
	// Seed, if zero or more, runs only this seed. It overrides Seeds and Stop.
	Seed int64
	// Stop, if set, runs seeds 1, 2, 3 and so on until it returns true. It
	// overrides Seeds. Use it for runs limited by time.
	Stop func() bool
	// Replay returns the command that runs one seed again. Please set it:
	// without it, a failure report cannot tell the reader how to replay.
	Replay func(seed uint64) string
}

// Run runs c on the seeds that o chooses. It reports the first failure after
// shrinking it.
func Run[S, Op any](t Reporter, c SpecComparator[S, Op], o Options) {
	t.Helper()
	report := func(f *Failure[Op]) {
		t.Helper()
		replay := ""
		if o.Replay != nil {
			replay = o.Replay(f.Seed)
		}
		t.Errorf("%s", c.Report(f, replay))
	}
	if o.Seed >= 0 {
		if f := c.RunSeed(uint64(o.Seed)); f != nil {
			report(f)
			return
		}
		t.Logf("seed %d: %d operations passed", o.Seed, c.Ops)
		return
	}
	n := 0
	for seed := uint64(1); ; seed++ {
		if o.Stop != nil {
			if o.Stop() {
				break
			}
		} else if seed > uint64(o.Seeds) {
			break
		}
		n++
		if f := c.RunSeed(seed); f != nil {
			report(f)
			return
		}
	}
	t.Logf("%d seeds x %d operations passed", n, c.Ops)
}
