package veritastest

import (
	"fmt"
	"slices"
	"strings"
)

// TB is the part of testing.TB the runner uses.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

// Check is one conformance check: how to build a fresh implementation paired
// with its veritas (S), how to generate operations (Op), and how to run one
// operation on both and compare.
type Check[S, Op any] struct {
	// Name is the Go type of Op as written in a regression test (for
	// example "core.IOAction"); it heads the printed sequence.
	Name string
	// New returns a fresh implementation and veritas, both empty.
	New func() S
	// Gen returns the i-th operation of a sequence. Draw everything from r.
	Gen func(r *Rand, i int) Op
	// Step applies op to the implementation and to the veritas and returns
	// an error describing any difference in their results.
	Step func(s S, op Op) error
	// Invariant, if set, runs after every Step.
	Invariant func(s S) error
	// Shrink, if set, returns simpler versions of op, simplest first.
	Shrink func(op Op) []Op
	// ShrinkAll, if set, returns simpler versions of a whole sequence,
	// simplest first: changes that only keep a failure if made everywhere
	// at once, like renaming a path in every operation that uses it. A
	// candidate must differ from ops, or it counts as progress forever.
	ShrinkAll func(ops []Op) [][]Op
	// Format returns op as a Go expression.
	Format func(op Op) string
	// Ops is the length of each generated sequence.
	Ops int
	// MaxShrinkRuns caps the sequences executed while shrinking (0: 5000).
	MaxShrinkRuns int
}

// Failure is a failing sequence, shrunk.
type Failure[Op any] struct {
	Seed     uint64
	Ops      []Op  // the shrunk sequence; its last operation fails
	Err      error // what went wrong at the last operation
	Original int   // length of the generated sequence up to the first failure
	Runs     int   // sequences executed while shrinking
}

// Generate returns the operations of seed's sequence.
func (c Check[S, Op]) Generate(seed uint64) []Op {
	r := NewRand(seed)
	ops := make([]Op, c.Ops)
	for i := range ops {
		ops[i] = c.Gen(r, i)
	}
	return ops
}

// Replay runs ops in lockstep on a fresh pair and returns the index of the
// first failing operation and its error, or -1 and nil. A panic in Step or
// Invariant counts as a failure of that operation.
func (c Check[S, Op]) Replay(ops []Op) (int, error) {
	s := c.New()
	for i, op := range ops {
		if err := c.step(s, op); err != nil {
			return i, err
		}
	}
	return -1, nil
}

func (c Check[S, Op]) step(s S, op Op) (err error) {
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

// RunSeed generates seed's sequence and runs it. If it fails, the failure is
// shrunk and returned; otherwise RunSeed returns nil.
func (c Check[S, Op]) RunSeed(seed uint64) *Failure[Op] {
	ops := c.Generate(seed)
	i, err := c.Replay(ops)
	if err == nil {
		return nil
	}
	f := &Failure[Op]{Seed: seed, Original: i + 1}
	f.Ops, f.Runs, f.Err = c.shrink(ops[:i+1], err)
	return f
}

// shrink returns a shorter or simpler sequence that still fails, and its
// error. ops must fail at its last operation. Passes, repeated until none
// makes progress:
//
//  1. drop chunks: halves, then quarters, ... down to single operations;
//  2. replace each operation with the first of its Shrink candidates that
//     still fails, repeatedly;
//  3. replace the whole sequence with the first ShrinkAll candidate that
//     still fails.
//
// After each success the sequence is cut just after its (new) failing
// operation. Every choice is a fixed order, so shrinking is deterministic.
// Any failure counts, not only the original one: the shrunk sequence may
// show a different bug, which is still a bug.
func (c Check[S, Op]) shrink(ops []Op, err error) ([]Op, int, error) {
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

// shrinkState shrinks one failing sequence. try reports whether cand fails;
// if it does, it becomes the sequence.
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
			cand := append(append([]Op(nil), s.ops[:i]...), s.ops[i+size:]...)
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

// Literal returns ops as a Go composite literal of type []c.Name.
func (c Check[S, Op]) Literal(ops []Op) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[]%s{\n", c.Name)
	for _, op := range ops {
		fmt.Fprintf(&b, "\t%s,\n", c.Format(op))
	}
	b.WriteString("}")
	return b.String()
}

// Report describes f: the seed, how to replay it, and the shrunk sequence.
func (c Check[S, Op]) Report(f *Failure[Op], replay string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %d: operation %d of the shrunk sequence failed: %v\n", f.Seed, len(f.Ops), f.Err)
	fmt.Fprintf(&b, "(shrunk from %d operations to %d in %d runs)\n", f.Original, len(f.Ops), f.Runs)
	if replay != "" {
		fmt.Fprintf(&b, "replay: %s\n", replay)
	}
	fmt.Fprintf(&b, "shrunk sequence:\n%s", c.Literal(f.Ops))
	return b.String()
}

// Options chooses the seeds Run runs. The caller maps its test flags onto it.
type Options struct {
	// Seeds runs seeds 1..Seeds.
	Seeds int
	// Seed, if >= 0, runs only this seed (overrides Seeds and Stop).
	Seed int64
	// Stop, if set, runs seeds 1, 2, ... until it returns true (overrides
	// Seeds), for wall-clock campaigns.
	Stop func() bool
	// Replay returns the command that replays one seed.
	Replay func(seed uint64) string
}

// Run runs c on the seeds o chooses and reports the first failure, shrunk.
func Run[S, Op any](t TB, c Check[S, Op], o Options) {
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
