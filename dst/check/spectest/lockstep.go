package spectest

import (
	"fmt"
	"strings"

	"pgregory.net/rapid"
)

// Reboots are the restart ops of a subject. A nil func is never drawn.
//
// After each of them the subject must still answer like the spec: a clean
// reboot and a crash both keep every answered write (Bornholt et al., SOSP
// 2021: forward progress and persistence).
type Reboots struct {
	// Reboot is a clean restart.
	Reboot func()
	// DirtyReboot is a crash. keep says what the crash leaves of the
	// checkpoints: 0 all, 1 all but the newest, 2 none.
	DirtyReboot func(keep int)
	// Checkpoint waits until a checkpoint covers everything written so far.
	Checkpoint func()
	// MaxKeep is the largest keep that DirtyReboot is called with. Lockstep
	// draws keep from 0 to MaxKeep.
	MaxKeep int
}

// StepKind says what one step of a Lockstep run does.
type StepKind int

// The kinds of step. KindOp is first, so rapid shrinks a step toward a plain op.
const (
	KindOp          StepKind = iota // run Op on the spec and on the subject
	KindReboot                      // Reboots.Reboot
	KindDirtyReboot                 // Reboots.DirtyReboot(Keep)
	KindCheckpoint                  // Reboots.Checkpoint
)

// Step is one step of a Lockstep run. Op is set only for KindOp, and Keep only
// for KindDirtyReboot.
type Step[Op any] struct {
	Kind StepKind
	Op   Op
	Keep int
}

// Lockstep draws a list of steps with rapid. It runs each op on a new spec and
// on a new subject, and compares the results. When the subject has a state
// hash (HasStateHash), it also compares the state hashes. Shrinking is done
// by rapid.
//
// S is the Go type of the subject. It is a type parameter so that Invariant,
// Reboots and HasFailed get the subject of the current run with its real type.
type Lockstep[Op, Res any, S Subject[Op, Res]] struct {
	// Name is the Go type of Op, used in the Go code of a failure, for example
	// "kv.Op". Empty: the name that fmt prints for the type.
	Name string
	// NewSpec makes a new spec. It is called once for each run.
	NewSpec func() Spec[Op, Res]
	// NewSubject makes a new subject. It is called once for each run.
	NewSubject func(t *rapid.T) S
	// Ops draws one op. Build it with SimplestFirst.
	Ops *rapid.Generator[Op]
	// Equal compares the spec result (want) with the subject result (got).
	Equal func(want, got Res) error
	// Invariant checks the subject after every step. Nil: no check.
	Invariant func(s S) error
	// Reboots returns the restart ops of s. Nil: the subject has none.
	Reboots func(s S) Reboots
	// HasFailed reports that a failure was injected into s. After that, an
	// error from the subject is allowed and the state hashes are not
	// compared, but a wrong answer is still a failure. This is the "has
	// failed" rule of the paper. Nil: never.
	HasFailed func(s S) bool
	// Format prints one op as Go code. Nil: fmt "%#v".
	Format func(op Op) string
	// Rerun builds the command that runs a failing seed again.
	Rerun Rerun
}

// run is the state of one Lockstep run.
type run[Op, Res any, S Subject[Op, Res]] struct {
	l       *Lockstep[Op, Res, S]
	t       *rapid.T
	spec    Spec[Op, Res]
	subject S
	reboots Reboots
	extra   []StepKind // the reboot kinds this subject has
	steps   *[]Step[Op]
	message *string // the first failure message; nil: not kept
}

// Steps is the body of one rapid check. Use it when the caller owns
// rapid.Check. Check is simpler and prints a better failure message.
func (l Lockstep[Op, Res, S]) Steps(t *rapid.T) {
	var steps []Step[Op]
	l.steps(t, &steps, nil)
}

// steps runs one rapid check. It writes every step it takes into rec, and the
// first failure message into message (when message is not nil).
func (l *Lockstep[Op, Res, S]) steps(t *rapid.T, rec *[]Step[Op], message *string) {
	*rec = (*rec)[:0]
	r := &run[Op, Res, S]{l: l, t: t, spec: l.NewSpec(), subject: l.NewSubject(t), steps: rec, message: message}
	if l.Reboots != nil {
		r.reboots = l.Reboots(r.subject)
	}
	if r.reboots.Reboot != nil {
		r.extra = append(r.extra, KindReboot)
	}
	if r.reboots.DirtyReboot != nil {
		r.extra = append(r.extra, KindDirtyReboot)
	}
	if r.reboots.Checkpoint != nil {
		r.extra = append(r.extra, KindCheckpoint)
	}
	t.Repeat(map[string]func(*rapid.T){"step": func(*rapid.T) { r.step() }})
}

// opWeight is how many plain ops are drawn for one of each reboot kind.
const opWeight = 10

// step draws one step and runs it.
func (r *run[Op, Res, S]) step() {
	s := Step[Op]{Kind: KindOp}
	if len(r.extra) > 0 {
		// Values below opWeight are a plain op. Rapid shrinks toward 0, so a
		// shrunk list has as few reboots as it can.
		if k := rapid.IntRange(0, opWeight+len(r.extra)-1).Draw(r.t, "kind"); k >= opWeight {
			s.Kind = r.extra[k-opWeight]
		}
	}
	switch s.Kind {
	case KindOp:
		s.Op = r.l.Ops.Draw(r.t, "op")
	case KindDirtyReboot:
		s.Keep = rapid.IntRange(0, max(0, r.reboots.MaxKeep)).Draw(r.t, "keep")
	case KindReboot, KindCheckpoint:
	}
	*r.steps = append(*r.steps, s)
	n := len(*r.steps) - 1
	switch s.Kind {
	case KindOp:
		r.doOp(n, s.Op)
	case KindReboot:
		r.reboots.Reboot()
		r.compareHash(n, "after a clean reboot")
	case KindDirtyReboot:
		r.reboots.DirtyReboot(s.Keep)
		r.compareHash(n, "after a crash")
	case KindCheckpoint:
		r.reboots.Checkpoint()
	}
	if r.l.Invariant != nil {
		if err := r.l.Invariant(r.subject); err != nil {
			r.fatalf("step %d: invariant broken: %v", n, err)
		}
	}
}

// fatalf keeps the message and stops the check.
func (r *run[Op, Res, S]) fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if r.message != nil && *r.message == "" {
		*r.message = msg
	}
	r.t.Fatal(msg)
}

// failed reports the "has failed" flag of the subject.
func (r *run[Op, Res, S]) failed() bool {
	return r.l.HasFailed != nil && r.l.HasFailed(r.subject)
}

// doOp runs op on the spec and on the subject and compares the results.
func (r *run[Op, Res, S]) doOp(n int, op Op) {
	want := r.spec.Apply(op)
	got, err := r.subject.Apply(op)
	if err != nil {
		if r.failed() {
			return
		}
		r.fatalf("step %d: %s: the subject gave no answer: %v", n, r.l.format(op), err)
		return
	}
	if err := r.l.Equal(want, got); err != nil {
		r.fatalf("step %d: %s: the subject answer is not the spec answer: %v\n  spec:    %+v\n  subject: %+v",
			n, r.l.format(op), err, want, got)
		return
	}
	r.compareHash(n, "after "+r.l.format(op))
}

// compareHash compares the state hashes when the subject has one and no
// failure was injected.
func (r *run[Op, Res, S]) compareHash(n int, when string) {
	h, ok := any(r.subject).(HasStateHash)
	if !ok || r.failed() {
		return
	}
	if want, got := r.spec.StateHash(), h.StateHash(); want != got {
		r.fatalf("step %d: state hash %s: spec %#x, subject %#x", n, when, want, got)
	}
}

// format prints one op as Go code.
func (l *Lockstep[Op, Res, S]) format(op Op) string {
	if l.Format != nil {
		return l.Format(op)
	}
	return fmt.Sprintf("%#v", op)
}

// typeName returns the Go type name of Op.
func (l *Lockstep[Op, Res, S]) typeName() string {
	if l.Name != "" {
		return l.Name
	}
	// A nil pointer to Op prints its type even when Op is an interface.
	return strings.TrimPrefix(fmt.Sprintf("%T", (*Op)(nil)), "*")
}

// GoCode prints steps as a Go slice of ops. A reboot step is not an op, so
// it is printed as a comment in its place.
func (l Lockstep[Op, Res, S]) GoCode(steps []Step[Op]) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[]%s{\n", l.typeName())
	for _, s := range steps {
		switch s.Kind {
		case KindOp:
			fmt.Fprintf(&b, "\t%s,\n", l.format(s.Op))
		case KindReboot:
			b.WriteString("\t// Reboot()\n")
		case KindDirtyReboot:
			fmt.Fprintf(&b, "\t// DirtyReboot(%d)\n", s.Keep)
		case KindCheckpoint:
			b.WriteString("\t// Checkpoint()\n")
		}
	}
	b.WriteString("}")
	return b.String()
}

// Failure is a failed Lockstep run, after rapid shrank it.
type Failure[Op any] struct {
	// Seed is the rapid seed that found the failure. 0: not known (for
	// example, rapid replayed a fail file).
	Seed uint64
	// Steps is the shrunk list of steps. Its last step fails.
	Steps []Step[Op]
	// Message says what went wrong.
	Message string
}

// Report prints f: what went wrong, the seed, the shrunk steps as Go code,
// and the command that runs it again. test is the full test name (t.Name()).
func (l Lockstep[Op, Res, S]) Report(f *Failure[Op], test string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "lockstep failed: %s\n", f.Message)
	if f.Seed == 0 {
		b.WriteString("seed: not known (rapid replayed a fail file)\n")
	} else {
		fmt.Fprintf(&b, "seed: %d\n", f.Seed)
	}
	fmt.Fprintf(&b, "shrunk steps (%d), as Go code:\n%s\n", len(f.Steps), l.GoCode(f.Steps))
	if f.Seed != 0 {
		fmt.Fprintf(&b, "run it again (in the package folder): %s\n", l.Rerun.Line(f.Seed, test))
	}
	return b.String()
}
