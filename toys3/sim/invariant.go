package sim

import "manifester/toys3/core"

// TimedFact is a core.Fact stamped by the simulator.
type TimedFact struct {
	Step int
	At   core.Time
	Inc  Incarnation
	Fact core.Fact
}

// View is the read-only window invariants get onto a running simulation.
type View interface {
	Step() int
	Now() core.Time
	Alive() bool // server is booted (it may still be recovering)
	Inc() Incarnation
	Quiet() bool // in QuietTail: no faults, no crashes
	FS() *FakeFS
	History() *History
	// Facts emitted so far, in order. Invariants remember how far they read.
	Facts() []TimedFact
}

// Invariant is checked after every simulator step and once at the end of the
// run. Checking every step (not just at the end) reports a bug at the step
// that caused it, which is what makes the event log readable.
type Invariant interface {
	Name() string
	AfterStep(v View) []Failure
	AtEnd(v View) []Failure
}

// DefaultInvariants returns fresh instances of every invariant below.
func DefaultInvariants() []Invariant {
	return []Invariant{&NoCorruptReads{}, &AckedIsDurable{}, &Linearizable{}, &Liveness{}}
}

// NoCorruptReads is invariant (c): every 200 GET body is exactly a value some
// PUT wrote (ValueWriter accepts it). A torn or corrupt object served with 200
// fails here immediately, even before the linearizability check.
type NoCorruptReads struct{ next int }

// AckedIsDurable checks each core.Committed fact against FakeFS.Durable() on
// the step it is emitted: the key's durable file must exist and decode to a
// payload with the committed checksum (or be absent for a delete). This is
// (b) checked eagerly: a missing fsync fails here without needing a crash.
type AckedIsDurable struct{ next int }

// Linearizable is invariants (a) and (b): it runs Oracle.CheckQuiescent at
// every quiescent point and at the end.
type Linearizable struct{}

// Liveness is invariant (d): at the end of QuietTail (server alive, no
// faults) no op is Pending, and every boot reached core.Recovered.
type Liveness struct{}

func (*NoCorruptReads) Name() string               { return "no-corrupt-reads" }
func (*NoCorruptReads) AfterStep(v View) []Failure { panic("unimplemented (phase 2)") }
func (*NoCorruptReads) AtEnd(v View) []Failure     { return nil }

func (*AckedIsDurable) Name() string               { return "acked-is-durable" }
func (*AckedIsDurable) AfterStep(v View) []Failure { panic("unimplemented (phase 2)") }
func (*AckedIsDurable) AtEnd(v View) []Failure     { return nil }

func (*Linearizable) Name() string               { return "linearizable" }
func (*Linearizable) AfterStep(v View) []Failure { panic("unimplemented (phase 2)") }
func (*Linearizable) AtEnd(v View) []Failure     { panic("unimplemented (phase 2)") }

func (*Liveness) Name() string               { return "liveness" }
func (*Liveness) AfterStep(v View) []Failure { return nil }
func (*Liveness) AtEnd(v View) []Failure     { panic("unimplemented (phase 2)") }
