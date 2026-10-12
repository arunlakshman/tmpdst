package sim

import (
	"fmt"
	"sort"
	"strings"

	"manifester/toys3/control"
	"manifester/toys3/format"
)

// TimedFact is a control.Fact stamped by the simulator.
type TimedFact struct {
	Step int
	At   control.Time
	Inc  Incarnation
	Fact control.Fact
}

// View is the read-only window invariants get onto a running simulation.
type View interface {
	Step() int
	Now() control.Time
	Alive() bool // server is booted (it may still be recovering)
	Inc() Incarnation
	Quiet() bool // in QuietTail: no faults, no crashes
	FS() *FakeFS
	Root() string // the server's data directory (control.Config.Dir)
	Oracle() *Oracle
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
	return []Invariant{&NoCorruptReads{}, &AckedIsDurable{}, &ObjNeverTorn{}, &Linearizable{}, &Liveness{}}
}

// BlackBoxInvariants returns only the invariants that judge what clients
// see (no peeking at the fake filesystem's durable state). Useful to learn
// which bugs are visible end to end, and which only to a ground-truth check.
func BlackBoxInvariants() []Invariant {
	return []Invariant{&NoCorruptReads{}, &Linearizable{}, &Liveness{}}
}

// NoCorruptReads is invariant (c): every 200 GET body is exactly a value some
// PUT wrote (ValueWriter accepts it). A torn or corrupt object served with 200
// fails here immediately, even before the linearizability check.
type NoCorruptReads struct{ next int }

// AckedIsDurable checks each control.Committed fact against FakeFS.Durable() on
// the step it is emitted: the key's durable file must exist and decode to a
// payload with the committed checksum (or be absent for a delete). This is
// (b) checked eagerly: a missing fsync fails here without needing a crash.
type AckedIsDurable struct{ next int }

// ObjNeverTorn checks, after every step, that every file under obj/ in
// FakeFS.Durable() is a complete object (header, length and CRC valid). The
// protocol renames only fsynced files into obj/, so even the most adversarial
// crash leaves obj/ clean; the object CRC is a safety net, not a mechanism.
type ObjNeverTorn struct{ gen uint64 }

// Linearizable is invariants (a) and (b): it runs Oracle.CheckQuiescent at
// every quiescent point and at the end.
type Linearizable struct{}

// Liveness is invariant (d): at the end of QuietTail (server alive, no
// faults) no op is Pending, and every boot reached control.Recovered.
// Additionally, a request issued during QuietTail never gets a 500: with no
// faults injected a correct server has no reason to fail (this is what makes
// a lost-but-acked object visible to clients: it reads back as 500 forever).
type Liveness struct {
	next      int
	quietFrom int // index of the first op invoked in QuietTail (once quietSeen)
	quietSeen bool
}

func (*NoCorruptReads) Name() string { return "no-corrupt-reads" }
func (n *NoCorruptReads) AfterStep(v View) []Failure {
	h := v.History()
	var fails []Failure
	for ; n.next < len(h.Completed); n.next++ {
		op := &h.Ops[h.Completed[n.next]]
		if _, isGet := op.Op.(control.Get); !isGet || op.Status != control.StatusOK {
			continue
		}
		w, ok := ValueWriter(op.Body)
		if !ok {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned bytes no PUT wrote: %s", describeOp(op))})
			continue
		}
		i := sort.Search(len(h.Ops), func(i int) bool { return h.Ops[i].Req >= w })
		if i == len(h.Ops) || h.Ops[i].Req != w {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned the value of unknown request %d: %s", w, describeOp(op))})
			continue
		}
		if _, isPut := h.Ops[i].Op.(control.Put); !isPut {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned a value attributed to non-PUT %s", describeOp(&h.Ops[i]))})
		}
	}
	return fails
}
func (*NoCorruptReads) AtEnd(v View) []Failure { return nil }

func (*AckedIsDurable) Name() string { return "acked-is-durable" }
func (a *AckedIsDurable) AfterStep(v View) []Failure {
	facts := v.Facts()
	var fails []Failure
	for ; a.next < len(facts); a.next++ {
		c, ok := facts[a.next].Fact.(control.Committed)
		if !ok {
			continue
		}
		path := format.ObjPath(v.Root(), c.Key)
		data, present := v.FS().DurableFile(path)
		switch {
		case c.Deleted && present:
			fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: DELETE %q acked but %s is still durable (%s)", c.Client, c.Key, path, describePayload(data))})
		case c.Deleted:
		case !present:
			fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but %s has no durable entry", c.Client, c.Key, path)})
		default:
			payload, valid := format.DecodeObject(data)
			if !valid {
				fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but durable %s is torn (%d bytes)", c.Client, c.Key, path, len(data))})
			} else if format.Checksum(payload) != c.Sum {
				fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but durable %s holds %s", c.Client, c.Key, path, describePayload(data))})
			}
		}
	}
	return fails
}
func (*AckedIsDurable) AtEnd(v View) []Failure { return nil }

func (*ObjNeverTorn) Name() string { return "obj-never-torn" }
func (o *ObjNeverTorn) AfterStep(v View) []Failure {
	g := v.FS().DurableGen()
	if g == o.gen {
		return nil // nothing durable changed since the last check
	}
	o.gen = g
	prefix := format.ObjDir(v.Root()) + "/"
	var fails []Failure
	for _, f := range v.FS().Durable().Files {
		if !strings.HasPrefix(f.Path, prefix) {
			continue
		}
		if _, ok := format.DecodeObject(f.Data); !ok {
			fails = append(fails, Failure{Msg: fmt.Sprintf("durable %s is torn or corrupt (%d bytes)", f.Path, len(f.Data))})
		}
	}
	return fails
}
func (*ObjNeverTorn) AtEnd(v View) []Failure { return nil }

func (*Linearizable) Name() string { return "linearizable" }
func (*Linearizable) AfterStep(v View) []Failure {
	o := v.Oracle()
	if !o.Quiescent() || o.Unchecked() == 0 {
		return nil
	}
	return o.CheckQuiescent()
}
func (*Linearizable) AtEnd(v View) []Failure { return v.Oracle().CheckFinal() }

func (*Liveness) Name() string { return "liveness" }
func (l *Liveness) AfterStep(v View) []Failure {
	h := v.History()
	if v.Quiet() && !l.quietSeen {
		l.quietSeen, l.quietFrom = true, len(h.Ops)
	}
	var fails []Failure
	for ; l.next < len(h.Completed); l.next++ {
		i := h.Completed[l.next]
		op := &h.Ops[i]
		if l.quietSeen && i >= l.quietFrom && op.Status == control.StatusServerError {
			fails = append(fails, Failure{Msg: "500 with no fault injected (QuietTail): " + describeOp(op)})
		}
	}
	return fails
}
func (*Liveness) AtEnd(v View) []Failure {
	var fails []Failure
	if !v.Alive() {
		fails = append(fails, Failure{Msg: "server not alive at the end of the run"})
	}
	recovered := false
	for _, f := range v.Facts() {
		if _, ok := f.Fact.(control.Recovered); ok && f.Inc == v.Inc() {
			recovered = true
		}
	}
	if !recovered {
		fails = append(fails, Failure{Msg: fmt.Sprintf("incarnation %d never emitted Recovered", v.Inc())})
	}
	h := v.History()
	for i := range h.Ops {
		if h.Ops[i].Outcome == Pending {
			fails = append(fails, Failure{Msg: "no reply by the end of QuietTail: " + describeOp(&h.Ops[i])})
		}
	}
	return fails
}
