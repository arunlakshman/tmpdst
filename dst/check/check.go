// Package check is the spec toolkit of dst. This package holds the parts that
// may run inside a simulation. Its subpackage spectest holds the parts that
// run in tests on rapid (Spec, Lockstep, SelfCheck, planted bugs, Rerun).
//
// The log oracle of a simulator: replay a log into a reference model and
// judge, one writer per epoch, no hole in the log, per-slot state agreement,
// monotonic floors, and the convergence predicate. Every check records
// through assert.Log, so a failure never panics and the run goes on.
//
// The crash checks by hand (Persistence, ForwardProgress, DirtyReboot), for a
// test that does not use spectest.Lockstep.
//
// The package knows no product types. A product gives it a Log (what its log
// fake exposes), callbacks that decode and apply entries, and the reference
// model. Bookkeeping that is specific to one shape of oracle, such as sorting
// reads by the log prefix they saw, stays in the product.
package check

import (
	"cmp"
	"fmt"

	"github.com/manifesterio/mds/dst/assert"
)

// Slot is a position in the log.
type Slot = int64

// Log is what a log fake must expose to the checks. History is the first
// committed entry per slot, so a lost reply or a duplicate append can not
// change it.
type Log interface {
	// End returns the number of slots from 0 up that have an entry, and
	// whether a slot exists beyond a gap (a hole).
	End() (end Slot, hole bool)
	// Entry returns the committed entry of slot, which must be below End.
	Entry(slot Slot) []byte
	// QuietAfter reports that no entry from slot `from` to End changes state
	// (a heartbeat or a fence does not). A node that has applied up to `from`
	// then has the final state.
	QuietAfter(from Slot) bool
}

// Checker records verdicts as properties named Prefix + name.
type Checker struct {
	Log    *assert.Log
	Prefix string
}

// Verdict records a check and returns ok, so the caller can skip work after a
// failure. The detail is formatted only when the check fails.
func (c Checker) Verdict(ok bool, property, format string, args ...any) bool {
	p := assert.Property(c.Prefix + property)
	if ok {
		c.Log.Always(true, p)
		return true
	}
	c.Log.Always(false, p, assert.Pair{Key: "detail", Value: fmt.Sprintf(format, args...)})
	return false
}

// NoHole checks that the log has no slot missing below its last slot.
func (c Checker) NoHole(l Log) {
	end, hole := l.End()
	c.Verdict(!hole, "log has no hole", "the log history has a slot missing below its last slot (contiguous up to %d)", end)
}

// Replay applies the log to spec, slot by slot, in log order. decode turns one
// entry into ops, apply runs one op on spec, and judge is called with prefix 0
// before the first slot and with slot+1 after each slot. Replay stops and
// returns false if an entry does not decode.
func Replay[Op, Spec any](c Checker, l Log, spec Spec,
	decode func(entry []byte) ([]Op, error),
	apply func(spec Spec, op Op),
	judge func(prefix Slot),
) bool {
	end, _ := l.End()
	judge(0)
	for slot := range end {
		ops, err := decode(l.Entry(slot))
		if !c.Verdict(err == nil, "log entries decode", "slot %d: %v", slot, err) {
			return false
		}
		for _, op := range ops {
			apply(spec, op)
		}
		judge(slot + 1)
	}
	return true
}

// Holders remembers who first held each epoch. The zero value is ready.
type Holders[W comparable] struct{ first map[uint64]W }

// Note records that who holds epoch. It returns the first holder, and ok=false
// when that is somebody else (two writers in one epoch).
func (h *Holders[W]) Note(epoch uint64, who W) (W, bool) {
	if h.first == nil {
		h.first = map[uint64]W{}
	}
	if first, seen := h.first[epoch]; seen {
		return first, first == who
	}
	h.first[epoch] = who
	return who, true
}

// Prefixes remembers the first state hash seen after each number of applied
// slots, to check that replicas agree on every slot. The zero value is ready.
type Prefixes[N any] struct {
	first map[Slot]seen[N]
}

type seen[N any] struct {
	hash uint64
	who  N
}

// Note records the state hash of who after `applied` slots. It returns the
// first hash and who showed it, and ok=false if that hash differs.
func (p *Prefixes[N]) Note(applied Slot, hash uint64, who N) (uint64, N, bool) {
	if p.first == nil {
		p.first = map[Slot]seen[N]{}
	}
	if prev, found := p.first[applied]; found {
		return prev.hash, prev.who, prev.hash == hash
	}
	p.first[applied] = seen[N]{hash: hash, who: who}
	return hash, who, true
}

// Monotonic holds lower bounds that only rise: what clients were told, so a
// later read may not show less. The zero value is ready.
type Monotonic[K comparable, V cmp.Ordered] struct{ m map[K]V }

// Raise lifts the floor of k to at least v and returns the new floor.
func (f *Monotonic[K, V]) Raise(k K, v V) V {
	if f.m == nil {
		f.m = map[K]V{}
	}
	f.m[k] = max(f.m[k], v)
	return f.m[k]
}

// Floor returns the floor of k, or the zero value.
func (f *Monotonic[K, V]) Floor(k K) V { return f.m[k] }

// AtLeast reports that got does not fall below the floor of k.
func (f *Monotonic[K, V]) AtLeast(k K, got V) bool { return got >= f.m[k] }

// Node is the state of one replica for the convergence predicate.
type Node struct {
	Up      bool
	Ready   bool
	Applied Slot
}

// Converged reports that every node is up, ready, and has applied everything
// in the log that changes state.
func Converged(l Log, nodes []Node) bool {
	for _, n := range nodes {
		if !n.Up || !n.Ready || !l.QuietAfter(n.Applied) {
			return false
		}
	}
	return true
}

// AllEqual reports that all values are equal, for example the state hashes of
// all live replicas. An empty list is equal.
func AllEqual[T comparable](vals []T) bool {
	for _, v := range vals[min(1, len(vals)):] {
		if v != vals[0] {
			return false
		}
	}
	return true
}
