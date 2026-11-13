// Package assert records properties (Always, Sometimes, Reachable, ...) without
// panicking, so a run keeps going after a failure.
//
// There are two ways to name a property:
//
//   - A typed handle, declared once in a package-level var block, for example
//     var oneWriter = assert.NewAlways("sim: one writer per epoch").
//     The declaration puts the name in the catalog (see Catalog). New code uses
//     handles.
//   - A plain Property string. Only check.Checker uses it, because its names are
//     built at run time (a prefix plus a name). The Log methods that take a
//     Property are used by the kernel for handles too, so they stay (T1.16b).
package assert

import (
	"fmt"
	"strings"
)

// Property is a stable assertion id. Do not interpolate dynamic values into it.
type Property string

// Kind classifies how a property was recorded.
type Kind string

// The kinds of property a Log records.
const (
	KindAlways              Kind = "always"
	KindAlwaysOrUnreachable Kind = "always_or_unreachable"
	KindSometimes           Kind = "sometimes"
	KindReachable           Kind = "reachable"
	KindUnreachable         Kind = "unreachable"
)

// isSafety reports whether a kind fails the run when it is false (Always,
// AlwaysOrUnreachable, Unreachable). The other kinds count coverage.
func isSafety(k Kind) bool {
	return k == KindAlways || k == KindAlwaysOrUnreachable || k == KindUnreachable
}

// Pair is one ordered context field for a failure detail.
type Pair struct {
	Key   string
	Value any
}

// Coverage counts evaluations of one property.
//
// FirstStep is the step of the first interesting evaluation: a safety check
// that failed, or a coverage check that passed. It is 0 when there was none,
// or when it happened before the first step. The step comes from the counter
// given to SetStepCounter.
type Coverage struct {
	Kind      Kind
	Checks    uint64
	Passes    uint64
	FirstStep uint64
}

// Log records and continues. A false Always calls onFail and the run keeps going.
// Invariant: assertions never panic.
type Log struct {
	onFail func(property, detail string)
	step   func() uint64
	stats  map[Property]*coverage
}

// coverage is Coverage plus a flag that says FirstStep is set.
type coverage struct {
	Coverage
	hit bool
}

// New returns a log that reports failures through onFail. onFail may be nil.
func New(onFail func(property, detail string)) *Log {
	return &Log{onFail: onFail, stats: map[Property]*coverage{}}
}

// SetStepCounter gives the log the step counter of the run, for
// Coverage.FirstStep. Without it, FirstStep stays 0.
func (l *Log) SetStepCounter(step func() uint64) { l.step = step }

func (l *Log) fail(property, detail string) {
	if l.onFail != nil {
		l.onFail(property, detail)
	}
}

// record counts one evaluation. ok is the result of the check.
func (l *Log) record(p Property, kind Kind, ok bool) {
	s := l.stats[p]
	if s == nil {
		s = &coverage{Coverage: Coverage{Kind: kind}}
		l.stats[p] = s
	}
	s.Checks++
	if ok {
		s.Passes++
	}
	if ok != isSafety(kind) && !s.hit {
		s.hit = true
		if l.step != nil {
			s.FirstStep = l.step()
		}
	}
}

func detailText(details []Pair) string {
	var b strings.Builder
	for _, d := range details {
		fmt.Fprintf(&b, " %s=%v", d.Key, d.Value)
	}
	return strings.TrimSpace(b.String())
}

// Always records cond under p. A false cond calls onFail.
func (l *Log) Always(cond bool, p Property, details ...Pair) {
	l.record(p, KindAlways, cond)
	if !cond {
		l.fail(string(p), detailText(details))
	}
}

// AlwaysOrUnreachable is like Always, but it is recorded with its own kind:
// it is fine if the code never reaches it.
func (l *Log) AlwaysOrUnreachable(cond bool, p Property, details ...Pair) {
	l.record(p, KindAlwaysOrUnreachable, cond)
	if !cond {
		l.fail(string(p), detailText(details))
	}
}

// Sometimes records whether cond held without failing the run.
func (l *Log) Sometimes(cond bool, p Property) {
	l.record(p, KindSometimes, cond)
}

// Reachable records that p was reached.
func (l *Log) Reachable(p Property) {
	l.record(p, KindReachable, true)
}

// Unreachable reports that an unreachable point was hit. It is counted as a
// check that did not pass.
func (l *Log) Unreachable(p Property, details ...Pair) {
	l.record(p, KindUnreachable, false)
	l.fail(string(p), "reached an Unreachable point "+detailText(details))
}

// Coverage returns a copy of the per-property stats.
func (l *Log) Coverage() map[string]Coverage {
	out := make(map[string]Coverage, len(l.stats))
	//determinismcheck:ok copies into another map; iteration order does not escape
	for p, s := range l.stats {
		out[string(p)] = s.Coverage
	}
	return out
}
