// Package assert records properties (Always, Sometimes, Reachable) without
// panicking, so a run keeps going after a failure.
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
	KindAlways    Kind = "always"
	KindSometimes Kind = "sometimes"
	KindReachable Kind = "reachable"
)

// Pair is one ordered context field for a failure detail.
type Pair struct {
	Key   string
	Value any
}

// Coverage counts evaluations of one property.
type Coverage struct {
	Kind   Kind
	Checks uint64
	Passes uint64
}

// Log records and continues. A false Always calls onFail and the run keeps going.
// Invariant: assertions never panic.
type Log struct {
	onFail func(property, detail string)
	stats  map[Property]*Coverage
}

// New returns a log that reports failures through onFail. onFail may be nil.
func New(onFail func(property, detail string)) *Log {
	return &Log{onFail: onFail, stats: map[Property]*Coverage{}}
}

func (l *Log) fail(property, detail string) {
	if l.onFail != nil {
		l.onFail(property, detail)
	}
}

func (l *Log) stat(p Property, kind Kind) *Coverage {
	s := l.stats[p]
	if s == nil {
		s = &Coverage{Kind: kind}
		l.stats[p] = s
	}
	return s
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
	s := l.stat(p, KindAlways)
	s.Checks++
	if cond {
		s.Passes++
		return
	}
	l.fail(string(p), detailText(details))
}

// AlwaysOrUnreachable is Always under the same property.
func (l *Log) AlwaysOrUnreachable(cond bool, p Property, details ...Pair) {
	l.Always(cond, p, details...)
}

// Sometimes records whether cond held without failing the run.
func (l *Log) Sometimes(cond bool, p Property) {
	s := l.stat(p, KindSometimes)
	s.Checks++
	if cond {
		s.Passes++
	}
}

// Reachable records that p was reached.
func (l *Log) Reachable(p Property) {
	s := l.stat(p, KindReachable)
	s.Checks++
	s.Passes++
}

// Unreachable reports that an unreachable point was hit.
func (l *Log) Unreachable(p Property, details ...Pair) {
	l.fail(string(p), "reached an Unreachable point "+detailText(details))
}

// Coverage returns a copy of the per-property stats.
func (l *Log) Coverage() map[string]Coverage {
	out := make(map[string]Coverage, len(l.stats))
	//determinismcheck:ok copies into another map; iteration order does not escape
	for p, s := range l.stats {
		out[string(p)] = *s
	}
	return out
}
