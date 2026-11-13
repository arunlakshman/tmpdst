package kernel

import (
	"slices"

	"github.com/manifesterio/mds/dst/machine"
)

// Timeline is the append-only list of facts of a run: one log in append order
// plus an index per name, so a rule that reads one fact name never scans the
// others. In a run only the kernel appends.
type Timeline struct {
	seq    uint64
	n      int
	byName map[string][]TimedFact
}

// Cursor remembers how far a reader has read one fact name. Use one Cursor
// per name.
type Cursor struct{ next int }

// TimedFact is one fact on the timeline.
type TimedFact struct {
	Seq    uint64
	At     Time
	Source machine.NodeInstance // zero for kernel, service and host facts
	Fact   machine.Fact
}

// NewTimeline returns an empty timeline.
func NewTimeline() *Timeline { return &Timeline{byName: map[string][]TimedFact{}} }

// Append stamps and records a fact and returns its sequence number (from 1).
// In a run only the kernel calls it; tests call it to build a timeline by
// hand.
func (t *Timeline) Append(source machine.NodeInstance, at Time, f machine.Fact) uint64 {
	t.seq++
	t.n++
	t.byName[f.Name] = append(t.byName[f.Name], TimedFact{Seq: t.seq, At: at, Source: source, Fact: f})
	return t.seq
}

// Len is the number of facts of every name.
func (t *Timeline) Len() int { return t.n }

// Since returns the facts named name that were appended after c, and moves c
// to the end.
func (t *Timeline) Since(name string, c *Cursor) []TimedFact {
	l := t.byName[name]
	if c.next >= len(l) {
		return nil
	}
	out := l[c.next:len(l):len(l)]
	c.next = len(l)
	return out
}

// Snapshot returns every fact named name so far. Do not change it.
func (t *Timeline) Snapshot(name string) []TimedFact {
	return slices.Clip(t.byName[name])
}
