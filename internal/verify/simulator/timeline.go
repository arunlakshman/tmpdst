package simulator

import "slices"

import "mds/internal/core"

// MemTimeline is the in-memory append-only Timeline: one log in append order
// plus a per-name index, so an invariant reading one fact name never scans the
// others. Only the simulator appends; invariants hold the read-only Timeline.
type MemTimeline struct {
	seq    uint64
	n      int
	byName map[string][]TimedFact
}

// NewTimeline returns an empty timeline.
func NewTimeline() *MemTimeline { return &MemTimeline{byName: map[string][]TimedFact{}} }

// Append stamps and records a fact, returning its sequence number (from 1).
func (t *MemTimeline) Append(source core.NodeInstance, at Time, f core.Fact) uint64 {
	t.seq++
	t.n++
	t.byName[f.Name] = append(t.byName[f.Name], TimedFact{Seq: t.seq, At: at, Source: source, Fact: f})
	return t.seq
}

// Len is the total number of facts of every name.
func (t *MemTimeline) Len() int { return t.n }

// Since returns the facts named name appended after c and advances c. Use one
// Cursor per name.
func (t *MemTimeline) Since(name string, c *Cursor) []TimedFact {
	l := t.byName[name]
	if c.next >= len(l) {
		return nil
	}
	out := l[c.next:len(l):len(l)]
	c.next = len(l)
	return out
}

// Snapshot returns every fact named name so far. The caller must not modify it.
func (t *MemTimeline) Snapshot(name string) []TimedFact {
	l := t.byName[name]
	return slices.Clip(l)
}

var _ Timeline = (*MemTimeline)(nil)
