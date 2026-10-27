package logsm

import (
	"context"

	"manifester/core"
)

// Kind is the type of a log entry.
type Kind uint8

// Entry kinds. KindNoop is written by a hole-filler and changes nothing.
const (
	KindNoop Kind = iota
	KindCommit
	KindCreateTopic
)

// Entry is the value of one slot: a command, never a computed result.
type Entry struct {
	// ID names this append attempt (writer and counter). It lets a writer
	// whose answer was lost recognise its own entry; it is not state.
	ID     string
	Kind   Kind
	Commit core.CommitSegment // KindCommit
	Topic  string             // KindCreateTopic
	Parts  int32              // KindCreateTopic
}

// Slot is an entry with its position.
type Slot struct {
	N core.LogSlot
	E Entry
}

// Log is a shared log of single-write slots (a loglet). A written slot never
// changes.
type Log interface {
	// Append writes e at slot if the slot is empty. cur is the entry in the
	// slot afterwards (e itself when won, the existing one when not). A
	// non-nil err means the outcome is unknown: the write may have landed.
	Append(ctx context.Context, slot int64, e Entry) (cur Entry, won bool, err error)
	// Read returns up to limit written slots >= from in ascending order. Empty
	// slots are skipped, so the result may have holes. The read is consistent.
	Read(ctx context.Context, from int64, limit int) ([]Slot, error)
	// CheckTail returns 1 + the highest written slot (0 for an empty log),
	// consistently.
	CheckTail(ctx context.Context) (int64, error)
}
