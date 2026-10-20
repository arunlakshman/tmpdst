package logcp

import (
	"context"
	"sync"
)

// Log is a shared log of single-write slots (a loglet). A written slot never
// changes.
type Log interface {
	// Append writes e at slot if the slot is empty. cur is the entry in the
	// slot afterwards (e itself when won, the existing one when not). A
	// non-nil err means the outcome is unknown: the write may have landed.
	Append(ctx context.Context, slot int64, e Entry) (cur Entry, won bool, err error)
	// Read returns up to limit written slots >= from in ascending order,
	// consistently. Empty slots are skipped. On a decode error it returns the
	// slots before the bad one together with the error.
	Read(ctx context.Context, from int64, limit int) ([]Slot, error)
	// CheckTail returns 1 + the highest written slot (0 for an empty log),
	// consistently.
	CheckTail(ctx context.Context) (int64, error)
}

// Trimmer is a Log that can drop slots below a position.
type Trimmer interface {
	// TrimBelow replaces the written slots < slot with tombstones and returns
	// how many it trimmed.
	TrimBelow(ctx context.Context, slot int64) (int, error)
}

// MemLog is the in-memory Log; slots are stored encoded, so it exercises the
// codec. Slots may be sparse. It is the Log of unit tests and the seeded harness.
type MemLog struct {
	mu    sync.Mutex
	slots map[int64][]byte
	tail  int64
}

// NewMemLog returns an empty log.
func NewMemLog() *MemLog { return &MemLog{slots: map[int64][]byte{}} }

// Append implements Log.
func (m *MemLog) Append(_ context.Context, slot int64, e Entry) (Entry, bool, error) {
	if EntrySize(e) > MaxEntryBytes {
		return Entry{}, false, ErrEntryTooLarge
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.slots[slot]; ok {
		got, err := UnmarshalEntry(cur)
		return got, false, err
	}
	m.slots[slot] = MarshalEntry(e, Version)
	m.tail = max(m.tail, slot+1)
	return e, true, nil
}

// Put stores raw bytes at slot (tests: a slot of an unknown format).
func (m *MemLog) Put(slot int64, raw []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slots[slot] = raw
	m.tail = max(m.tail, slot+1)
}

// Read implements Log.
func (m *MemLog) Read(_ context.Context, from int64, limit int) ([]Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Slot
	for n := max(from, 0); n < m.tail && len(out) < limit; n++ {
		raw, ok := m.slots[n]
		if !ok {
			continue
		}
		e, err := UnmarshalEntry(raw)
		if err != nil {
			return out, err
		}
		out = append(out, Slot{N: n, E: e})
	}
	return out, nil
}

// CheckTail implements Log.
func (m *MemLog) CheckTail(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tail, nil
}

// TrimBelow implements Trimmer: slots below the position become tombstones.
func (m *MemLog) TrimBelow(_ context.Context, slot int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for s, raw := range m.slots {
		if s < slot && !(len(raw) == 1 && raw[0] == tombstone[0]) {
			m.slots[s] = tombstone
			n++
		}
	}
	return n, nil
}
