package logsm

import (
	"context"
	"sync"
)

// MemLog is the in-memory Log. Slots may be sparse.
type MemLog struct {
	mu    sync.Mutex
	slots []*Entry
}

// NewMemLog returns an empty log.
func NewMemLog() *MemLog { return &MemLog{} }

// Append implements Log.
func (m *MemLog) Append(_ context.Context, slot int64, e Entry) (Entry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for int64(len(m.slots)) <= slot {
		m.slots = append(m.slots, nil)
	}
	if cur := m.slots[slot]; cur != nil {
		return *cur, false, nil
	}
	m.slots[slot] = &e
	return e, true, nil
}

// Read implements Log.
func (m *MemLog) Read(_ context.Context, from int64, limit int) ([]Slot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Slot
	for n := max(from, 0); n < int64(len(m.slots)) && len(out) < limit; n++ {
		if e := m.slots[n]; e != nil {
			out = append(out, Slot{N: n, E: *e})
		}
	}
	return out, nil
}

// CheckTail implements Log.
func (m *MemLog) CheckTail(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.slots)), nil
}
