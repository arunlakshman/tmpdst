package executor

import (
	"context"
	"slices"
	"sync"

	"mds/internal/core"
)

// Log is a byte-oriented shared loglet. Slots hold opaque machine payloads.
type Log interface {
	Append(ctx context.Context, slot core.LogPosition, data []byte) (existing []byte, taken bool, err error)
	Read(ctx context.Context, from core.LogPosition, limit int) ([]core.LogEntry, error)
	CheckTail(ctx context.Context) (core.LogPosition, error)
	TrimThrough(ctx context.Context, through core.LogPosition) error
}

// MemoryLog is an in-memory Log for tests and -store mem demos.
type MemoryLog struct {
	mu    sync.Mutex
	slots []slotRecord
}

type slotKind uint8

const (
	slotEmpty slotKind = iota
	slotEntry
	slotDeleted
)

type slotRecord struct {
	kind slotKind
	data []byte
}

// NewMemoryLog returns an empty in-memory log.
func NewMemoryLog() *MemoryLog { return &MemoryLog{} }

func (m *MemoryLog) ensure(slot core.LogPosition) {
	n := int(slot) + 1
	if n > len(m.slots) {
		m.slots = append(m.slots, make([]slotRecord, n-len(m.slots))...)
	}
}

// Append implements Log.
func (m *MemoryLog) Append(_ context.Context, slot core.LogPosition, data []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot < 0 {
		return nil, false, ErrInvalidSlot
	}
	m.ensure(slot)
	cur := m.slots[slot]
	switch cur.kind {
	case slotEntry:
		return slices.Clone(cur.data), true, nil
	case slotDeleted:
		return nil, true, nil
	}
	m.slots[slot] = slotRecord{kind: slotEntry, data: slices.Clone(data)}
	return nil, false, nil
}

// Read implements Log.
func (m *MemoryLog) Read(_ context.Context, from core.LogPosition, limit int) ([]core.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from < 0 || limit < 0 {
		return nil, ErrInvalidSlot
	}
	var out []core.LogEntry
	for i := int(from); i < len(m.slots) && len(out) < limit; i++ {
		if m.slots[i].kind != slotEntry {
			continue
		}
		out = append(out, core.LogEntry{Slot: core.LogPosition(i), Data: slices.Clone(m.slots[i].data)})
	}
	return out, nil
}

// CheckTail implements Log.
func (m *MemoryLog) CheckTail(context.Context) (core.LogPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range slices.Backward(m.slots) {
		if m.slots[i].kind != slotEmpty {
			return core.LogPosition(i) + 1, nil
		}
	}
	return 0, nil
}

// TrimThrough implements Log.
func (m *MemoryLog) TrimThrough(_ context.Context, through core.LogPosition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if through < 0 {
		return ErrInvalidSlot
	}
	m.ensure(through)
	for i := range int(through) + 1 {
		if m.slots[i].kind == slotEntry {
			m.slots[i] = slotRecord{kind: slotDeleted}
		}
	}
	return nil
}
