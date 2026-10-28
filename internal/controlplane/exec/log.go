package exec

import (
	"context"
	"slices"
	"sync"

	"manifester/internal/core"
)

// Log is a byte-oriented shared loglet. Slots hold opaque machine payloads.
type Log interface {
	Append(ctx context.Context, slot core.LogSlot, data []byte) (existing []byte, taken bool, err error)
	Read(ctx context.Context, from core.LogSlot, limit int) ([]core.LogEntry, error)
	CheckTail(ctx context.Context) (core.LogSlot, error)
	TrimThrough(ctx context.Context, through core.LogSlot) error
}

// MemLog is an in-memory Log for tests and -store mem demos.
type MemLog struct {
	mu    sync.Mutex
	slots []slotRec
}

type slotKind uint8

const (
	slotEmpty slotKind = iota
	slotEntry
	slotTomb
)

type slotRec struct {
	kind slotKind
	data []byte
}

// NewMemLog returns an empty in-memory log.
func NewMemLog() *MemLog { return &MemLog{} }

func (m *MemLog) ensure(slot core.LogSlot) {
	n := int(slot) + 1
	if n > len(m.slots) {
		m.slots = append(m.slots, make([]slotRec, n-len(m.slots))...)
	}
}

// Append implements Log.
func (m *MemLog) Append(_ context.Context, slot core.LogSlot, data []byte) ([]byte, bool, error) {
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
	case slotTomb:
		return nil, true, nil
	}
	m.slots[slot] = slotRec{kind: slotEntry, data: slices.Clone(data)}
	return nil, false, nil
}

// Read implements Log.
func (m *MemLog) Read(_ context.Context, from core.LogSlot, limit int) ([]core.LogEntry, error) {
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
		out = append(out, core.LogEntry{Slot: core.LogSlot(i), Data: slices.Clone(m.slots[i].data)})
	}
	return out, nil
}

// CheckTail implements Log.
func (m *MemLog) CheckTail(context.Context) (core.LogSlot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range slices.Backward(m.slots) {
		if m.slots[i].kind != slotEmpty {
			return core.LogSlot(i) + 1, nil
		}
	}
	return 0, nil
}

// TrimThrough implements Log.
func (m *MemLog) TrimThrough(_ context.Context, through core.LogSlot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if through < 0 {
		return ErrInvalidSlot
	}
	m.ensure(through)
	for i := range int(through) + 1 {
		if m.slots[i].kind == slotEntry {
			m.slots[i] = slotRec{kind: slotTomb}
		}
	}
	return nil
}
