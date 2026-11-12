package executor

import (
	"context"
	"slices"
	"sync"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Log is a byte-oriented shared loglet. Slots hold opaque machine payloads.
//
// The log also keeps two small values next to the slots: the writer epoch
// (ClaimEpoch) and the floor (TrimThrough). The floor is the highest slot that
// may already have been deleted by a trim.
type Log interface {
	// Append puts data at slot. It has three outcomes:
	//   - success: (nil, false, nil). The entry is stored and it is a commit.
	//   - taken with an existing entry: (existing, true, nil), where existing is
	//     not empty. The slot holds another entry above the floor. The put is
	//     not a commit.
	//   - taken with empty existing: (nil, true, nil). The slot is at or below
	//     the floor, so it may have been trimmed. The put is not a commit. A
	//     stray row may stay in the slot. Read never returns it.
	Append(ctx context.Context, slot runtime.LogPosition, data []byte) (existing []byte, taken bool, err error)
	// Read returns up to limit entries at or after from, in slot order. Rows at
	// or below the floor are never returned.
	Read(ctx context.Context, from runtime.LogPosition, limit int) ([]runtime.LogEntry, error)
	// CheckTail returns the highest written slot plus one.
	CheckTail(ctx context.Context) (runtime.LogPosition, error)
	// TrimThrough raises the floor to through, and then deletes the slots at or
	// below through. The floor never moves down.
	TrimThrough(ctx context.Context, through runtime.LogPosition) error
	// ClaimEpoch is a compare-and-swap on the writer epoch. It wins only if the
	// stored epoch equals expect (no stored epoch counts as 0). On a win the
	// stored epoch becomes expect+1 and the owner becomes owner, and it returns
	// (expect+1, owner, true, nil). On a loss it returns the stored epoch and
	// owner with won false and a nil error.
	ClaimEpoch(ctx context.Context, expect uint64, owner string) (epoch uint64, storedOwner string, won bool, err error)
}

// MemoryLog is an in-memory Log for tests and -store mem demos.
// Use NewMemoryLog. The zero value is not usable, because its floor is 0
// and not -1 (none).
type MemoryLog struct {
	mu    sync.Mutex
	slots []slotRecord
	floor int64 // highest slot that may have been trimmed; -1 means none
	epoch uint64
	owner string
}

type slotKind uint8

const (
	slotEmpty slotKind = iota
	slotEntry
)

type slotRecord struct {
	kind slotKind
	data []byte
}

// NewMemoryLog returns an empty in-memory log.
func NewMemoryLog() *MemoryLog { return &MemoryLog{floor: -1} }

func (m *MemoryLog) ensure(slot runtime.LogPosition) {
	n := int(slot) + 1
	if n > len(m.slots) {
		m.slots = append(m.slots, make([]slotRecord, n-len(m.slots))...)
	}
}

// belowFloor reports whether slot is at or below the floor. The lock is held by the caller.
func (m *MemoryLog) belowFloor(slot runtime.LogPosition) bool {
	return int64(slot) <= m.floor
}

// Append implements Log.
func (m *MemoryLog) Append(_ context.Context, slot runtime.LogPosition, data []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot < 0 {
		return nil, false, ErrInvalidSlot
	}
	m.ensure(slot)
	if cur := m.slots[slot]; cur.kind == slotEntry {
		if m.belowFloor(slot) {
			return nil, true, nil
		}
		return slices.Clone(cur.data), true, nil
	}
	m.slots[slot] = slotRecord{kind: slotEntry, data: slices.Clone(data)}
	if m.belowFloor(slot) {
		return nil, true, nil
	}
	return nil, false, nil
}

// Read implements Log. It scans first and reads the floor under the same lock,
// the same order as DynamoLog.Read.
func (m *MemoryLog) Read(_ context.Context, from runtime.LogPosition, limit int) ([]runtime.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from < 0 || limit < 0 {
		return nil, ErrInvalidSlot
	}
	var out []runtime.LogEntry
	for i := int(from); i < len(m.slots) && len(out) < limit; i++ {
		if m.slots[i].kind != slotEntry {
			continue
		}
		out = append(out, runtime.LogEntry{Slot: runtime.LogPosition(i), Data: slices.Clone(m.slots[i].data)})
	}
	// Rows at or below the floor are stray rows left by a rejected put.
	return slices.DeleteFunc(out, func(e runtime.LogEntry) bool { return m.belowFloor(e.Slot) }), nil
}

// CheckTail implements Log.
func (m *MemoryLog) CheckTail(context.Context) (runtime.LogPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range slices.Backward(m.slots) {
		if m.slots[i].kind != slotEmpty {
			return runtime.LogPosition(i) + 1, nil
		}
	}
	return 0, nil
}

// TrimThrough implements Log. The floor goes up first, then the slots are
// cleared.
func (m *MemoryLog) TrimThrough(_ context.Context, through runtime.LogPosition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if through < 0 {
		return ErrInvalidSlot
	}
	m.floor = max(m.floor, int64(through))
	for i := range min(int(through)+1, len(m.slots)) {
		m.slots[i] = slotRecord{}
	}
	return nil
}

// ClaimEpoch implements Log.
func (m *MemoryLog) ClaimEpoch(_ context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch != expect {
		return m.epoch, m.owner, false, nil
	}
	m.epoch = expect + 1
	m.owner = owner
	return m.epoch, m.owner, true, nil
}
