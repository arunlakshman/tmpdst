package stateupdater

import (
	"bytes"
	"slices"
	"sync"
)

// MemoryKeyValueStore is an in-memory KV: a sorted slice copied on every write transaction.
// It is the default store of the machine in the simulator and in tests. Production
// manicp passes Pebble instead (see stateupdater/pebblestore and statemachine.Config.OpenStore).
type MemoryKeyValueStore struct {
	mu      sync.RWMutex
	entries []memoryEntry
}

type memoryEntry struct{ key, value []byte }

// NewMemoryKeyValueStore returns an empty store.
func NewMemoryKeyValueStore() *MemoryKeyValueStore { return &MemoryKeyValueStore{} }

// Close implements KeyValueStore.
func (m *MemoryKeyValueStore) Close() error { return nil }

// View implements KeyValueStore.
func (m *MemoryKeyValueStore) View(fn func(Transaction)) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn(&memoryTransaction{entries: m.entries})
	return nil
}

// Update implements KeyValueStore.
func (m *MemoryKeyValueStore) Update(fn func(Transaction)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx := &memoryTransaction{entries: slices.Clone(m.entries)}
	fn(tx)
	m.entries = tx.entries
	return nil
}

type memoryTransaction struct{ entries []memoryEntry }

// at is the index of the first entry with key >= k.
func (t *memoryTransaction) at(k []byte) (int, bool) {
	return slices.BinarySearchFunc(t.entries, k, func(e memoryEntry, k []byte) int { return bytes.Compare(e.key, k) })
}

func (t *memoryTransaction) Get(key []byte) []byte {
	if i, ok := t.at(key); ok {
		return t.entries[i].value
	}
	return nil
}

func (t *memoryTransaction) Set(key, val []byte) {
	e := memoryEntry{bytes.Clone(key), bytes.Clone(val)}
	if i, ok := t.at(key); ok {
		t.entries[i] = e
	} else {
		t.entries = slices.Insert(t.entries, i, e)
	}
}

func (t *memoryTransaction) Delete(key []byte) {
	if i, ok := t.at(key); ok {
		t.entries = slices.Delete(t.entries, i, i+1)
	}
}

func (t *memoryTransaction) Floor(lo, hi []byte) ([]byte, []byte) {
	i := len(t.entries)
	if hi != nil {
		i, _ = t.at(hi)
	}
	if i == 0 || bytes.Compare(t.entries[i-1].key, lo) < 0 {
		return nil, nil
	}
	return t.entries[i-1].key, t.entries[i-1].value
}

func (t *memoryTransaction) Ascend(lo, hi []byte, fn func(key, val []byte) bool) {
	i, _ := t.at(lo)
	for ; i < len(t.entries) && (hi == nil || bytes.Compare(t.entries[i].key, hi) < 0); i++ {
		if !fn(t.entries[i].key, t.entries[i].value) {
			return
		}
	}
}
