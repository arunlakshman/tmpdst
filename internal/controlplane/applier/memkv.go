package applier

import (
	"bytes"
	"slices"
	"sync"
)

// MemoryKeyValueStore is an in-memory KV: a sorted slice copied on every write transaction.
// It is a test fake for small states. The chosen production store is Pebble
// (see applier/pebblekv), but the control-plane machine uses MemoryKeyValueStore today.
type MemoryKeyValueStore struct {
	mu sync.RWMutex
	es []memoryEntry
}

type memoryEntry struct{ k, v []byte }

// NewMemoryKeyValueStore returns an empty store.
func NewMemoryKeyValueStore() *MemoryKeyValueStore { return &MemoryKeyValueStore{} }

// Close implements KeyValueStore.
func (m *MemoryKeyValueStore) Close() error { return nil }

// View implements KeyValueStore.
func (m *MemoryKeyValueStore) View(fn func(Transaction)) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn(&memoryTransaction{es: m.es})
	return nil
}

// Update implements KeyValueStore.
func (m *MemoryKeyValueStore) Update(fn func(Transaction)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx := &memoryTransaction{es: slices.Clone(m.es)}
	fn(tx)
	m.es = tx.es
	return nil
}

type memoryTransaction struct{ es []memoryEntry }

// at is the index of the first entry with key >= k.
func (t *memoryTransaction) at(k []byte) (int, bool) {
	return slices.BinarySearchFunc(t.es, k, func(e memoryEntry, k []byte) int { return bytes.Compare(e.k, k) })
}

func (t *memoryTransaction) Get(key []byte) []byte {
	if i, ok := t.at(key); ok {
		return t.es[i].v
	}
	return nil
}

func (t *memoryTransaction) Set(key, val []byte) {
	e := memoryEntry{bytes.Clone(key), bytes.Clone(val)}
	if i, ok := t.at(key); ok {
		t.es[i] = e
	} else {
		t.es = slices.Insert(t.es, i, e)
	}
}

func (t *memoryTransaction) Delete(key []byte) {
	if i, ok := t.at(key); ok {
		t.es = slices.Delete(t.es, i, i+1)
	}
}

func (t *memoryTransaction) Floor(lo, hi []byte) ([]byte, []byte) {
	i := len(t.es)
	if hi != nil {
		i, _ = t.at(hi)
	}
	if i == 0 || bytes.Compare(t.es[i-1].k, lo) < 0 {
		return nil, nil
	}
	return t.es[i-1].k, t.es[i-1].v
}

func (t *memoryTransaction) Ascend(lo, hi []byte, fn func(key, val []byte) bool) {
	i, _ := t.at(lo)
	for ; i < len(t.es) && (hi == nil || bytes.Compare(t.es[i].k, hi) < 0); i++ {
		if !fn(t.es[i].k, t.es[i].v) {
			return
		}
	}
}
