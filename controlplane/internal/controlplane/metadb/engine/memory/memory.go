// Package memory is the in-memory engine.Engine: the default engine of the machine
// in the simulator and in tests. It is its own package so that the engine
// interface package imports no sync.
package memory

import (
	"bytes"
	"slices"
	"sync"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
)

// Engine is an in-memory engine.Engine: a sorted slice copied on every write transaction.
// It is the default store of the machine in the simulator and in tests. Production
// mdscontrolplane passes Pebble instead (see metadb/engine/pebble and replica.Config.OpenStore).
type Engine struct {
	mu      sync.RWMutex
	entries []memoryEntry
}

type memoryEntry struct{ key, value []byte }

// New returns an empty engine.
func New() *Engine { return &Engine{} }

// Close implements engine.Engine.
func (m *Engine) Close() error { return nil }

// View implements engine.Engine.
func (m *Engine) View(fn func(engine.Txn)) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn(&memoryTxn{entries: m.entries})
	return nil
}

// Update implements engine.Engine.
func (m *Engine) Update(fn func(engine.Txn)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	transaction := &memoryTxn{entries: slices.Clone(m.entries)}
	fn(transaction)
	m.entries = transaction.entries
	return nil
}

type memoryTxn struct{ entries []memoryEntry }

// at is the index of the first entry with key >= k.
func (t *memoryTxn) at(k []byte) (int, bool) {
	return slices.BinarySearchFunc(t.entries, k, func(e memoryEntry, k []byte) int { return bytes.Compare(e.key, k) })
}

func (t *memoryTxn) Get(key []byte) []byte {
	if i, ok := t.at(key); ok {
		return t.entries[i].value
	}
	return nil
}

func (t *memoryTxn) Set(key, val []byte) {
	e := memoryEntry{bytes.Clone(key), bytes.Clone(val)}
	if i, ok := t.at(key); ok {
		t.entries[i] = e
	} else {
		t.entries = slices.Insert(t.entries, i, e)
	}
}

func (t *memoryTxn) Delete(key []byte) {
	if i, ok := t.at(key); ok {
		t.entries = slices.Delete(t.entries, i, i+1)
	}
}

func (t *memoryTxn) Floor(lo, hi []byte) ([]byte, []byte) {
	i := len(t.entries)
	if hi != nil {
		i, _ = t.at(hi)
	}
	if i == 0 || bytes.Compare(t.entries[i-1].key, lo) < 0 {
		return nil, nil
	}
	return t.entries[i-1].key, t.entries[i-1].value
}

func (t *memoryTxn) Ascend(lo, hi []byte, fn func(key, val []byte) bool) {
	i, _ := t.at(lo)
	for ; i < len(t.entries) && (hi == nil || bytes.Compare(t.entries[i].key, hi) < 0); i++ {
		if !fn(t.entries[i].key, t.entries[i].value) {
			return
		}
	}
}
