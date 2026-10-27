package applier

import (
	"bytes"
	"slices"
	"sync"
)

// MemKV is an in-memory KV: a sorted slice copied on every write transaction.
// It is the test fake (small states only); the production store is boltkv.
type MemKV struct {
	mu sync.RWMutex
	es []memEntry
}

type memEntry struct{ k, v []byte }

// NewMemKV returns an empty store.
func NewMemKV() *MemKV { return &MemKV{} }

// Close implements KV.
func (m *MemKV) Close() error { return nil }

// View implements KV.
func (m *MemKV) View(fn func(Tx)) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn(&memTx{es: m.es})
	return nil
}

// Update implements KV.
func (m *MemKV) Update(fn func(Tx)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx := &memTx{es: slices.Clone(m.es)}
	fn(tx)
	m.es = tx.es
	return nil
}

type memTx struct{ es []memEntry }

// at is the index of the first entry with key >= k.
func (t *memTx) at(k []byte) (int, bool) {
	return slices.BinarySearchFunc(t.es, k, func(e memEntry, k []byte) int { return bytes.Compare(e.k, k) })
}

func (t *memTx) Get(key []byte) []byte {
	if i, ok := t.at(key); ok {
		return t.es[i].v
	}
	return nil
}

func (t *memTx) Set(key, val []byte) {
	e := memEntry{bytes.Clone(key), bytes.Clone(val)}
	if i, ok := t.at(key); ok {
		t.es[i] = e
	} else {
		t.es = slices.Insert(t.es, i, e)
	}
}

func (t *memTx) Delete(key []byte) {
	if i, ok := t.at(key); ok {
		t.es = slices.Delete(t.es, i, i+1)
	}
}

func (t *memTx) Floor(lo, hi []byte) ([]byte, []byte) {
	i := len(t.es)
	if hi != nil {
		i, _ = t.at(hi)
	}
	if i == 0 || bytes.Compare(t.es[i-1].k, lo) < 0 {
		return nil, nil
	}
	return t.es[i-1].k, t.es[i-1].v
}

func (t *memTx) Ascend(lo, hi []byte, fn func(key, val []byte) bool) {
	i, _ := t.at(lo)
	for ; i < len(t.es) && (hi == nil || bytes.Compare(t.es[i].k, hi) < 0); i++ {
		if !fn(t.es[i].k, t.es[i].v) {
			return
		}
	}
}
