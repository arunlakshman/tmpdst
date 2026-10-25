// Package pebblekv is the production applier.KV over Pebble
// (github.com/cockroachdb/pebble). It is the only package that imports the
// store. Commits do not fsync: the applier's state is rebuilt from the log and a
// checkpoint into a fresh directory, never trusted after a crash.
package pebblekv

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"manifester/cp/applier"
)

// Options sizes the store. Zero values mean the defaults (64 MiB each).
type Options struct {
	CacheBytes    int64
	MemTableBytes uint64
}

// DB is a Pebble database used as an applier.KV.
type DB struct {
	db   *pebble.DB
	once sync.Once
	err  error
}

var _ applier.KV = (*DB)(nil)

type quiet struct{}

func (quiet) Infof(string, ...any)  {}
func (quiet) Errorf(string, ...any) {}
func (quiet) Fatalf(f string, a ...any) {
	panic(fmt.Sprintf("pebblekv: "+f, a...))
}

// Open opens (creating if needed) the database in dir.
func Open(dir string, o Options) (*DB, error) {
	if o.CacheBytes <= 0 {
		o.CacheBytes = 64 << 20
	}
	if o.MemTableBytes == 0 {
		o.MemTableBytes = 64 << 20
	}
	cache := pebble.NewCache(o.CacheBytes)
	defer cache.Unref()
	db, err := pebble.Open(dir, &pebble.Options{Cache: cache, MemTableSize: o.MemTableBytes, Logger: quiet{}})
	if err != nil {
		return nil, fmt.Errorf("pebblekv: %w", err)
	}
	return &DB{db: db}, nil
}

// Close implements applier.KV; closing twice is harmless.
func (d *DB) Close() error {
	d.once.Do(func() { d.err = d.db.Close() })
	return d.err
}

// View implements applier.KV: a snapshot, so a multi-read query is consistent.
func (d *DB) View(fn func(applier.Tx)) error {
	s := d.db.NewSnapshot()
	defer func() { _ = s.Close() }() // read-only snapshot; nothing to flush
	t := &txn{get: s.Get, iter: s.NewIter}
	fn(t)
	return t.err
}

// Update implements applier.KV: an indexed batch (reads see its own writes),
// committed without sync.
func (d *DB) Update(fn func(applier.Tx)) error {
	b := d.db.NewIndexedBatch()
	defer func() { _ = b.Close() }() // Commit already reported any write error
	t := &txn{b: b, get: b.Get, iter: b.NewIter}
	fn(t)
	if t.err != nil {
		return t.err
	}
	return b.Commit(pebble.NoSync)
}

type txn struct {
	b    *pebble.Batch
	get  func([]byte) ([]byte, io.Closer, error)
	iter func(*pebble.IterOptions) (*pebble.Iterator, error)
	err  error // first error, latched
}

func (t *txn) fail(err error) {
	if err != nil && t.err == nil {
		t.err = err
	}
}

func (t *txn) Get(key []byte) []byte {
	v, c, err := t.get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		t.fail(err)
		return nil
	}
	defer func() { _ = c.Close() }() // read-only value handle
	return append([]byte{}, v...)
}

func (t *txn) Set(key, val []byte) { t.fail(t.b.Set(key, val, nil)) }

func (t *txn) Delete(key []byte) { t.fail(t.b.Delete(key, nil)) }

func (t *txn) Floor(lo, hi []byte) (key, val []byte) {
	it, err := t.iter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.fail(err)
		return nil, nil
	}
	defer func() { _ = it.Close() }() // read-only iterator
	if !it.Last() {
		t.fail(it.Error())
		return nil, nil
	}
	return append([]byte{}, it.Key()...), append([]byte{}, it.Value()...)
}

func (t *txn) Ascend(lo, hi []byte, fn func(key, val []byte) bool) {
	it, err := t.iter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.fail(err)
		return
	}
	defer func() { _ = it.Close() }() // read-only iterator
	for ok := it.First(); ok; ok = it.Next() {
		if !fn(it.Key(), it.Value()) {
			return
		}
	}
	t.fail(it.Error())
}
