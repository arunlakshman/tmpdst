package applier

import "bytes"

// KeyValueStore is the embedded ordered key-value store the applier keeps its state in.
// It is a deliberately small seam: a read transaction, an atomic write
// transaction, nothing else. Durability is not required (state is rebuilt from
// the log and a checkpoint), so an implementation may skip fsync.
type KeyValueStore interface {
	// View runs fn on a consistent read-only view.
	View(fn func(Transaction)) error
	// Update runs fn as one atomic write transaction, reading its own writes.
	// Nothing is applied if the store reports an error.
	Update(fn func(Transaction)) error
	Close() error
}

// Transaction is one transaction. A store error does not interrupt fn: the first error
// is latched, reads then return nothing, and View or Update returns the error,
// so the callers stay free of error plumbing. Bytes passed to Set must stay
// unchanged until Update returns; bytes returned stay valid until fn returns
// (copy to keep them).
type Transaction interface {
	// Get returns the value of key, nil when absent. Values are never empty.
	Get(key []byte) []byte
	// Floor returns the last entry with lo <= key < hi, a nil key if none.
	Floor(lo, hi []byte) (key, val []byte)
	// Ascend calls fn for the entries with lo <= key < hi in key order until
	// fn returns false. A nil hi is unbounded. fn must not write.
	Ascend(lo, hi []byte, fn func(key, val []byte) bool)
	Set(key, val []byte)
	Delete(key []byte)
}

// prefixEnd is the smallest key greater than every key that starts with p
// (nil when there is none).
func prefixEnd(p []byte) []byte {
	e := bytes.Clone(p)
	for len(e) > 0 {
		if e[len(e)-1] != 0xFF {
			e[len(e)-1]++
			return e
		}
		e = e[:len(e)-1]
	}
	return nil
}
