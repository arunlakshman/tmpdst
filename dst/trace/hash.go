package trace

// Hash is a running 64-bit FNV-1a fingerprint.
// Integers fold as 8 little-endian bytes. Strings and byte slices are length-prefixed.
// Invariant: identical fold sequences yield identical Sum.
type Hash uint64

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = fnvP1
)

// New returns the FNV-1a offset basis.
func New() Hash { return fnvOffset }

// Sum is the hash so far.
func (h Hash) Sum() uint64 { return uint64(h) }

// Byte folds one byte.
func (h *Hash) Byte(b byte) { *h = (*h ^ Hash(b)) * fnvPrime }

// Uint64 folds v as 8 little-endian bytes.
func (h *Hash) Uint64(v uint64) { *h = Hash(fnv64Hash(uint64(*h), v)) }

// Int64 folds v as Uint64 does.
func (h *Hash) Int64(v int64) { h.Uint64(uint64(v)) }

// Bool folds 1 or 0 as Uint64 does.
func (h *Hash) Bool(b bool) {
	if b {
		h.Uint64(1)
	} else {
		h.Uint64(0)
	}
}

// String folds the length of s, then its bytes.
func (h *Hash) String(s string) {
	h.Uint64(uint64(len(s)))
	for _, c := range []byte(s) {
		h.Byte(c)
	}
}

// Bytes folds the length of b, then its bytes.
func (h *Hash) Bytes(b []byte) {
	h.Uint64(uint64(len(b)))
	for _, c := range b {
		h.Byte(c)
	}
}
