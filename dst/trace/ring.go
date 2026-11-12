package trace

// Ring keeps the last N opaque step records for failure reports.
// T is caller-defined. dst does not format domain events.
type Ring[T any] struct {
	buf []T
	pos int
	n   int
}

// NewRing returns a ring that retains at most n records. n <= 0 yields an empty ring.
func NewRing[T any](n int) *Ring[T] {
	if n <= 0 {
		return &Ring[T]{}
	}
	return &Ring[T]{buf: make([]T, n)}
}

// Add appends v, overwriting the oldest entry when full.
func (r *Ring[T]) Add(v T) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.pos%len(r.buf)] = v
	r.pos++
	if r.n < len(r.buf) {
		r.n++
	}
}

// Tail returns the retained records oldest first.
func (r *Ring[T]) Tail() []T {
	if r.n == 0 {
		return nil
	}
	out := make([]T, 0, r.n)
	first := max(0, r.pos-len(r.buf))
	for i := first; i < r.pos; i++ {
		out = append(out, r.buf[i%len(r.buf)])
	}
	return out
}
