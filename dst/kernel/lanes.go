package kernel

// Lanes gives one lane per key, in first-use order, from a counter shared by
// every Lanes of the world, so two key types never share a lane. Making a lane
// draws no randomness. The same key gives the same lane for the whole run.
// K is any comparable product type, for example
// struct{ pair uint64; dir uint8 }.
type Lanes[K comparable] struct {
	w *World
	m map[K]Lane
}

// NewLanes makes a lane table for key type K.
func NewLanes[K comparable](w *World) *Lanes[K] {
	return &Lanes[K]{w: w, m: map[K]Lane{}}
}

// Get returns the lane of key. Product lanes stay below HostLane; running out
// of them is a protocol failure that halts the run.
func (l *Lanes[K]) Get(key K) Lane {
	if v, ok := l.m[key]; ok {
		return v
	}
	w := l.w
	if w.nextLane >= HostLane {
		w.Fail(FailProtocol, "lanes", "no product lane is left")
		w.halted = true
		return NoLane
	}
	v := w.nextLane
	w.nextLane++
	l.m[key] = v
	return v
}
