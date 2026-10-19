package minikv

import (
	"bytes"
	"fmt"
)

// kind says what a queued entry does when it is popped.
type kind uint8

const (
	kindDeliver  kind = iota // hand Event to the server
	kindComplete             // apply Action to the disk, then hand the server the *Done event
)

// Entry is one future event. Plain data, like the server's events.
// Order is (At, Tie, Seq): virtual time, then a seeded coin flip for
// events that fall on the same instant, then insertion order so the
// order is total even when the coin flip ties.
type Entry struct {
	At     Time
	Tie    uint64
	Seq    uint64
	Kind   kind
	Event  EventBody // kindDeliver
	Action Action    // kindComplete
}

// Queue is a binary heap of entries.
type Queue struct {
	items []Entry
	seq   uint64
}

// Schedule enqueues e. Tie is drawn from r, Seq is assigned here.
// An entry scheduled in the past is clamped to now: time does not go backwards.
func (q *Queue) Schedule(r *RNG, now Time, e Entry) {
	if e.At < now {
		e.At = now
	}
	e.Tie = r.Uint64()
	q.seq++
	e.Seq = q.seq
	q.items = append(q.items, e)
	q.up(len(q.items) - 1)
}

// Pop returns the earliest entry. ok is false when the queue is empty.
func (q *Queue) Pop() (Entry, bool) {
	n := len(q.items)
	if n == 0 {
		return Entry{}, false
	}
	e := q.items[0]
	q.items[0] = q.items[n-1]
	q.items[n-1] = Entry{}
	q.items = q.items[:n-1]
	if len(q.items) > 0 {
		q.down(0)
	}
	return e, true
}

func (q *Queue) less(i, j int) bool {
	a, b := &q.items[i], &q.items[j]
	if a.At != b.At {
		return a.At < b.At
	}
	if a.Tie != b.Tie {
		return a.Tie < b.Tie
	}
	return a.Seq < b.Seq
}

func (q *Queue) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !q.less(i, p) {
			return
		}
		q.items[i], q.items[p] = q.items[p], q.items[i]
		i = p
	}
}

func (q *Queue) down(i int) {
	n := len(q.items)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		m := l
		if r := l + 1; r < n && q.less(r, l) {
			m = r
		}
		if !q.less(m, i) {
			return
		}
		q.items[i], q.items[m] = q.items[m], q.items[i]
		i = m
	}
}

// Disk is a perfect filesystem: a map from path to bytes.
// Every operation succeeds. Fsync and FsyncDir change nothing, and that is
// the lie. Part 2 replaces this map with a disk that can forget.
// Keys are only ever looked up, never ranged over. Ranging a Go map is
// not deterministic, and a trace hash that did it would change every run.
type Disk struct {
	files map[string][]byte
}

func NewDisk() *Disk { return &Disk{files: map[string][]byte{}} }

// apply performs a filesystem action and returns the completion event.
// The bytes stored are a clone: the server and the disk do not share a slice.
func apply(d *Disk, a Action) EventBody {
	switch a := a.(type) {
	case Write:
		d.files[a.Path] = bytes.Clone(a.Data)
		return WriteDone{IO: a.IO}
	case Fsync:
		return FsyncDone{IO: a.IO}
	case Rename:
		d.files[a.To] = bytes.Clone(d.files[a.From])
		delete(d.files, a.From)
		return RenameDone{IO: a.IO}
	case FsyncDir:
		return FsyncDirDone{IO: a.IO}
	case Read:
		b, ok := d.files[a.Path]
		if !ok {
			return ReadDone{IO: a.IO, Found: false}
		}
		return ReadDone{IO: a.IO, Found: true, Data: bytes.Clone(b)}
	default:
		panic(fmt.Sprintf("minikv: not a disk action %T", a))
	}
}

// Result is one simulated run.
type Result struct {
	Hash    uint64
	Calls   uint64
	Steps   int
	Log     []string
	Replies []Reply
}

// script is the workload for Part 1: five requests at fixed virtual times.
// Two of them share an instant on purpose, so the tie-break has something to do.
// Part 2 replaces this list with a generator.
func script() []Entry {
	put := func(c uint64, key, val string) EventBody {
		return Request{Client: c, Op: Put{Key: key, Data: []byte(val)}}
	}
	get := func(c uint64, key string) EventBody {
		return Request{Client: c, Op: Get{Key: key}}
	}
	return []Entry{
		{At: 0, Kind: kindDeliver, Event: put(1, "a", "one")},
		{At: 1 * Millisecond, Kind: kindDeliver, Event: get(2, "a")},
		{At: 2 * Millisecond, Kind: kindDeliver, Event: put(3, "b", "two")},
		{At: 2 * Millisecond, Kind: kindDeliver, Event: get(4, "a")},
		{At: 5 * Millisecond, Kind: kindDeliver, Event: get(5, "missing")},
	}
}

// Run executes one seed to completion and returns the trace hash, the log,
// and the replies. The same seed always returns the same Result.
func Run(seed uint64) Result {
	rng := NewRNG(seed)
	q := &Queue{}
	disk := NewDisk()
	srv := &Server{}
	var now Time
	// FNV-1a offset basis. The hash is only a fingerprint of the trace.
	trace := uint64(14695981039346656037)

	for _, e := range script() {
		q.Schedule(rng, 0, e)
	}

	var res Result
	const maxSteps = 10000
	for res.Steps < maxSteps {
		e, ok := q.Pop()
		if !ok {
			break
		}
		res.Steps++
		now = e.At
		trace = fold(trace, uint64(now))
		trace = fold(trace, uint64(e.Kind))

		var ev EventBody
		switch e.Kind {
		case kindDeliver:
			ev = e.Event
		case kindComplete:
			ev = apply(disk, e.Action)
		default:
			panic(fmt.Sprintf("minikv: bad kind %d", e.Kind))
		}
		trace = hashEvent(trace, ev)

		actions := srv.Step(Event{At: now, Body: ev})
		line := fmt.Sprintf("t=%s %s => %s", fmtTime(now), describeEvent(ev), describeActions(actions))
		res.Log = append(res.Log, line)

		for _, a := range actions {
			trace = hashAction(trace, a)
			if rep, ok := a.(Reply); ok {
				res.Replies = append(res.Replies, rep)
				continue
			}
			// 1..500us. The delay is drawn from the seed, so it is part of
			// the run, not noise around it.
			delay := Time(rng.Intn(500)+1) * Microsecond
			q.Schedule(rng, now, Entry{At: now + delay, Kind: kindComplete, Action: a})
		}
	}
	if res.Steps == maxSteps {
		panic("minikv: step limit hit; the queue is not draining")
	}
	res.Hash = trace
	res.Calls = rng.Calls()
	return res
}

func fmtTime(t Time) string {
	return fmt.Sprintf("%.3fus", float64(t)/float64(Microsecond))
}

func describeEvent(ev EventBody) string {
	switch e := ev.(type) {
	case Request:
		return fmt.Sprintf("Request{c%d %s}", e.Client, describeOp(e.Op))
	case WriteDone:
		return fmt.Sprintf("WriteDone{io%d}", e.IO)
	case FsyncDone:
		return fmt.Sprintf("FsyncDone{io%d}", e.IO)
	case RenameDone:
		return fmt.Sprintf("RenameDone{io%d}", e.IO)
	case FsyncDirDone:
		return fmt.Sprintf("FsyncDirDone{io%d}", e.IO)
	case ReadDone:
		if !e.Found {
			return fmt.Sprintf("ReadDone{io%d missing}", e.IO)
		}
		return fmt.Sprintf("ReadDone{io%d %q}", e.IO, e.Data)
	default:
		return fmt.Sprintf("%T", ev)
	}
}

func describeOp(op Op) string {
	switch o := op.(type) {
	case Put:
		return fmt.Sprintf("PUT %s %q", o.Key, o.Data)
	case Get:
		return fmt.Sprintf("GET %s", o.Key)
	default:
		return fmt.Sprintf("%T", op)
	}
}

func describeActions(as []Action) string {
	if len(as) == 0 {
		return "(queued)"
	}
	out := ""
	for i, a := range as {
		if i > 0 {
			out += " "
		}
		out += describeAction(a)
	}
	return out
}

func describeAction(a Action) string {
	switch a := a.(type) {
	case Write:
		return fmt.Sprintf("Write{io%d %s %q}", a.IO, a.Path, a.Data)
	case Fsync:
		return fmt.Sprintf("Fsync{io%d %s}", a.IO, a.Path)
	case Rename:
		return fmt.Sprintf("Rename{io%d %s -> %s}", a.IO, a.From, a.To)
	case FsyncDir:
		return fmt.Sprintf("FsyncDir{io%d %s}", a.IO, a.Dir)
	case Read:
		return fmt.Sprintf("Read{io%d %s}", a.IO, a.Path)
	case Reply:
		if a.Body == nil {
			return fmt.Sprintf("Reply{c%d %d}", a.Client, a.Status)
		}
		return fmt.Sprintf("Reply{c%d %d %q}", a.Client, a.Status, a.Body)
	default:
		return fmt.Sprintf("%T", a)
	}
}

// fold mixes x into h. It is a small finalizer, not a cryptographic hash:
// we want collisions to be absurdly unlikely, and we want it to have no
// global state of its own.
func fold(h, x uint64) uint64 {
	h ^= x
	h *= 1099511628211 // FNV-1a prime
	return h
}

func hashEvent(h uint64, ev EventBody) uint64 {
	switch e := ev.(type) {
	case Request:
		h = fold(h, 1)
		h = fold(h, e.Client)
		return hashOp(h, e.Op)
	case WriteDone:
		return fold(fold(h, 2), uint64(e.IO))
	case FsyncDone:
		return fold(fold(h, 3), uint64(e.IO))
	case RenameDone:
		return fold(fold(h, 4), uint64(e.IO))
	case FsyncDirDone:
		return fold(fold(h, 5), uint64(e.IO))
	case ReadDone:
		h = fold(h, 6)
		h = fold(h, uint64(e.IO))
		if e.Found {
			h = fold(h, 1)
		} else {
			h = fold(h, 0)
		}
		return hashBytes(h, e.Data)
	default:
		panic(fmt.Sprintf("minikv: hash event %T", ev))
	}
}

func hashOp(h uint64, op Op) uint64 {
	switch o := op.(type) {
	case Put:
		h = fold(h, 1)
		h = hashStr(h, o.Key)
		return hashBytes(h, o.Data)
	case Get:
		h = fold(h, 2)
		return hashStr(h, o.Key)
	default:
		panic(fmt.Sprintf("minikv: hash op %T", op))
	}
}

func hashAction(h uint64, a Action) uint64 {
	switch a := a.(type) {
	case Write:
		h = fold(h, 1)
		h = fold(h, uint64(a.IO))
		h = hashStr(h, a.Path)
		return hashBytes(h, a.Data)
	case Fsync:
		h = fold(h, 2)
		h = fold(h, uint64(a.IO))
		return hashStr(h, a.Path)
	case Rename:
		h = fold(h, 3)
		h = fold(h, uint64(a.IO))
		h = hashStr(h, a.From)
		return hashStr(h, a.To)
	case FsyncDir:
		h = fold(h, 4)
		h = fold(h, uint64(a.IO))
		return hashStr(h, a.Dir)
	case Read:
		h = fold(h, 5)
		h = fold(h, uint64(a.IO))
		return hashStr(h, a.Path)
	case Reply:
		h = fold(h, 6)
		h = fold(h, a.Client)
		h = fold(h, uint64(a.Status))
		return hashBytes(h, a.Body)
	default:
		panic(fmt.Sprintf("minikv: hash action %T", a))
	}
}

func hashStr(h uint64, s string) uint64 {
	h = fold(h, uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h = fold(h, uint64(s[i]))
	}
	return h
}

func hashBytes(h uint64, b []byte) uint64 {
	h = fold(h, uint64(len(b)))
	for _, c := range b {
		h = fold(h, uint64(c))
	}
	return h
}
