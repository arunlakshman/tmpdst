package sim

import (
	"encoding/binary"
	"strconv"

	"manifester/toys3/core"
)

// OpMix weights the operations clients issue. Zero weights disable an op.
type OpMix struct {
	Put, Get, Delete, Copy int
	// Invalid issues a request with an empty key (always 400).
	Invalid int
}

// WorkloadConfig shapes client traffic.
type WorkloadConfig struct {
	// Clients is the number of concurrent clients; each has at most one
	// request outstanding, so Clients bounds concurrency.
	Clients int
	// Keys is the size of the key set ("k0".."k{n-1}"). Small sets force
	// same-key contention, which is where the interesting bugs live.
	Keys int
	// MaxValueSize bounds PUT payloads (at least ValueHeader).
	MaxValueSize int
	Mix          OpMix
	// Think is the [min, max] delay between a reply and the client's next request.
	Think [2]core.Duration
	// QuiesceEvery, if > 0, periodically stops clients from issuing until all
	// outstanding requests have replied. Quiescent points split the history
	// into independent windows for the linearizability checker, bounding its
	// search. A crash is also a quiescent point.
	QuiesceEvery core.Duration
}

// clientState is one simulated client.
type clientState struct {
	busy bool           // a request is outstanding
	req  core.ClientReq // the outstanding request, if busy
}

// Workload is the set of simulated clients. It is driven by the simulator
// (not a core.StateMachine: it never crashes and needs no runtime).
//
// Every PUT carries a fresh, self-describing value (see MakeValue), so any
// bytes returned by a GET identify the PUT that wrote them, and COPY
// propagates that identity unchanged. This is what lets the oracle check
// reads exactly instead of guessing.
type Workload struct {
	cfg     WorkloadConfig
	rng     *Rand
	nextReq core.ClientReq
	clients []clientState
	keys    []string
}

// NewWorkload returns idle clients over cfg.Keys keys.
func NewWorkload(cfg WorkloadConfig, r *Rand) *Workload {
	if cfg.Clients < 1 {
		cfg.Clients = 1
	}
	if cfg.Keys < 1 {
		cfg.Keys = 1
	}
	if cfg.MaxValueSize < ValueHeader {
		cfg.MaxValueSize = ValueHeader
	}
	if cfg.Mix == (OpMix{}) {
		cfg.Mix = OpMix{Put: 1, Get: 1}
	}
	w := &Workload{cfg: cfg, rng: r, clients: make([]clientState, cfg.Clients)}
	for i := 0; i < cfg.Keys; i++ {
		w.keys = append(w.keys, "k"+strconv.Itoa(i))
	}
	return w
}

// Clients is the number of clients.
func (w *Workload) Clients() int { return len(w.clients) }

// Busy reports whether client has a request outstanding.
func (w *Workload) Busy(client int) bool { return w.clients[client].busy }

// Think draws a think time.
func (w *Workload) Think() core.Duration {
	return core.Duration(w.rng.Between(int64(w.cfg.Think[0]), int64(w.cfg.Think[1])))
}

func (w *Workload) key() string { return w.keys[w.rng.Intn(len(w.keys))] }

// Issue builds the next request of an idle client and marks it busy.
func (w *Workload) Issue(client int, now core.Time) core.Request {
	w.nextReq++
	req := w.nextReq
	m := w.cfg.Mix
	total := m.Put + m.Get + m.Delete + m.Copy + m.Invalid
	x := w.rng.Intn(total)
	var op core.Op
	switch {
	case x < m.Put:
		size := int(w.rng.Between(ValueHeader, int64(w.cfg.MaxValueSize)))
		op = core.Put{Key: w.key(), Data: MakeValue(req, size)}
	case x < m.Put+m.Get:
		op = core.Get{Key: w.key()}
	case x < m.Put+m.Get+m.Delete:
		op = core.Delete{Key: w.key()}
	case x < m.Put+m.Get+m.Delete+m.Copy:
		op = core.Copy{Src: w.key(), Dst: w.key()}
	default:
		if w.rng.Intn(2) == 0 {
			op = core.Get{Key: ""}
		} else {
			op = core.Put{Key: "", Data: MakeValue(req, ValueHeader)}
		}
	}
	w.clients[client] = clientState{busy: true, req: req}
	return core.Request{Client: req, Op: op}
}

// Complete marks the client that owns rep.Client idle and returns it. ok is
// false for an unknown or already completed request (a duplicate reply: an
// invariant failure the simulator reports).
func (w *Workload) Complete(rep core.Reply) (client int, ok bool) {
	for i, c := range w.clients {
		if c.busy && c.req == rep.Client {
			w.clients[i] = clientState{}
			return i, true
		}
	}
	return -1, false
}

// Crash abandons every outstanding request (their clients become idle and
// wait for the restart) and returns the abandoned ids.
func (w *Workload) Crash() []core.ClientReq {
	var ids []core.ClientReq
	for i, c := range w.clients {
		if c.busy {
			ids = append(ids, c.req)
			w.clients[i] = clientState{}
		}
	}
	return ids
}

// Idle reports whether no request is outstanding (a quiescent point).
func (w *Workload) Idle() bool {
	for _, c := range w.clients {
		if c.busy {
			return false
		}
	}
	return true
}

// ValueHeader is the size of the self-describing prefix of every value:
// writer ClientReq (8 bytes LE) + total length (4 bytes LE).
const ValueHeader = 12

// MakeValue returns the unique value written by PUT req: ValueHeader followed
// by filler that is a pure function of (req, size). size >= ValueHeader.
func MakeValue(req core.ClientReq, size int) []byte {
	if size < ValueHeader {
		size = ValueHeader
	}
	b := make([]byte, size)
	binary.LittleEndian.PutUint64(b, uint64(req))
	binary.LittleEndian.PutUint32(b[8:], uint32(size))
	x := uint64(req)*0x9e3779b97f4a7c15 + uint64(size)
	for i := ValueHeader; i < size; i++ {
		x = Fold(x, uint64(i))
		b[i] = byte(x)
	}
	return b
}

// ValueWriter parses b's header and returns the PUT that wrote it. ok is
// false unless b is byte-for-byte equal to MakeValue(writer, len(b)), so a
// torn or mixed value is always rejected.
func ValueWriter(b []byte) (writer core.ClientReq, ok bool) {
	if len(b) < ValueHeader || int(binary.LittleEndian.Uint32(b[8:])) != len(b) {
		return 0, false
	}
	writer = core.ClientReq(binary.LittleEndian.Uint64(b))
	want := MakeValue(writer, len(b))
	for i := range b {
		if b[i] != want[i] {
			return 0, false
		}
	}
	return writer, true
}
