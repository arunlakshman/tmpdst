package applier

import (
	"encoding/binary"
	"errors"
	"fmt"

	"manifester/core"
)

// Entry kinds are the Dynamo loglet payload kinds.
const (
	KindNoop Kind = iota
	KindCommit
	KindCreateTopic
)

// Kind is the type of a log entry.
type Kind uint8

// Entry is one slot's command payload. KindNoop changes nothing on apply.
type Entry struct {
	ID     string
	Kind   Kind
	Commit core.CommitSegment // KindCommit
	Topic  string             // KindCreateTopic
	Parts  int32              // KindCreateTopic
	// WriterBases, when set and equal in length to Commit.Batches, forces those
	// bases at apply (sim BugWriterOffsets). Not on the wire.
	WriterBases []core.Offset
}

// ErrCorrupt means the bytes are not a valid Entry.
var ErrCorrupt = errors.New("applier: corrupt entry")

type encoder struct {
	buf []byte
	tab []string
	idx map[string]int
}

func (e *encoder) u(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }
func (e *encoder) i(v int64)  { e.buf = binary.AppendVarint(e.buf, v) }
func (e *encoder) s(v string) { e.u(uint64(len(v))); e.buf = append(e.buf, v...) }

func (e *encoder) ref(v string) uint64 {
	if n, ok := e.idx[v]; ok {
		return uint64(n)
	}
	e.idx[v] = len(e.tab)
	e.tab = append(e.tab, v)
	return uint64(len(e.tab) - 1)
}

// Encode serialises an entry.
func Encode(en Entry) []byte {
	e := &encoder{idx: map[string]int{}}
	e.u(uint64(en.Kind))
	e.s(en.ID)
	switch en.Kind {
	case KindCommit:
		e.commit(en.Commit)
	case KindCreateTopic:
		e.s(en.Topic)
		e.i(int64(en.Parts))
	}
	return e.buf
}

func (e *encoder) commit(c core.CommitSegment) {
	e.s(c.SegmentKey)
	head := e.buf
	e.buf = nil
	for _, b := range c.Batches {
		e.u(e.ref(b.TP.Topic))
		e.i(int64(b.TP.Partition))
		e.i(b.Records)
		e.u(e.ref(b.Loc.Key))
		e.i(b.Loc.Off)
		e.i(b.Loc.Len)
	}
	body := e.buf
	e.buf = head
	e.u(uint64(len(e.tab)))
	for _, t := range e.tab {
		e.s(t)
	}
	e.u(uint64(len(c.Batches)))
	e.buf = append(e.buf, body...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) u() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) i() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) s() string {
	n := d.u()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err = ErrCorrupt
		return ""
	}
	v := string(d.b[:n])
	d.b = d.b[n:]
	return v
}

// Decode parses an entry made by Encode.
func Decode(b []byte) (Entry, error) {
	d := &decoder{b: b}
	en := Entry{Kind: Kind(d.u())}
	en.ID = d.s()
	switch en.Kind {
	case KindNoop:
	case KindCommit:
		en.Commit = d.commit()
	case KindCreateTopic:
		en.Topic = d.s()
		en.Parts = int32(d.i())
	default:
		d.err = ErrCorrupt
	}
	if d.err == nil && len(d.b) != 0 {
		d.err = ErrCorrupt
	}
	if d.err != nil {
		return Entry{}, d.err
	}
	return en, nil
}

func (d *decoder) commit() core.CommitSegment {
	c := core.CommitSegment{SegmentKey: d.s()}
	tab := make([]string, 0, 8)
	for n := d.u(); n > 0 && d.err == nil; n-- {
		tab = append(tab, d.s())
	}
	n := d.u()
	if n > uint64(len(d.b)) {
		d.err = ErrCorrupt
		return c
	}
	c.Batches = make([]core.PendingBatch, 0, n)
	str := func() string {
		i := d.u()
		if i >= uint64(len(tab)) {
			d.err = ErrCorrupt
			return ""
		}
		return tab[i]
	}
	for ; n > 0 && d.err == nil; n-- {
		var b core.PendingBatch
		b.TP.Topic = str()
		b.TP.Partition = int32(d.i())
		b.Records = d.i()
		b.Loc.Key = str()
		b.Loc.Off = d.i()
		b.Loc.Len = d.i()
		c.Batches = append(c.Batches, b)
	}
	return c
}

// NewMem returns an applier over an in-memory KV. No Pebble. Suitable for the
// DSM Step fold and for tests that need the pure command fold.
func NewMem() *Applier { return New(NewMemKV()) }

// Apply folds one log entry at slot into the store. No-ops leave state unchanged.
// A store failure panics (fail-stop), matching Commit.
func (a *Applier) Apply(slot int64, e Entry) core.CPDone {
	switch e.Kind {
	case KindNoop:
		return core.CPDone{Req: core.ReqID(slot), Outcome: core.Applied}
	case KindCommit:
		if len(e.WriterBases) == len(e.Commit.Batches) && len(e.WriterBases) > 0 {
			d := a.commitWithBases(slot, e.Commit, e.WriterBases)
			d.Req = core.ReqID(slot)
			return d
		}
		d, err := a.Commit(slot, e.Commit)
		if err != nil {
			panic(fmt.Errorf("applier: store failed: %w", err))
		}
		d.Req = core.ReqID(slot)
		return d
	case KindCreateTopic:
		return core.CPDone{Req: core.ReqID(slot), Err: a.CreateTopic(e.Topic, e.Parts)}
	default:
		return core.CPDone{Req: core.ReqID(slot), Err: core.ErrInvalid}
	}
}

// ApplyBytes decodes data then Apply. Returns ErrCorrupt without mutating state.
func (a *Applier) ApplyBytes(slot int64, data []byte) (core.CPDone, error) {
	e, err := Decode(data)
	if err != nil {
		return core.CPDone{}, err
	}
	return a.Apply(slot, e), nil
}
