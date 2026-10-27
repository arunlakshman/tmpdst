package logsm

import (
	"encoding/binary"
	"errors"

	"manifester/core"
)

// The entry codec is compact on purpose: topic names and segment-object keys
// repeat per batch, so each entry carries a string table and batches refer to
// it by index. Layout, all integers uvarint (zigzag for signed):
//
//	kind, id, then per kind:
//	commit       key, table size, table strings, batch count, then per batch
//	             topic index, partition, records, loc key index, off, len
//	create topic name, partitions
var errCorrupt = errors.New("logsm: corrupt entry")

type encoder struct {
	buf []byte
	tab []string
	idx map[string]int
}

func (e *encoder) u(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }
func (e *encoder) i(v int64)  { e.buf = binary.AppendVarint(e.buf, v) }
func (e *encoder) s(v string) { e.u(uint64(len(v))); e.buf = append(e.buf, v...) }

// ref returns the table index of v, adding it on first use.
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
	// Batches first into a scratch buffer, so the table (known only after all
	// refs) can precede them.
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
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) i() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) s() string {
	n := d.u()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err = errCorrupt
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
		d.err = errCorrupt
	}
	if d.err == nil && len(d.b) != 0 {
		d.err = errCorrupt // trailing bytes
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
	if n > uint64(len(d.b)) { // every batch takes at least one byte
		d.err = errCorrupt
		return c
	}
	c.Batches = make([]core.PendingBatch, 0, n)
	str := func() string {
		i := d.u()
		if i >= uint64(len(tab)) {
			d.err = errCorrupt
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
