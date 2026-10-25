package logcp

import (
	"encoding/binary"
	"errors"
	"fmt"

	"manifester/cp/logsm"
)

// Slot format (docs/log-cp-service.md "Slot format and versioning"). A slot is
// one DynamoDB item: pk "LOG", sk the slot as 20 digits, and
//
//	d    B  the entry, MarshalEntry (below)
//	v    N  format version of d (informational copy of its first byte)
//	w    S  writer name, id S append id, ts N writer clock in ms, n N commands
//	ttl  N  epoch seconds after which DynamoDB may delete the item
//
// MarshalEntry: version byte, uvarint rank, writer, id, varint ts, uvarint
// command count, then per command a uvarint length and the cp/logsm command
// encoding (kind, key, string table, batches).
const (
	// Version is the highest entry format this build reads and the default
	// it writes.
	Version = 1
	// MaxEntryBytes is the size guard of one slot's item (DynamoDB allows 400 KB).
	MaxEntryBytes = 350 << 10
	// itemOverhead is the allowance for keys and the informational attributes.
	itemOverhead = 256
)

// ErrEntryTooLarge means the entry does not fit in one slot.
var ErrEntryTooLarge = errors.New("logcp: entry larger than one slot")

// UnsupportedError is a slot written in a format newer than this build reads.
// A replica that meets it stops applying (it must not skip a slot): upgrade
// the node.
type UnsupportedError struct{ Version int }

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("logcp: slot format v%d is newer than this build reads (v%d)", e.Version, Version)
}

var errCorrupt = errors.New("logcp: corrupt slot")

// ErrTrimmed means the slot was trimmed. Trimming overwrites a slot with a
// tombstone instead of deleting it, so a node that is behind the trim point
// (a reader, or a writer whose slot number is stale) finds a tombstone where
// it expected a slot, never an empty slot it could fill. Such a node must
// restart and restore from the checkpoint.
var ErrTrimmed = errors.New("logcp: the log below this node was trimmed; restart to restore from the checkpoint")

// tombstone is the payload of a trimmed slot: one byte that is no format version.
var tombstone = []byte{0xFE}

// Entry is the value of one slot: the commands of one append, in order. No
// commands is a no-op (a hole filler or a heartbeat).
type Entry struct {
	Writer string // node name
	Rank   int    // the writer's preference rank (0 is the preferred appender)
	ID     string // names the append attempt: a writer recognises its own entry by it
	TS     int64  // the writer's clock, ms: advisory, never part of the state
	Cmds   []logsm.Entry

	raw [][]byte // the encoded commands, when the writer already has them
}

// Slot is an entry with its position.
type Slot struct {
	N int64
	E Entry
}

// MarshalEntry encodes e as format version ver (1 is the only one).
func MarshalEntry(e Entry, ver byte) []byte {
	b := []byte{ver}
	b = binary.AppendUvarint(b, uint64(e.Rank))
	b = appendStr(b, e.Writer)
	b = appendStr(b, e.ID)
	b = binary.AppendVarint(b, e.TS)
	b = binary.AppendUvarint(b, uint64(len(e.Cmds)))
	for i, c := range e.Cmds {
		var enc []byte
		if i < len(e.raw) {
			enc = e.raw[i]
		} else {
			enc = logsm.Encode(c)
		}
		b = binary.AppendUvarint(b, uint64(len(enc)))
		b = append(b, enc...)
	}
	return b
}

func appendStr(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) uv() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errCorrupt
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) sv() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = errCorrupt
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.uv()
	if r.err != nil || n > uint64(len(r.b)) {
		r.err = errCorrupt
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// UnmarshalEntry decodes MarshalEntry's output. A version above Version is an
// *UnsupportedError.
func UnmarshalEntry(b []byte) (Entry, error) {
	if len(b) == 0 {
		return Entry{}, errCorrupt
	}
	if len(b) == 1 && b[0] == tombstone[0] {
		return Entry{}, ErrTrimmed
	}
	if b[0] == 0 {
		return Entry{}, errCorrupt
	}
	if b[0] > Version {
		return Entry{}, &UnsupportedError{Version: int(b[0])}
	}
	r := &reader{b: b[1:]}
	e := Entry{Rank: int(r.uv())}
	e.Writer = string(r.bytes())
	e.ID = string(r.bytes())
	e.TS = r.sv()
	n := r.uv()
	if r.err != nil || n > uint64(len(r.b)) {
		return Entry{}, errCorrupt
	}
	for ; n > 0 && r.err == nil; n-- {
		c, err := logsm.Decode(r.bytes())
		if err != nil {
			return Entry{}, err
		}
		e.Cmds = append(e.Cmds, c)
	}
	if r.err != nil || len(r.b) != 0 {
		return Entry{}, errCorrupt
	}
	return e, nil
}

// EntrySize is the stored size of e, with the allowance for item overhead.
func EntrySize(e Entry) int { return len(MarshalEntry(e, Version)) + itemOverhead }
