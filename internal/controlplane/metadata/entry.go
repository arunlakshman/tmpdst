package metadata

import (
	"errors"

	"github.com/manifesterio/mds/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/controlplane/topics"
	"github.com/manifesterio/mds/internal/core"
)

// The log format of an entry is: the kind (uvarint), the id (string), and then
// the payload. The kernel writes the kind and the id. The module that owns the
// kind writes and reads the payload, so a new context brings its own encoding
// and the kernel does not change. The bytes of every kind are fixed by the
// golden test (TestGoldenEntryBytes): a log written by an older node must be
// read by a newer one.

// Kind is the kind of a log entry. See module.Kind.
type Kind = module.Kind

// The entry kinds. The kernel owns the no-op (zero). Each other kind belongs to
// the module that defines it.
const (
	KindNoop                 Kind = 0
	KindCommit                    = topics.KindCommit
	KindCreateTopic               = topics.KindCreateTopic
	KindCreateTopicExclusive      = topics.KindCreateTopicExclusive
	KindGroupInstall              = storage.KindGroupInstall
	KindGroupAssign               = storage.KindGroupAssign
	KindGroupRemove               = storage.KindGroupRemove
	KindOffsetCommit              = storage.KindOffsetCommit
)

// Entry is one slot's command payload. KindNoop changes nothing on apply.
type Entry struct {
	ID   string
	Kind Kind
	// Op is the logged operation that the entry carries. It is nil for KindNoop.
	Op core.ControlPlaneOperation
	// WriterBases, when set and equal in length to the commit's batches, forces
	// those bases at apply (sim BugWriterOffsets). Not on the wire.
	WriterBases []core.Offset
}

// CommitSegment is the CommitSegment that the entry carries. It is false for any
// other entry.
func (e Entry) CommitSegment() (core.CommitSegment, bool) {
	c, ok := e.Op.(core.CommitSegment)
	return c, ok
}

// ErrCorrupt means the bytes are not a valid Entry.
var ErrCorrupt = errors.New("metadata: corrupt entry")

// registry is the list of modules, and which module owns each entry kind. Every
// Store uses the same list, and so does the codec.
var registry = newRegistry(topics.Module{}, storage.NewModule(topics.PartitionCount))

type modules struct {
	list  []module.Module
	owner map[Kind]module.Module
}

func newRegistry(list ...module.Module) modules {
	r := modules{list: list, owner: map[Kind]module.Module{}}
	for _, m := range list {
		for _, k := range m.Kinds() {
			r.owner[k] = m
		}
	}
	return r
}

// EntryOf is the log entry of a logged operation. It returns false for an
// operation that is not logged (a read, or an unknown one).
func EntryOf(operation core.ControlPlaneOperation) (Entry, bool) {
	for _, m := range registry.list {
		if kind, ok := m.KindOf(operation); ok {
			return Entry{Kind: kind, Op: operation}, true
		}
	}
	return Entry{}, false
}

// CreateEntry is the log entry of a CreateTopic.
func CreateEntry(operation core.CreateTopic) Entry {
	e, _ := EntryOf(operation)
	return e
}

// Encode serialises an entry.
func Encode(en Entry) []byte {
	w := kv.NewWriter()
	w.Uvarint(uint64(en.Kind))
	w.String(en.ID)
	if m := registry.owner[en.Kind]; m != nil {
		m.Encode(w, en.Kind, en.Op)
	}
	return w.Bytes()
}

// Decode parses an entry made by Encode.
func Decode(b []byte) (Entry, error) {
	r := kv.NewReader(b)
	en := Entry{Kind: Kind(r.Uvarint())}
	en.ID = r.String()
	if en.Kind != KindNoop {
		m := registry.owner[en.Kind]
		if m == nil {
			return Entry{}, ErrCorrupt
		}
		en.Op = m.Decode(r, en.Kind)
	}
	if !r.Done() {
		return Entry{}, ErrCorrupt
	}
	return en, nil
}

// NewMem returns a metadata store over an in-memory KV. No Pebble. Suitable for the
// DSM Step fold and for tests that need the pure command fold.
func NewMem() *Store { return New(kv.NewMemory()) }

// Apply folds one log entry at slot into the store. No-ops leave state unchanged.
// A store failure panics (fail-stop), matching Commit.
func (a *Store) Apply(slot int64, e Entry) core.ControlPlaneDone {
	if e.Kind == KindNoop {
		return core.ControlPlaneDone{Req: core.RequestID(slot), Outcome: core.Applied}
	}
	if c, ok := e.Op.(core.CommitSegment); ok && len(e.WriterBases) == len(c.Batches) && len(e.WriterBases) > 0 {
		d := a.commitWithBases(slot, c, e.WriterBases)
		d.Req = core.RequestID(slot)
		return d
	}
	if e.Op == nil {
		return core.ControlPlaneDone{Req: core.RequestID(slot), Err: core.ErrInvalid}
	}
	d, err := a.Write(slot, e.Op)
	mustStore(err)
	d.Req = core.RequestID(slot)
	return d
}

// ApplyBytes decodes data then Apply. Returns ErrCorrupt without mutating state.
func (a *Store) ApplyBytes(slot int64, data []byte) (core.ControlPlaneDone, error) {
	e, err := Decode(data)
	if err != nil {
		return core.ControlPlaneDone{}, err
	}
	return a.Apply(slot, e), nil
}
