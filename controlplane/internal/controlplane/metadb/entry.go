package metadb

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	logv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/log/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// A log entry is the protobuf message logv1.Entry: an id and at most one
// command. Encode writes it with deterministic marshaling. Decode reads any
// valid protobuf bytes of an Entry, by the rules of the wire format, but an
// entry with a field that logv1 does not define is ErrCorrupt: no node may
// skip a part of a command. Decode does not ask for the exact bytes that
// Encode writes, because protobuf does not promise the same bytes in another
// version; a log written by one build must be readable by the next. The
// conversion to and from the contract types is in entry_codec.go.
//
// Only commands are in the log. What a command does (for example the offsets of
// a CommitSegment) is decided when it is applied, on every replica.

// Kind is the kind of a log entry. It is not in the bytes: Decode sets it from
// the command that the entry holds.
type Kind uint8

// The entry kinds. KindNoop is an entry with no command.
const (
	KindNoop Kind = iota
	KindCommit
	KindCreateTopic
	KindGroupInstall
	KindGroupAssign
	KindGroupRemove
	KindOffsetCommit
	// KindCreateTopicExclusive is a CreateTopic with FailIfExists.
	KindCreateTopicExclusive
)

// Entry is one slot's command payload. KindNoop changes nothing on apply.
type Entry struct {
	ID   string
	Kind Kind
	// Op is the logged operation that the entry carries. It is nil for KindNoop.
	Op runtime.ControlPlaneOperation
}

// CommitSegment is the CommitSegment that the entry carries. It is false for any
// other entry.
func (e Entry) CommitSegment() (contract.CommitSegment, bool) {
	c, ok := e.Op.(contract.CommitSegment)
	return c, ok
}

// ErrCorrupt means the bytes are not a valid Entry.
var ErrCorrupt = errors.New("metadata: corrupt entry")

// EntryOf is the log entry of a logged operation. It returns false for an
// operation that is not logged (a read, or an unknown one).
func EntryOf(operation runtime.ControlPlaneOperation) (Entry, bool) {
	kind := kindOf(operation)
	if kind == KindNoop {
		return Entry{}, false
	}
	return Entry{Kind: kind, Op: operation}, true
}

// Encode serialises an entry. It reads ID and Op, not Kind: an entry with no Op
// is a no-op. An Op that is not a logged operation, or a string that is not
// valid UTF-8, is a bug in the caller, and Encode panics. Every command comes in
// through gRPC, which accepts only valid UTF-8 strings.
func Encode(en Entry) []byte {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(entryToLog(en))
	if err != nil {
		panic(fmt.Errorf("metadata: encode entry %q: %w", en.ID, err))
	}
	return b
}

// Decode parses an entry made by Encode. It never panics: bad bytes give
// ErrCorrupt.
func Decode(b []byte) (Entry, error) {
	var m logv1.Entry
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(b, &m); err != nil {
		return Entry{}, ErrCorrupt
	}
	if hasUnknown(m.ProtoReflect()) {
		return Entry{}, ErrCorrupt
	}
	return entryFromLog(&m), nil
}

// hasUnknown reports whether m, or a message inside it, has fields that its
// type does not define. Range visits the fields in no fixed order, but the
// answer is only yes or no, so the order does not matter.
func hasUnknown(m protoreflect.Message) bool {
	if len(m.GetUnknown()) > 0 {
		return true
	}
	found := false
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.Message() == nil:
		case fd.IsList():
			for i := range v.List().Len() {
				found = found || hasUnknown(v.List().Get(i).Message())
			}
		default:
			found = hasUnknown(v.Message())
		}
		return !found
	})
	return found
}

// NewMem returns a metadb over an in-memory KV. No Pebble. Suitable for the
// replica Step and for tests that need the pure command fold.
func NewMem() *DB { return New(memory.New()) }

// Apply folds one log entry at slot into the store. No-ops leave state unchanged.
// A store failure panics (fail-stop).
func (a *DB) Apply(slot int64, e Entry) runtime.ControlPlaneDone {
	if e.Kind == KindNoop {
		return runtime.ControlPlaneDone{Req: runtime.RequestID(slot), Outcome: runtime.Applied}
	}
	if e.Op == nil {
		return runtime.ControlPlaneDone{Req: runtime.RequestID(slot), Err: runtime.ErrInvalid}
	}
	d, err := a.Write(slot, e.Op)
	mustStore(err)
	d.Req = runtime.RequestID(slot)
	return d
}

// ApplyBytes decodes data then Apply. Returns ErrCorrupt without mutating state.
func (a *DB) ApplyBytes(slot int64, data []byte) (runtime.ControlPlaneDone, error) {
	e, err := Decode(data)
	if err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	return a.Apply(slot, e), nil
}
