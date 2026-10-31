package stateupdater

import (
	"encoding/binary"
	"errors"
	"fmt"

	"mds/internal/core"
)

// Entry kinds are the Dynamo loglet payload kinds.
const (
	KindNoop Kind = iota
	KindCommit
	KindCreateTopic
	KindGroupInstall
	KindGroupAssign
	KindGroupRemove
	KindOffsetCommit
	// KindCreateTopicExclusive is CreateTopic with FailIfExists. It is a new
	// kind, not a flag on KindCreateTopic, so old log entries keep their bytes.
	KindCreateTopicExclusive
)

// Kind is the type of a log entry.
type Kind uint8

// Entry is one slot's command payload. KindNoop changes nothing on apply.
type Entry struct {
	ID     string
	Kind   Kind
	Commit core.CommitSegment // KindCommit
	Topic  string             // KindCreateTopic, KindCreateTopicExclusive
	Parts  int32              // KindCreateTopic, KindCreateTopicExclusive
	// Group commands (KindGroupInstall, KindGroupAssign, KindGroupRemove,
	// KindOffsetCommit). Only the field of the kind is used.
	Install core.GroupInstall
	Assign  core.GroupAssign
	Remove  core.GroupRemove
	Offsets core.OffsetCommit
	// WriterBases, when set and equal in length to Commit.Batches, forces those
	// bases at apply (sim BugWriterOffsets). Not on the wire.
	WriterBases []core.Offset
}

// ErrCorrupt means the bytes are not a valid Entry.
var ErrCorrupt = errors.New("stateupdater: corrupt entry")

type entryEncoder struct {
	buf []byte
	tab []string
	idx map[string]int
}

func (e *entryEncoder) u(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }
func (e *entryEncoder) i(v int64)  { e.buf = binary.AppendVarint(e.buf, v) }
func (e *entryEncoder) s(v string) { e.u(uint64(len(v))); e.buf = append(e.buf, v...) }

func (e *entryEncoder) ref(v string) uint64 {
	if n, ok := e.idx[v]; ok {
		return uint64(n)
	}
	e.idx[v] = len(e.tab)
	e.tab = append(e.tab, v)
	return uint64(len(e.tab) - 1)
}

// Encode serialises an entry.
func Encode(en Entry) []byte {
	e := &entryEncoder{idx: map[string]int{}}
	e.u(uint64(en.Kind))
	e.s(en.ID)
	switch en.Kind {
	case KindCommit:
		e.commit(en.Commit)
	case KindCreateTopic, KindCreateTopicExclusive:
		e.s(en.Topic)
		e.i(int64(en.Parts))
	case KindGroupInstall:
		e.groupInstall(en.Install)
	case KindGroupAssign:
		e.groupAssign(en.Assign)
	case KindGroupRemove:
		e.groupRemove(en.Remove)
	case KindOffsetCommit:
		e.offsetCommit(en.Offsets)
	}
	return e.buf
}

func (e *entryEncoder) groupInstall(operation core.GroupInstall) {
	e.s(operation.Group)
	e.s(operation.ProtocolType)
	e.s(operation.Protocol)
	e.i(int64(operation.Generation))
	e.s(operation.Leader)
	e.i(operation.TimeMs)
	e.u(uint64(len(operation.Members)))
	for _, m := range operation.Members {
		e.s(m.MemberID)
		e.s(m.ClientID)
		e.s(m.ClientHost)
		e.s(m.GroupInstanceID)
		e.i(int64(m.SessionTimeoutMs))
		e.i(int64(m.RebalanceTimeoutMs))
		e.s(string(m.Metadata))
	}
}

func (e *entryEncoder) groupAssign(operation core.GroupAssign) {
	e.s(operation.Group)
	e.i(int64(operation.Generation))
	e.s(operation.Leader)
	e.i(operation.TimeMs)
	e.u(uint64(len(operation.Assignments)))
	for _, a := range operation.Assignments {
		e.s(a.Member)
		e.s(string(a.Bytes))
	}
}

func (e *entryEncoder) groupRemove(operation core.GroupRemove) {
	e.s(operation.Group)
	e.i(operation.TimeMs)
	e.s(operation.Reason)
	e.u(uint64(len(operation.Members)))
	for _, m := range operation.Members {
		e.s(m)
	}
}

func (e *entryEncoder) offsetCommit(operation core.OffsetCommit) {
	e.s(operation.Group)
	e.s(operation.Member)
	e.i(int64(operation.Generation))
	e.i(operation.TimeMs)
	e.u(uint64(len(operation.Offsets)))
	for _, o := range operation.Offsets {
		e.s(o.TopicPartition.Topic)
		e.i(int64(o.TopicPartition.Partition))
		e.i(o.Offset)
		e.i(int64(o.LeaderEpoch))
		e.s(o.Metadata)
	}
}

func (e *entryEncoder) commit(c core.CommitSegment) {
	e.s(c.SegmentKey)
	head := e.buf
	e.buf = nil
	for _, b := range c.Batches {
		e.u(e.ref(b.TopicPartition.Topic))
		e.i(int64(b.TopicPartition.Partition))
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

type entryDecoder struct {
	b   []byte
	err error
}

func (d *entryDecoder) u() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *entryDecoder) i() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *entryDecoder) s() string {
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
	d := &entryDecoder{b: b}
	en := Entry{Kind: Kind(d.u())}
	en.ID = d.s()
	switch en.Kind {
	case KindNoop:
	case KindCommit:
		en.Commit = d.commit()
	case KindCreateTopic, KindCreateTopicExclusive:
		en.Topic = d.s()
		en.Parts = int32(d.i())
	case KindGroupInstall:
		en.Install = d.groupInstall()
	case KindGroupAssign:
		en.Assign = d.groupAssign()
	case KindGroupRemove:
		en.Remove = d.groupRemove()
	case KindOffsetCommit:
		en.Offsets = d.offsetCommit()
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

// count reads a list length; each item takes at least one byte.
func (d *entryDecoder) count() int {
	n := d.u()
	if n > uint64(len(d.b)) {
		d.err = ErrCorrupt
		return 0
	}
	return int(n)
}

// blob reads a byte field; an empty one is nil.
func (d *entryDecoder) blob() []byte {
	s := d.s()
	if s == "" {
		return nil
	}
	return []byte(s)
}

func (d *entryDecoder) groupInstall() core.GroupInstall {
	var operation core.GroupInstall
	operation.Group = d.s()
	operation.ProtocolType = d.s()
	operation.Protocol = d.s()
	operation.Generation = int32(d.i())
	operation.Leader = d.s()
	operation.TimeMs = d.i()
	for n := d.count(); n > 0 && d.err == nil; n-- {
		var m core.GroupMember
		m.MemberID = d.s()
		m.ClientID = d.s()
		m.ClientHost = d.s()
		m.GroupInstanceID = d.s()
		m.SessionTimeoutMs = int32(d.i())
		m.RebalanceTimeoutMs = int32(d.i())
		m.Metadata = d.blob()
		operation.Members = append(operation.Members, m)
	}
	return operation
}

func (d *entryDecoder) groupAssign() core.GroupAssign {
	var operation core.GroupAssign
	operation.Group = d.s()
	operation.Generation = int32(d.i())
	operation.Leader = d.s()
	operation.TimeMs = d.i()
	for n := d.count(); n > 0 && d.err == nil; n-- {
		var a core.GroupAssignment
		a.Member = d.s()
		a.Bytes = d.blob()
		operation.Assignments = append(operation.Assignments, a)
	}
	return operation
}

func (d *entryDecoder) groupRemove() core.GroupRemove {
	var operation core.GroupRemove
	operation.Group = d.s()
	operation.TimeMs = d.i()
	operation.Reason = d.s()
	for n := d.count(); n > 0 && d.err == nil; n-- {
		operation.Members = append(operation.Members, d.s())
	}
	return operation
}

func (d *entryDecoder) offsetCommit() core.OffsetCommit {
	var operation core.OffsetCommit
	operation.Group = d.s()
	operation.Member = d.s()
	operation.Generation = int32(d.i())
	operation.TimeMs = d.i()
	for n := d.count(); n > 0 && d.err == nil; n-- {
		var o core.PartitionOffset
		o.TopicPartition.Topic = d.s()
		o.TopicPartition.Partition = int32(d.i())
		o.Offset = d.i()
		o.LeaderEpoch = int32(d.i())
		o.Metadata = d.s()
		operation.Offsets = append(operation.Offsets, o)
	}
	return operation
}

func (d *entryDecoder) commit() core.CommitSegment {
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
		b.TopicPartition.Topic = str()
		b.TopicPartition.Partition = int32(d.i())
		b.Records = d.i()
		b.Loc.Key = str()
		b.Loc.Off = d.i()
		b.Loc.Len = d.i()
		c.Batches = append(c.Batches, b)
	}
	return c
}

// NewMem returns a state updater over an in-memory KV. No Pebble. Suitable for the
// DSM Step fold and for tests that need the pure command fold.
func NewMem() *StateUpdater { return New(NewMemoryKeyValueStore()) }

// Apply folds one log entry at slot into the store. No-ops leave state unchanged.
// A store failure panics (fail-stop), matching Commit.
func (a *StateUpdater) Apply(slot int64, e Entry) core.ControlPlaneDone {
	switch e.Kind {
	case KindNoop:
		return core.ControlPlaneDone{Req: core.RequestID(slot), Outcome: core.Applied}
	case KindCommit:
		if len(e.WriterBases) == len(e.Commit.Batches) && len(e.WriterBases) > 0 {
			d := a.commitWithBases(slot, e.Commit, e.WriterBases)
			d.Req = core.RequestID(slot)
			return d
		}
		d, err := a.Commit(slot, e.Commit)
		if err != nil {
			panic(fmt.Errorf("stateupdater: store failed: %w", err))
		}
		d.Req = core.RequestID(slot)
		return d
	case KindCreateTopic, KindCreateTopicExclusive:
		err := a.CreateTopicIf(e.Topic, e.Parts, e.Kind == KindCreateTopicExclusive)
		return core.ControlPlaneDone{Req: core.RequestID(slot), Err: err}
	case KindGroupInstall, KindGroupAssign, KindGroupRemove, KindOffsetCommit:
		d, err := a.groupWrite(e.groupOp())
		if err != nil {
			panic(fmt.Errorf("stateupdater: store failed: %w", err))
		}
		d.Req = core.RequestID(slot)
		return d
	default:
		return core.ControlPlaneDone{Req: core.RequestID(slot), Err: core.ErrInvalid}
	}
}

// ApplyBytes decodes data then Apply. Returns ErrCorrupt without mutating state.
func (a *StateUpdater) ApplyBytes(slot int64, data []byte) (core.ControlPlaneDone, error) {
	e, err := Decode(data)
	if err != nil {
		return core.ControlPlaneDone{}, err
	}
	return a.Apply(slot, e), nil
}

// groupOp is the group command of a group entry.
func (e Entry) groupOp() core.ControlPlaneOperation {
	switch e.Kind {
	case KindGroupInstall:
		return e.Install
	case KindGroupAssign:
		return e.Assign
	case KindGroupRemove:
		return e.Remove
	}
	return e.Offsets
}

// CreateEntry is the log entry of a CreateTopic op.
func CreateEntry(operation core.CreateTopic) Entry {
	k := KindCreateTopic
	if operation.FailIfExists {
		k = KindCreateTopicExclusive
	}
	return Entry{Kind: k, Topic: operation.Name, Parts: operation.Partitions}
}

// GroupEntry is the log entry of a group write. It returns false for any
// other operation.
func GroupEntry(operation core.ControlPlaneOperation) (Entry, bool) {
	switch x := operation.(type) {
	case core.GroupInstall:
		return Entry{Kind: KindGroupInstall, Install: x}, true
	case core.GroupAssign:
		return Entry{Kind: KindGroupAssign, Assign: x}, true
	case core.GroupRemove:
		return Entry{Kind: KindGroupRemove, Remove: x}, true
	case core.OffsetCommit:
		return Entry{Kind: KindOffsetCommit, Offsets: x}, true
	}
	return Entry{}, false
}
