// Package topics is the topics and segments context of the control plane: the
// topics and their partition counts, the segment commits that give out offsets,
// the idempotency window of the segment keys, and the lookups of runs. It keeps
// its state in the ordered key-value store under the keys T, E, R, K and S, and
// implements module.Module. The functions run in a transaction the caller opens.
//
// Layout (all keys are prefix-free per kind):
//
//	T name                         topic -> partition count
//	E tp                           partition -> end offset (next offset)
//	R tp base(8, big endian)       one packed run per (partition, segment commit)
//	K segment-key                  idempotency record: slot, request digest, bases
//	S slot(8) segment-key          window index, so Trim can drop old K records
//
// where tp is uvarint(len topic), topic, partition (4, big endian). A run holds
// the contiguous batches one commit assigned to one partition, packed as in
// cp/dynamo/runs.go (key table, then varints per batch), about 6 B per batch.
//
// Idempotency window (decision 48): a K record is kept until Trim drops it,
// that is for as long as the log keeps the command. A replay inside the window
// gets the original ranges (rebuilt from the replayed request and the stored
// bases) or ErrConflict when the request digest differs. After Trim the key is
// forgotten and the same key commits again as a new segment.
//
// Nothing here knows about consumer groups. The groups context asks this one
// how many partitions a topic has, through PartitionCount.
package topics

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

// Known answers operation from the idempotency window when its key is already
// present. ok is false when the key is unknown (the caller must append).
func Known(transaction kv.Transaction, operation core.CommitSegment) (core.ControlPlaneDone, bool) {
	rec := transaction.Get(duplicateCheckKey(operation.SegmentKey))
	if rec == nil {
		return core.ControlPlaneDone{}, false
	}
	return replay(rec, checksum(operation.Batches), operation), true
}

// Lookup answers a Lookup from the stored runs.
func Lookup(transaction kv.Transaction, operation core.FindSegments) core.ControlPlaneDone {
	if operation.From < 0 {
		return core.ControlPlaneDone{Err: core.ErrInvalid}
	}
	return lookupTransaction(transaction, operation)
}

// Describe answers a DescribeTopics.
func Describe(transaction kv.Transaction, operation core.DescribeTopics) core.ControlPlaneDone {
	return describeTransaction(transaction, operation)
}

// Create creates name with at least partitions partitions (never shrinks).
// ErrInvalid for an empty name or partitions <= 0. With failIfExists, an
// existing topic gives ErrTopicExists and nothing changes. The check and the
// write are in one transaction, so the answer depends only on the state before
// this command.
func Create(transaction kv.Transaction, name string, partitions int32, failIfExists bool) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	if failIfExists && transaction.Get(topicKey(name)) != nil {
		return core.ErrTopicExists
	}
	addPartitions(transaction, map[string]int32{}, name, partitions)
	return core.OK
}

// End is the next offset of topicPartition (0 if unknown).
func End(transaction kv.Transaction, topicPartition core.TopicPartition) core.Offset {
	return core.Offset(readVarint(transaction.Get(endKey(topicPartition))))
}

// PartitionCount is how many partitions topic has (0 when it does not exist).
func PartitionCount(transaction kv.Transaction, topic string) int64 {
	return readVarint(transaction.Get(topicKey(topic)))
}

// CommitWithBases is the BugWriterOffsets apply path: the bases come from the
// writer instead of from the partition ends.
func CommitWithBases(transaction kv.Transaction, slot int64, operation core.CommitSegment, bases []core.Offset) core.ControlPlaneDone {
	dig := checksum(operation.Batches)
	if rec := transaction.Get(duplicateCheckKey(operation.SegmentKey)); rec != nil {
		return replay(rec, dig, operation)
	}
	if !validCommit(operation) || len(bases) != len(operation.Batches) {
		return core.ControlPlaneDone{Err: core.ErrInvalid}
	}
	return storeCommit(transaction, slot, operation, dig, assignAt(transaction, operation.Batches, bases))
}

// InWindow reports whether key belongs to the idempotency window (the K and S
// keys). The state hash leaves these out, because Trim may drop them.
func InWindow(key []byte) bool { return key[0] == prefixKey || key[0] == prefixSlot }

// Trim forgets the idempotency records of commands at slots below slot (the
// log no longer holds them) and returns how many keys it dropped. Offsets and
// runs are untouched. A key forgotten here commits again as a new segment.
func Trim(transaction kv.Transaction, slot int64) int {
	hi := binary.BigEndian.AppendUint64([]byte{prefixSlot}, uint64(slot))
	var keysToDrop [][]byte
	transaction.Ascend([]byte{prefixSlot}, hi, func(k, _ []byte) bool {
		keysToDrop = append(keysToDrop, bytes.Clone(k))
		return true
	})
	for _, k := range keysToDrop {
		transaction.Delete(duplicateCheckKey(string(k[9:])))
		transaction.Delete(k)
	}
	return len(keysToDrop)
}

// Window is the number of segment keys currently remembered.
func Window(transaction kv.Transaction) int {
	var n int
	lo := []byte{prefixSlot}
	transaction.Ascend(lo, kv.PrefixEnd(lo), func(_, _ []byte) bool { n++; return true })
	return n
}

// Assigned reports the ranges operation was given if it is the first commit of its
// key at slot, rebuilt from the key's record. An indexer uses it to write the
// run rows of a slot after the fact.
func Assigned(transaction kv.Transaction, slot int64, operation core.CommitSegment) ([]core.BatchRange, AssignState) {
	rec := transaction.Get(duplicateCheckKey(operation.SegmentKey))
	if rec == nil {
		return nil, OffsetOwnerUnknown
	}
	first, _ := binary.Uvarint(rec)
	d := replay(rec, checksum(operation.Batches), operation)
	if int64(first) != slot || d.Err != core.OK {
		return nil, OffsetOwnedElsewhere
	}
	return d.Resp.(core.CommitSegmentResp).Assigned, OffsetOwnedHere
}

// AssignState says what Assigned knows about a command.
type AssignState int

// Assigned results.
const (
	// OffsetOwnerUnknown: no record of the key (invalid, or forgotten by Trim).
	OffsetOwnerUnknown AssignState = iota
	// OffsetOwnedElsewhere: the key committed at another slot, or with another
	// request: this command was a replay or a conflict and assigned nothing.
	OffsetOwnedElsewhere
	// OffsetOwnedHere: the command is the first commit of its key, at this slot.
	OffsetOwnedHere
)

// Module is the topics context in the replicated state: CreateTopic and
// CommitSegment are logged, DescribeTopics and FindSegments are reads. It
// implements module.Module.
type Module struct{}

var _ module.Module = Module{}

// Mode reports how the module treats operation.
func (Module) Mode(operation core.ControlPlaneOperation) module.Mode {
	switch operation.(type) {
	case core.CommitSegment, core.CreateTopic:
		return module.Logged
	case core.FindSegments, core.DescribeTopics:
		return module.ReadOnly
	}
	return module.NotMine
}

// Write applies a CommitSegment or a CreateTopic at slot.
func (Module) Write(transaction kv.Transaction, slot int64, operation core.ControlPlaneOperation) core.ControlPlaneDone {
	switch x := operation.(type) {
	case core.CommitSegment:
		return Commit(transaction, slot, x)
	case core.CreateTopic:
		return core.ControlPlaneDone{Err: Create(transaction, x.Name, x.Partitions, x.FailIfExists)}
	}
	return core.ControlPlaneDone{Err: core.ErrInvalid}
}

// Read answers a FindSegments or a DescribeTopics.
func (Module) Read(transaction kv.Transaction, operation core.ControlPlaneOperation) core.ControlPlaneDone {
	switch x := operation.(type) {
	case core.FindSegments:
		return Lookup(transaction, x)
	case core.DescribeTopics:
		return Describe(transaction, x)
	}
	return core.ControlPlaneDone{Err: core.ErrInvalid}
}

func validBatch(b core.PendingBatch) bool {
	return b.Records > 0 && b.TopicPartition.Topic != "" && b.TopicPartition.Partition >= 0
}

func validCommit(operation core.CommitSegment) bool {
	return operation.SegmentKey != "" && len(operation.Batches) > 0 && slices.IndexFunc(operation.Batches, func(b core.PendingBatch) bool { return !validBatch(b) }) < 0
}

// Commit applies a CommitSegment that sits at log slot slot. A key that is
// already known gets its original answer, or ErrConflict if the request differs.
func Commit(transaction kv.Transaction, slot int64, operation core.CommitSegment) core.ControlPlaneDone {
	dig := checksum(operation.Batches)
	if rec := transaction.Get(duplicateCheckKey(operation.SegmentKey)); rec != nil {
		return replay(rec, dig, operation)
	}
	if !validCommit(operation) {
		return core.ControlPlaneDone{Err: core.ErrInvalid}
	}
	return storeCommit(transaction, slot, operation, dig, assign(transaction, operation.Batches))
}

// replay answers a commit whose key is known: the original ranges, or
// ErrConflict if the request differs.
func replay(rec []byte, dig [checksumLength]byte, operation core.CommitSegment) core.ControlPlaneDone {
	was, bases, err := parseDuplicateCheck(rec)
	if err != nil {
		panic(fmt.Errorf("topics: key %q: %w", operation.SegmentKey, err))
	}
	if was != dig {
		return core.ControlPlaneDone{Err: core.ErrConflict}
	}
	if len(bases) != len(operation.Batches) {
		panic(fmt.Errorf("topics: key %q: %w", operation.SegmentKey, errCorrupt))
	}
	rs := make([]core.BatchRange, len(operation.Batches))
	for i, b := range operation.Batches {
		rs[i] = core.BatchRange{TopicPartition: b.TopicPartition, Base: core.Offset(bases[i]), Count: b.Records, Loc: b.Loc}
	}
	return core.ControlPlaneDone{Resp: core.CommitSegmentResp{Assigned: rs}, Outcome: core.Applied}
}

func storeCommit(transaction kv.Transaction, slot int64, operation core.CommitSegment, dig [checksumLength]byte, assigned []core.BatchRange) core.ControlPlaneDone {
	transaction.Set(duplicateCheckKey(operation.SegmentKey), duplicateCheckRecord(slot, dig, assigned))
	transaction.Set(slotKey(slot, operation.SegmentKey), []byte{1})
	return core.ControlPlaneDone{Resp: core.CommitSegmentResp{Assigned: assigned}, Outcome: core.Applied}
}

// offsetPlan is the per-partition end and run built while handing out offsets.
type offsetPlan struct {
	order []core.TopicPartition
	ends  map[core.TopicPartition]int64
	runs  map[core.TopicPartition][]core.BatchRange
	seen  map[core.TopicPartition]bool
}

func newOffsetPlan() *offsetPlan {
	return &offsetPlan{
		ends: map[core.TopicPartition]int64{},
		runs: map[core.TopicPartition][]core.BatchRange{},
		seen: map[core.TopicPartition]bool{},
	}
}

func (p *offsetPlan) add(b core.PendingBatch, base int64) core.BatchRange {
	if !p.seen[b.TopicPartition] {
		p.order = append(p.order, b.TopicPartition)
		p.seen[b.TopicPartition] = true
	}
	r := core.BatchRange{TopicPartition: b.TopicPartition, Base: core.Offset(base), Count: b.Records, Loc: b.Loc}
	if end := base + b.Records; end > p.ends[b.TopicPartition] {
		p.ends[b.TopicPartition] = end
	}
	p.runs[b.TopicPartition] = append(p.runs[b.TopicPartition], r)
	return r
}

func (p *offsetPlan) write(transaction kv.Transaction) {
	writeAssign(transaction, p.order, p.ends, p.runs)
}

// assign hands out offsets: each batch's Base is its partition's end, in
// request order, the end then growing by Records. It writes the new ends, one
// packed run per partition and grown topics, and returns the ranges.
func assign(transaction kv.Transaction, bs []core.PendingBatch) []core.BatchRange {
	p := newOffsetPlan()
	out := make([]core.BatchRange, 0, len(bs))
	for _, b := range bs {
		base := p.ends[b.TopicPartition]
		if !p.seen[b.TopicPartition] {
			base = readVarint(transaction.Get(endKey(b.TopicPartition)))
		}
		out = append(out, p.add(b, base))
	}
	p.write(transaction)
	return out
}

// assignAt is assign with writer-chosen bases (BugWriterOffsets).
func assignAt(transaction kv.Transaction, bs []core.PendingBatch, bases []core.Offset) []core.BatchRange {
	p := newOffsetPlan()
	out := make([]core.BatchRange, 0, len(bs))
	for i, b := range bs {
		out = append(out, p.add(b, int64(bases[i])))
	}
	p.write(transaction)
	return out
}

func writeAssign(transaction kv.Transaction, order []core.TopicPartition, ends map[core.TopicPartition]int64, runs map[core.TopicPartition][]core.BatchRange) {
	topics := map[string]int32{}
	for _, topicPartition := range order {
		transaction.Set(endKey(topicPartition), writeVarint(ends[topicPartition]))
		transaction.Set(batchRunKey(topicPartition, int64(runs[topicPartition][0].Base)), encodeBatchRun(runs[topicPartition]))
		addPartitions(transaction, topics, topicPartition.Topic, topicPartition.Partition+1)
	}
}

// addPartitions raises topic's partition count to at least n; seen caches the
// counts read in this transaction.
func addPartitions(transaction kv.Transaction, seen map[string]int32, topic string, n int32) {
	cur, ok := seen[topic]
	if !ok {
		cur = int32(readVarint(transaction.Get(topicKey(topic))))
	}
	if n > cur {
		cur = n
		transaction.Set(topicKey(topic), writeVarint(int64(cur)))
	}
	seen[topic] = cur
}

func lookupTransaction(transaction kv.Transaction, operation core.FindSegments) core.ControlPlaneDone {
	response := core.FindSegmentsResp{End: core.Offset(readVarint(transaction.Get(endKey(operation.TopicPartition))))}
	lo := batchRunKey(operation.TopicPartition, 0)
	hi := kv.PrefixEnd(topicPartitionPrefix(prefixRun, operation.TopicPartition))
	if k, _ := transaction.Floor(lo, append(batchRunKey(operation.TopicPartition, int64(operation.From)), 0)); k != nil {
		lo = k // the run holding From starts at or before it
	}
	var size int64
	var bad error
	transaction.Ascend(lo, hi, func(k, v []byte) bool {
		rs, err := decodeBatchRun(operation.TopicPartition, batchRunBase(k), v)
		if err != nil {
			bad = err
			return false
		}
		for _, r := range rs {
			if r.Base+core.Offset(r.Count) <= operation.From {
				continue
			}
			response.Ranges = append(response.Ranges, r)
			size += r.Loc.Len
			if operation.MaxBytes > 0 && size >= int64(operation.MaxBytes) {
				return false
			}
		}
		return true
	})
	if bad != nil {
		panic(fmt.Errorf("topics: %v: %w", operation.TopicPartition, bad))
	}
	return core.ControlPlaneDone{Resp: response}
}

func describeTransaction(transaction kv.Transaction, operation core.DescribeTopics) core.ControlPlaneDone {
	var response core.DescribeTopicsResp
	lo := []byte{prefixTopic}
	transaction.Ascend(lo, kv.PrefixEnd(lo), func(k, v []byte) bool {
		name := string(bytes.Clone(k[1:]))
		if len(operation.Topics) == 0 || slices.Contains(operation.Topics, name) {
			response.Topics = append(response.Topics, core.TopicInfo{Name: name, Partitions: int32(readVarint(v))})
		}
		return true
	})
	return core.ControlPlaneDone{Resp: response}
}
