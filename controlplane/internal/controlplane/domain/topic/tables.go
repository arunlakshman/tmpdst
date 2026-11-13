package topic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The tables of the topic context: its keys, the View over a transaction,
// Apply, which writes events, and the reads. Layout (all keys are prefix-free
// per kind):
//
//	T name                         topic -> partition count
//	E tp                           partition -> end offset (next offset)
//	R tp base(8, big endian)       one packed run per (partition, segment commit)
//	K segment-key                  idempotency record: slot, request digest, bases
//	S slot(8) segment-key          window index, so Trim can drop old K records
//
// where tp is uvarint(len topic), topic, partition (4, big endian). A run holds
// the contiguous batches one commit assigned to one partition, packed as a key
// table followed by varints per batch, about 6 B per batch.

const (
	prefixTopic     = 'T'
	prefixEndOffset = 'E'
	prefixRun       = 'R'
	prefixKey       = 'K'
	prefixSlot      = 'S'
)

func topicKey(name string) []byte { return append([]byte{prefixTopic}, name...) }

// topicPartitionPrefix is the prefix-free key prefix of one partition for kind p.
func topicPartitionPrefix(p byte, topicPartition contract.TopicPartition) []byte {
	return engine.AppendTopicPartition([]byte{p}, topicPartition)
}

func endKey(topicPartition contract.TopicPartition) []byte {
	return topicPartitionPrefix(prefixEndOffset, topicPartition)
}

func batchRunKey(topicPartition contract.TopicPartition, base int64) []byte {
	return binary.BigEndian.AppendUint64(topicPartitionPrefix(prefixRun, topicPartition), uint64(base))
}

// batchRunBase is the base offset in an R key.
func batchRunBase(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[len(key)-8:]))
}

func duplicateCheckKey(segKey string) []byte { return append([]byte{prefixKey}, segKey...) }

func slotKey(slot int64, segKey string) []byte {
	return append(binary.BigEndian.AppendUint64([]byte{prefixSlot}, uint64(slot)), segKey...)
}

// txnView is the View over a transaction.
type txnView struct{ transaction engine.Txn }

func (v txnView) Topic(name string) (int32, bool) {
	b := v.transaction.Get(topicKey(name))
	if b == nil {
		return 0, false
	}
	return int32(readVarint(b)), true
}

func (v txnView) End(topicPartition contract.TopicPartition) int64 {
	return readVarint(v.transaction.Get(endKey(topicPartition)))
}

// Commit reads the K record of a key. A corrupt record stops the node
// (fail-stop on corrupt state).
func (v txnView) Commit(segmentKey string) (CommitRecord, bool) {
	b := v.transaction.Get(duplicateCheckKey(segmentKey))
	if b == nil {
		return CommitRecord{}, false
	}
	dig, bases, err := parseDuplicateCheck(b)
	if err != nil {
		panic(fmt.Errorf("topics: key %q: %w", segmentKey, err))
	}
	return CommitRecord{Digest: dig, Bases: bases}, true
}

// Apply writes event to the tables.
func Apply(transaction engine.Txn, event Event) {
	switch e := event.(type) {
	case SegmentCommitted:
		applyCommit(transaction, e)
	case TopicCreated:
		transaction.Set(topicKey(e.Topic), writeVarint(int64(e.Partitions)))
	case PartitionsGrown:
		transaction.Set(topicKey(e.Topic), writeVarint(int64(e.Partitions)))
	default:
		panic(fmt.Sprintf("topics: unknown event %T", event))
	}
}

// applyCommit writes a new segment: the new end and one packed run of each
// partition, in the order the partitions first appear, then the K record and
// its S index.
func applyCommit(transaction engine.Txn, e SegmentCommitted) {
	var order []contract.TopicPartition
	runs := map[contract.TopicPartition][]contract.BatchRange{} // lookups only
	ends := map[contract.TopicPartition]int64{}                 // lookups only
	for _, r := range e.Assigned {
		if _, seen := runs[r.TopicPartition]; !seen {
			order = append(order, r.TopicPartition)
		}
		runs[r.TopicPartition] = append(runs[r.TopicPartition], r)
		ends[r.TopicPartition] = max(ends[r.TopicPartition], int64(r.Base)+r.Count)
	}
	for _, topicPartition := range order {
		transaction.Set(endKey(topicPartition), writeVarint(ends[topicPartition]))
		transaction.Set(batchRunKey(topicPartition, int64(runs[topicPartition][0].Base)), encodeBatchRun(runs[topicPartition]))
	}
	transaction.Set(duplicateCheckKey(e.SegmentKey), duplicateCheckRecord(e.Slot, e.Digest, e.Assigned))
	transaction.Set(slotKey(e.Slot, e.SegmentKey), []byte{1})
}

// PartitionCount is how many partitions topic has (0 when it does not exist).
// The group context asks it, through a function given at wiring.
func PartitionCount(transaction engine.Txn, topic string) int64 {
	return readVarint(transaction.Get(topicKey(topic)))
}

// InWindow reports whether key belongs to the idempotency window (the K and S
// keys). The state hash leaves these out, because Trim may drop them.
func InWindow(key []byte) bool { return key[0] == prefixKey || key[0] == prefixSlot }

// Trim forgets the idempotency records of commands at slots below slot (the
// log no longer holds them) and returns how many keys it dropped. Offsets and
// runs are untouched. A key forgotten here commits again as a new segment.
func Trim(transaction engine.Txn, slot int64) int {
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

// FindSegments answers a FindSegments from the stored runs: the ranges from
// From on, stopping once MaxBytes of them are found. ErrInvalid when From < 0.
func FindSegments(transaction engine.Txn, operation contract.FindSegments) runtime.ControlPlaneDone {
	if operation.From < 0 {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}
	}
	response := contract.FindSegmentsResp{End: contract.Offset(readVarint(transaction.Get(endKey(operation.TopicPartition))))}
	lo := batchRunKey(operation.TopicPartition, 0)
	hi := engine.PrefixEnd(topicPartitionPrefix(prefixRun, operation.TopicPartition))
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
			if r.Base+contract.Offset(r.Count) <= operation.From {
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
	return runtime.ControlPlaneDone{Resp: response}
}

// DescribeTopics answers a DescribeTopics: the topics asked for (every topic
// when none is named), by name.
func DescribeTopics(transaction engine.Txn, operation contract.DescribeTopics) runtime.ControlPlaneDone {
	var response contract.DescribeTopicsResp
	lo := []byte{prefixTopic}
	transaction.Ascend(lo, engine.PrefixEnd(lo), func(k, v []byte) bool {
		name := string(bytes.Clone(k[1:]))
		if len(operation.Topics) == 0 || slices.Contains(operation.Topics, name) {
			response.Topics = append(response.Topics, contract.TopicInfo{Name: name, Partitions: int32(readVarint(v))})
		}
		return true
	})
	return runtime.ControlPlaneDone{Resp: response}
}
