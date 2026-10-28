package core

// CPOp is a control-plane operation as seen by a broker (decision 3). The wire
// protocol for new work is gRPC (`controlplane/api/v1`, decision 57); CPOp remains
// the in-process vocabulary until cutover. The metadata store is still invisible
// to the broker. The invariants below are not provisional.
//
// Brokers are stateless (decision 44): there is no registration, epoch or
// fencing, and no idempotent producers (decision 41). The control plane
// assigns offsets (decision 42): the broker never proposes one. How a store
// stays correct under concurrent committers (a compare-and-set on the run base
// with an internal retry) is the store's business, not part of this contract.
type CPOp interface{ isCPOp() }

// CPResp is the answer to a CPOp.
type CPResp interface{ isCPResp() }

// CommitSegment registers one uploaded object holding batches for many
// partitions and assigns their offsets atomically: every batch is assigned or
// none is.
//
// Each batch gets Base = the partition's next offset, the end of its newest
// run (0 for an empty log), in request order: a later batch of the same
// partition continues after the earlier one. Offsets are contiguous per
// partition and never reused, whatever the interleaving of committers.
//
// MUST be idempotent per SegmentKey: a replay of a committed key with an
// identical request returns the originally assigned ranges, never new ones
// (so a retry after MaybeApplied is safe); the same key with a different
// request is ErrConflict. That is the only use of ErrConflict.
type CommitSegment struct {
	SegmentKey string
	Batches    []PendingBatch
}

// PendingBatch describes one uploaded batch awaiting offset assignment.
type PendingBatch struct {
	TP      TopicPartition
	Records int64 // record count in the batch
	Loc     SegmentRef
}

// CommitSegmentResp has one entry per request batch, in request order.
type CommitSegmentResp struct{ Assigned []BatchRange }

// BatchRange is one batch of a partition's log: records [Base, Base+Count)
// stored at Loc.
type BatchRange struct {
	TP    TopicPartition
	Base  Offset
	Count int64
	Loc   SegmentRef
}

// Lookup finds which segments hold offsets >= From for a partition.
type Lookup struct {
	TP       TopicPartition
	From     Offset
	MaxBytes int32
}

// LookupResp lists the batches holding offsets >= From in offset order. End is
// the partition end offset (the next offset to be assigned, ListOffsets
// latest), derived from the newest run.
type LookupResp struct {
	Ranges []BatchRange
	End    Offset
}

// DescribeTopics answers Kafka Metadata requests.
type DescribeTopics struct{ Topics []string } // empty = all

// DescribeTopicsResp answers DescribeTopics.
type DescribeTopicsResp struct{ Topics []TopicInfo }

// TopicInfo describes one topic.
type TopicInfo struct {
	Name       string
	Partitions int32
}

func (CommitSegment) isCPOp()  {}
func (Lookup) isCPOp()         {}
func (DescribeTopics) isCPOp() {}

func (CommitSegmentResp) isCPResp()  {}
func (LookupResp) isCPResp()         {}
func (DescribeTopicsResp) isCPResp() {}
