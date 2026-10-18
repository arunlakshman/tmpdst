package core

// Control-plane operations as seen by a broker (decision 3). The control plane
// is an HTTP service over a metadata store (SQLite locally, DynamoDB in prod)
// that the broker never sees. Shapes are provisional until the control-plane
// session; the invariants below are not.
//
// Brokers are stateless (decision 44): there is no registration, epoch or
// fencing, and no idempotent producers (decision 41). Offsets are assigned by
// the broker proposing a base per batch and the control plane accepting it by
// compare-and-set (decision 42).
type CPOp interface{ isCPOp() }
type CPResp interface{ isCPResp() }

// CommitSegment registers one uploaded object holding batches for many
// partitions, atomically: every batch is appended or none is.
//
// Each batch names the offset it expects to occupy (Base). The log of a
// partition is a sequence of runs, each [Base, Base+Records). A batch is
// accepted iff its Base is the partition's next offset: the end of its newest
// run (0 for an empty log), counting earlier batches of the same request. A
// Base below that offset is taken (some run already exists there): the whole
// call fails with ErrConflict and changes nothing; the broker re-reads the end
// offsets (EndOffsets or Lookup.End), recomputes the bases and retries. A Base
// above it would leave a hole and is ErrInvalid.
//
// MUST be idempotent per SegmentKey: a replay of a committed key with the same
// request returns the originally accepted ranges (OK); the same key with a
// different request is ErrConflict. A failed commit records nothing, so its
// key is reusable.
type CommitSegment struct {
	SegmentKey string
	Batches    []PendingBatch
}

type PendingBatch struct {
	TP      TopicPartition
	Base    Offset // expected first offset: the partition's next offset when the commit lands
	Records int64  // record count in the batch
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

// EndOffsets reads the next offset of many partitions in one call: the end of
// each partition's newest run (0 for an unknown or empty partition). It is the
// read a broker does before proposing bases. One call lets the store fetch the
// partitions in parallel.
type EndOffsets struct{ TPs []TopicPartition }

// EndOffsetsResp has one entry per requested partition, in request order.
type EndOffsetsResp struct{ Ends []Offset }

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

type DescribeTopicsResp struct{ Topics []TopicInfo }

type TopicInfo struct {
	Name       string
	Partitions int32
}

func (CommitSegment) isCPOp()  {}
func (EndOffsets) isCPOp()     {}
func (Lookup) isCPOp()         {}
func (DescribeTopics) isCPOp() {}

func (CommitSegmentResp) isCPResp()  {}
func (EndOffsetsResp) isCPResp()     {}
func (LookupResp) isCPResp()         {}
func (DescribeTopicsResp) isCPResp() {}
