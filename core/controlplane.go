package core

// Control-plane operations as seen by a broker (decision 3). The control plane
// is an HTTP service over a metadata store (SQLite locally, DynamoDB in prod)
// that the broker never sees. Shapes are provisional until the control-plane
// session; the invariants below are not.
type CPOp interface{ isCPOp() }
type CPResp interface{ isCPResp() }

// Register announces a new boot and returns its fencing epoch.
type Register struct{ Self NodeRef }
type RegisterResp struct{ Epoch Epoch }

// CommitSegment registers one uploaded object holding batches for many
// partitions and assigns their offsets atomically.
// MUST be idempotent per SegmentKey: a retry after MaybeApplied returns the
// originally assigned offsets, never new ones.
type CommitSegment struct {
	SegmentKey string
	Owner      NodeRef
	Epoch      Epoch
	Batches    []PendingBatch
}

type PendingBatch struct {
	TP      TopicPartition
	Records int64 // record count in the batch
	Loc     SegmentRef
	// Idempotent producer identity (decision 28). The zero value (NoProducer)
	// means a non-idempotent batch: never deduplicated.
	Producer ProducerID
	PEpoch   int16
	BaseSeq  int32
}

// ProducerID is a Kafka producer ID allocated by the control plane.
type ProducerID int64

// NoProducer marks a batch without idempotence. It is the zero value so a
// forgotten field is safe; the broker maps Kafka's -1 to it. The control plane
// allocates real IDs from 1000.
const NoProducer ProducerID = 0

// CommitSegmentResp has one entry per request batch, in request order.
type CommitSegmentResp struct{ Assigned []BatchRange }

// BatchRange is where one partition's batch landed after the commit.
//
// Per-batch idempotence (decision 28): Err is OK for an assigned batch.
// Dup means the (Producer, PEpoch, BaseSeq) batch was already committed by an
// earlier segment; Base/Count/Loc then name that original commit and this
// segment's copy of the bytes is orphaned. Err == ErrOutOfOrder or
// ErrFenced (stale PEpoch) means the batch was not assigned; Base is -1.
type BatchRange struct {
	TP    TopicPartition
	Base  Offset
	Count int64
	Loc   SegmentRef
	Err   ErrCode
	Dup   bool
}

// InitProducer allocates a fresh idempotent producer ID with PEpoch 0. Not
// idempotent itself: a retry after MaybeApplied may allocate another ID, which
// is harmless (an unused ID is never referenced).
type InitProducer struct{}
type InitProducerResp struct {
	Producer ProducerID
	PEpoch   int16
}

// Lookup finds which segments hold offsets >= From for a partition.
type Lookup struct {
	TP       TopicPartition
	From     Offset
	MaxBytes int32
}

type LookupResp struct {
	Ranges        []BatchRange
	HighWatermark Offset
}

// DescribeTopics answers Kafka Metadata requests.
type DescribeTopics struct{ Topics []string } // empty = all

type DescribeTopicsResp struct{ Topics []TopicInfo }

type TopicInfo struct {
	Name       string
	Partitions int32
}

func (Register) isCPOp()       {}
func (InitProducer) isCPOp()   {}
func (CommitSegment) isCPOp()  {}
func (Lookup) isCPOp()         {}
func (DescribeTopics) isCPOp() {}

func (RegisterResp) isCPResp()       {}
func (InitProducerResp) isCPResp()   {}
func (CommitSegmentResp) isCPResp()  {}
func (LookupResp) isCPResp()         {}
func (DescribeTopicsResp) isCPResp() {}
