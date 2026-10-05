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
}

type CommitSegmentResp struct{ Assigned []BatchRange }

// BatchRange is where one partition's batch landed after the commit.
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
func (CommitSegment) isCPOp()  {}
func (Lookup) isCPOp()         {}
func (DescribeTopics) isCPOp() {}

func (RegisterResp) isCPResp()       {}
func (CommitSegmentResp) isCPResp()  {}
func (LookupResp) isCPResp()         {}
func (DescribeTopicsResp) isCPResp() {}
