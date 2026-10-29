package kafka

import "manifester/internal/core"

// History ops. One ProduceOp covers a batch and all its retries (HistOp.ID and
// Value are the batch's id); one ReadOp is one Fetch request.

// ProduceOp is one record batch for one partition. Vals are the record values,
// unique across the run ("c<client>.b<batch>.r<i>|pad"). Every retry resends
// the same bytes. Idem marks a batch that claims a producer id (the broker
// must reject it).
type ProduceOp struct {
	Client, Batch int
	TP            core.TopicPartition
	Vals          []string
	Idem          bool
}

// ProduceRes is a produce op's result. Base is the acked base offset (-1
// unless OpDefinite success); Code is the last Kafka error seen (0 none).
type ProduceRes struct {
	Base     int64
	Code     int16
	Why      string
	Attempts int
}

// ReadOp is one Fetch: the partitions asked and the offset asked for each.
type ReadOp struct {
	Client int
	Parts  []ReadFrom
}

// ReadFrom names one partition and the offset to fetch from.
type ReadFrom struct {
	TP  core.TopicPartition
	Off int64
}

// ReadRes is the Fetch answer, per asked partition in order. Records are the
// ones at or above the fetch offset (the broker returns whole batches). Why is
// set when the fetch got no answer (OpAbandoned).
type ReadRes struct {
	Parts []ReadPart
	Why   string
}

// ReadPart is the Fetch answer for one partition.
type ReadPart struct {
	Code    int16
	HWM     int64
	Recs    []Rec
	Corrupt bool // a returned batch failed kwire.ParseBatch (CRC or structure)
}

// Rec is one returned record: its offset and value.
type Rec struct {
	Off int64
	Val string
}
