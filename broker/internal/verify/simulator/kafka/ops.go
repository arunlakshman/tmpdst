package kafka

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
)

// History ops. One ProduceOp covers a batch and all its retries (the id and
// the value of the history operation are the batch's id); one ReadOp is one
// Fetch request.

// Op is the sum of the operations the clients record in the history.
//
//sumtype:decl
type Op interface{ isOp() }

// Res is the sum of the results of the operations.
//
//sumtype:decl
type Res interface{ isRes() }

func (ProduceOp) isOp()   {}
func (ReadOp) isOp()      {}
func (ProduceRes) isRes() {}
func (ReadRes) isRes()    {}

// History is the history of the Kafka scenario.
type History = history.History[Op, Res]

// Operation is one operation of the history of the Kafka scenario.
type Operation = history.Operation[Op, Res]

// ProduceOp is one record batch for one partition. Vals are the record values,
// unique across the run ("c<client>.b<batch>.r<i>|pad"). Every retry resends
// the same bytes. Idem marks a batch that claims a producer id (the broker
// must reject it).
type ProduceOp struct {
	Client, Batch  int
	TopicPartition core.TopicPartition
	Vals           []string
	Idem           bool
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
	TopicPartition core.TopicPartition
	Off            int64
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
	Corrupt bool // a returned batch failed kafkawire.ParseBatch (CRC or structure)
}

// Rec is one returned record: its offset and value.
type Rec struct {
	Off int64
	Val string
}
