package broker

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
	"manifester/internal/wire/kwire"
)

// produce admits one batch per partition into the open segment. Acks 1 and -1
// both mean "committed" (acks=0 never gets here: the connection is closed).
func (b *Broker) produce(s *slot, r *kmsg.ProduceRequest) []core.Action {
	resp := produceSkeleton(r)
	s.resp = resp
	var acts []core.Action
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &resp.Topics[i].Partitions[j]
			tp := core.TopicPartitionKey(t.Topic, p.Partition)
			pb, code := b.admit(s, r, tp, p.Records, rp)
			if code != errNone {
				rp.ErrorCode = int16(code)
				continue
			}
			s.pending++
			acts = append(acts, b.addBatch(pb)...)
		}
	}
	return acts
}

func produceSkeleton(r *kmsg.ProduceRequest) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	resp.Topics = make([]kmsg.ProduceResponseTopic, len(r.Topics))
	for i, t := range r.Topics {
		rt := &resp.Topics[i]
		rt.Topic = t.Topic
		rt.Partitions = make([]kmsg.ProduceResponseTopicPartition, len(t.Partitions))
		for j, p := range t.Partitions {
			rp := &rt.Partitions[j]
			rp.Default()
			rp.Partition, rp.BaseOffset, rp.LogStartOffset = p.Partition, -1, 0
		}
	}
	return resp
}

// admit validates one partition's payload and returns the batch to buffer, or
// the Kafka error code that answers the partition.
func (b *Broker) admit(s *slot, r *kmsg.ProduceRequest, tp core.TopicPartition, raw []byte, rp *kmsg.ProduceResponseTopicPartition) (*pbatch, kafkaCode) {
	switch {
	case r.Acks != 0 && r.Acks != 1 && r.Acks != -1:
		return nil, errInvalidRequiredAcks
	case r.TransactionID != nil:
		return nil, errInvalidRequest
	case !b.knows(tp):
		return nil, errUnknownTopicOrPart
	}
	one, err := kwire.OneBatch(raw)
	if err != nil {
		return nil, errCorruptMessage
	}
	bt, code := b.checkBatch(one)
	if code != errNone {
		return nil, code
	}
	b.reachCodec(bt.Codec())
	return &pbatch{s: s, part: rp, tp: tp, data: one, records: int64(bt.NumRecords)}, errNone
}

// checkBatch validates one batch's header without decompressing it
// (decision 45): magic 2, CRC32C over the possibly compressed bytes, a known
// codec, no transaction or control flags, a record count that matches
// lastOffsetDelta, and no producer id (decision 41). The payload stays opaque;
// the broker only ever relies on the header count.
func (b *Broker) checkBatch(one []byte) (kwire.Batch, kafkaCode) {
	bt, err := b.parseProduceBatch(one)
	if err != nil {
		return bt, b.batchParseCode(err)
	}
	return bt, b.batchPolicy(bt)
}

func (b *Broker) parseProduceBatch(one []byte) (kwire.Batch, error) {
	if b.cfg.Bug == BugSkipProduceCRC {
		return kwire.ParseBatchUnverified(one)
	}
	return kwire.ParseBatch(one)
}

func (b *Broker) batchParseCode(err error) kafkaCode {
	switch {
	case errors.Is(err, kwire.ErrUnknownCodec):
		return errUnsupportedCompress
	case errors.Is(err, kwire.ErrUnsupportedBatch):
		return errUnsupportedForFormat
	default:
		b.env.Reachable("broker: corrupt produce batch rejected")
		return errCorruptMessage
	}
}

func (b *Broker) batchPolicy(bt kwire.Batch) kafkaCode {
	switch {
	case bt.IsTransactional() || bt.IsControl():
		return errInvalidRequest
	case bt.NumRecords <= 0 || bt.LastOffsetDelta != bt.NumRecords-1:
		return errCorruptMessage
	case bt.ProducerID >= 0:
		// Idempotent producers are unsupported (decision 41): without the
		// sequence checks a retry would duplicate records, so refuse rather
		// than silently weaken the guarantee. INVALID_REQUEST is not retriable.
		b.env.Reachable("broker: idempotent batch rejected")
		return errInvalidRequest
	default:
		return errNone
	}
}

// reachCodec declares that a batch of codec c (1..4) was admitted. The strings
// are constants: the broker core builds no names at run time.
func (b *Broker) reachCodec(c int) {
	switch c {
	case kwire.CodecGzip:
		b.env.Reachable("broker: gzip batch admitted")
	case kwire.CodecSnappy:
		b.env.Reachable("broker: snappy batch admitted")
	case kwire.CodecLZ4:
		b.env.Reachable("broker: lz4 batch admitted")
	case kwire.CodecZstd:
		b.env.Reachable("broker: zstd batch admitted")
	}
}
