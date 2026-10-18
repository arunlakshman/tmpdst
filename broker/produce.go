package broker

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
)

// produce admits one batch per partition into the open segment. Acks 1 and -1
// both mean "committed" (acks=0 never gets here: the connection is closed).
func (b *Broker) produce(s *slot, r *kmsg.ProduceRequest) []core.Action {
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
	s.resp = resp
	var acts []core.Action
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &resp.Topics[i].Partitions[j]
			tp := core.TopicPartition{Topic: t.Topic, Partition: p.Partition}
			pb, code := b.admit(s, r, tp, p.Records, rp)
			if code != errNone {
				rp.ErrorCode = code
				continue
			}
			s.pending++
			acts = append(acts, b.addBatch(pb)...)
		}
	}
	return acts
}

// admit validates one partition's payload and returns the batch to buffer, or
// the Kafka error code that answers the partition.
func (b *Broker) admit(s *slot, r *kmsg.ProduceRequest, tp core.TopicPartition, raw []byte, rp *kmsg.ProduceResponseTopicPartition) (*pbatch, int16) {
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
	bt, err := kwire.ParseBatch(one)
	switch {
	case errors.Is(err, kwire.ErrUnsupportedBatch):
		if len(one) >= 23 && (one[22]&7) != 0 {
			return nil, errUnsupportedCompress
		}
		return nil, errUnsupportedForFormat
	case err != nil:
		return nil, errCorruptMessage
	case bt.IsTransactional() || bt.IsControl():
		return nil, errInvalidRequest
	case bt.NumRecords <= 0 || bt.LastOffsetDelta != bt.NumRecords-1:
		return nil, errCorruptMessage
	}
	if bt.ProducerID >= 0 {
		// Idempotent producers are unsupported (decision 41): without the
		// sequence checks a retry would duplicate records, so refuse rather
		// than silently weaken the guarantee. INVALID_REQUEST is not retriable.
		b.env.Reachable("broker: idempotent batch rejected")
		return nil, errInvalidRequest
	}
	return &pbatch{s: s, part: rp, tp: tp, data: one, records: int64(bt.NumRecords)}, errNone
}
