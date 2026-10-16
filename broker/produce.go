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
	pb := &pbatch{s: s, part: rp, tp: tp, data: one, records: int64(bt.NumRecords), pid: bt.ProducerID}
	if bt.ProducerID != -1 {
		// 0 is core.NoProducer; the control plane numbers real ids from 1000.
		if bt.ProducerID <= 0 {
			return nil, errInvalidPIDMapping
		}
		if bt.ProducerEpoch < 0 || bt.BaseSequence < 0 {
			return nil, errCorruptMessage
		}
		if b.cfg.Bug != BugNoIdempotence {
			pb.producer = core.ProducerID(bt.ProducerID)
		}
		pb.pepoch, pb.baseSeq = bt.ProducerEpoch, bt.BaseSequence
	}
	return pb, errNone
}
