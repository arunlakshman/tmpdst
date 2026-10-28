package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
)

// metadata answers Metadata (decision 29): this broker is the only broker and
// the leader of every partition. Topics come from the control plane and are
// never auto-created.
func (b *Broker) metadata(s *slot, r *kmsg.MetadataRequest) []core.Action {
	var names []string
	for _, t := range r.Topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}
	o := &op{kind: opMeta, s: s, creq: r}
	if r.Topics != nil {
		if len(names) == 0 { // nothing to ask: no topics, or only unusable ids
			b.metaResp(o, nil, errNone)
			return nil
		}
		o.names = names
	}
	return b.startOp(o)
}

// metaResp builds the Metadata response of o from the control plane's topics.
// A non-zero code (the call failed) marks every requested topic with it.
func (b *Broker) metaResp(o *op, infos []core.TopicInfo, code kafkaCode) {
	r := o.creq.(*kmsg.MetadataRequest)
	resp := kmsg.NewPtrMetadataResponse()
	cluster := "manifester"
	resp.ClusterID, resp.ControllerID = &cluster, b.cfg.BrokerID
	resp.Brokers = []kmsg.MetadataResponseBroker{{NodeID: b.cfg.BrokerID, Host: b.cfg.Host, Port: b.cfg.Port}}
	if r.Topics == nil { // all topics
		for _, in := range infos {
			resp.Topics = append(resp.Topics, b.metaTopic(in.Name, infos, code))
		}
	}
	for _, rt := range r.Topics {
		if rt.Topic == nil { // topic ids are not supported
			t := kmsg.NewMetadataResponseTopic()
			t.TopicID, t.ErrorCode = rt.TopicID, int16(errUnknownTopicID)
			resp.Topics = append(resp.Topics, t)
			continue
		}
		resp.Topics = append(resp.Topics, b.metaTopic(*rt.Topic, infos, code))
	}
	o.s.resp = resp
}

func (b *Broker) metaTopic(name string, infos []core.TopicInfo, code kafkaCode) kmsg.MetadataResponseTopic {
	n := name
	t := kmsg.NewMetadataResponseTopic()
	t.Topic = &n
	for _, in := range infos {
		if in.Name != name {
			continue
		}
		for p := int32(0); p < in.Partitions; p++ {
			mp := kmsg.NewMetadataResponseTopicPartition()
			mp.Partition, mp.Leader, mp.LeaderEpoch = p, b.cfg.BrokerID, 0
			mp.Replicas, mp.ISR = []int32{b.cfg.BrokerID}, []int32{b.cfg.BrokerID}
			t.Partitions = append(t.Partitions, mp)
		}
		return t
	}
	t.ErrorCode = int16(errUnknownTopicOrPart)
	if code != errNone {
		t.ErrorCode = int16(code)
	}
	return t
}

// listOffsets serves earliest (always 0, no retention) and latest (the end
// offset); other timestamps are unsupported (decision 29).
func (b *Broker) listOffsets(s *slot, r *kmsg.ListOffsetsRequest) []core.Action {
	resp := listOffsetsSkeleton(r)
	s.resp = resp
	var acts []core.Action
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &resp.Topics[i].Partitions[j]
			acts = append(acts, b.listOne(s, core.TP(t.Topic, p.Partition), p.Timestamp, rp)...)
		}
	}
	return acts
}

func listOffsetsSkeleton(r *kmsg.ListOffsetsRequest) *kmsg.ListOffsetsResponse {
	resp := kmsg.NewPtrListOffsetsResponse()
	resp.Topics = make([]kmsg.ListOffsetsResponseTopic, len(r.Topics))
	for i, t := range r.Topics {
		rt := &resp.Topics[i]
		rt.Topic = t.Topic
		rt.Partitions = make([]kmsg.ListOffsetsResponseTopicPartition, len(t.Partitions))
		for j := range t.Partitions {
			rp := &rt.Partitions[j]
			rp.Partition, rp.Timestamp, rp.Offset, rp.LeaderEpoch = t.Partitions[j].Partition, -1, -1, 0
		}
	}
	return resp
}

func (b *Broker) listOne(s *slot, tp core.TopicPartition, ts int64, rp *kmsg.ListOffsetsResponseTopicPartition) []core.Action {
	switch {
	case !b.knows(tp):
		rp.ErrorCode = int16(errUnknownTopicOrPart)
	case ts == -2:
		rp.Offset = 0
	case ts == -1:
		return b.startOp(&op{kind: opList, s: s, tp: tp, lpart: rp})
	default:
		rp.ErrorCode = int16(errUnsupportedForFormat)
	}
	return nil
}
