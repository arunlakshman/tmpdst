package broker

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// metadata answers Metadata (decision 29): this broker is the leader of every
// partition. Topics come from the control plane and are never auto-created.
func (b *Broker) metadata(s *slot, r *kmsg.MetadataRequest) []core.Action {
	var names []string
	for _, t := range r.Topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}
	o := &operation{kind: opMetadata, s: s, creq: r}
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
func (b *Broker) metaResp(o *operation, infos []core.TopicInfo, code kafkaErrorCode) {
	r := o.creq.(*kmsg.MetadataRequest)
	response := kmsg.NewPtrMetadataResponse()
	cluster := "manifester"
	response.ClusterID, response.ControllerID = &cluster, b.config.BrokerID
	response.Brokers = b.metaBrokers()
	if r.Topics == nil { // all topics
		for _, in := range infos {
			response.Topics = append(response.Topics, b.metaTopic(in.Name, infos, code))
		}
	}
	for _, rt := range r.Topics {
		if rt.Topic == nil { // topic ids are not supported
			t := kmsg.NewMetadataResponseTopic()
			t.TopicID, t.ErrorCode = rt.TopicID, int16(errUnknownTopicID)
			response.Topics = append(response.Topics, t)
			continue
		}
		response.Topics = append(response.Topics, b.metaTopic(*rt.Topic, infos, code))
	}
	o.s.response = response
}

// metaBrokers lists the live brokers, sorted by node id (Q2). This broker is
// always in the list, with its own configured address, even when the live set
// is empty or does not have it yet.
func (b *Broker) metaBrokers() []kmsg.MetadataResponseBroker {
	all := b.liveList()
	out := make([]kmsg.MetadataResponseBroker, 0, len(all))
	for _, p := range all {
		out = append(out, kmsg.MetadataResponseBroker{NodeID: p.NodeID, Host: p.Host, Port: p.Port})
	}
	return out
}

// liveList is the live broker set this broker answers with: itself plus the
// brokers of the last LiveBrokers, sorted by node id. FindCoordinator hashes over
// it too (coordinator.go), so every answer uses one list.
func (b *Broker) liveList() []core.BrokerAddr {
	self := core.BrokerAddr{NodeID: b.config.BrokerID, Host: b.config.Host, Port: b.config.Port}
	all := []core.BrokerAddr{self}
	for _, p := range b.live {
		if p.NodeID != self.NodeID {
			all = append(all, p)
		}
	}
	slices.SortFunc(all, func(x, y core.BrokerAddr) int {
		switch {
		case x.NodeID < y.NodeID:
			return -1
		case x.NodeID > y.NodeID:
			return 1
		}
		return 0
	})
	return all
}

func (b *Broker) metaTopic(name string, infos []core.TopicInfo, code kafkaErrorCode) kmsg.MetadataResponseTopic {
	n := name
	t := kmsg.NewMetadataResponseTopic()
	t.Topic = &n
	for _, in := range infos {
		if in.Name != name {
			continue
		}
		for p := int32(0); p < in.Partitions; p++ {
			mp := kmsg.NewMetadataResponseTopicPartition()
			mp.Partition, mp.Leader, mp.LeaderEpoch = p, b.config.BrokerID, 0
			mp.Replicas, mp.ISR = []int32{b.config.BrokerID}, []int32{b.config.BrokerID}
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
	response := listOffsetsSkeleton(r)
	s.response = response
	var acts []core.Action
	for i, t := range r.Topics {
		for j, p := range t.Partitions {
			rp := &response.Topics[i].Partitions[j]
			acts = append(acts, b.listOne(s, core.NewTopicPartition(t.Topic, p.Partition), p.Timestamp, rp)...)
		}
	}
	return acts
}

func listOffsetsSkeleton(r *kmsg.ListOffsetsRequest) *kmsg.ListOffsetsResponse {
	response := kmsg.NewPtrListOffsetsResponse()
	response.Topics = make([]kmsg.ListOffsetsResponseTopic, len(r.Topics))
	for i, t := range r.Topics {
		rt := &response.Topics[i]
		rt.Topic = t.Topic
		rt.Partitions = make([]kmsg.ListOffsetsResponseTopicPartition, len(t.Partitions))
		for j := range t.Partitions {
			rp := &rt.Partitions[j]
			rp.Partition, rp.Timestamp, rp.Offset, rp.LeaderEpoch = t.Partitions[j].Partition, -1, -1, 0
		}
	}
	return response
}

func (b *Broker) listOne(s *slot, topicPartition core.TopicPartition, ts int64, rp *kmsg.ListOffsetsResponseTopicPartition) []core.Action {
	switch {
	case !b.knows(topicPartition):
		rp.ErrorCode = int16(errUnknownTopicOrPart)
	case ts == -2:
		rp.Offset = 0
	case ts == -1:
		return b.startOp(&operation{kind: opList, s: s, topicPartition: topicPartition, lpart: rp})
	default:
		rp.ErrorCode = int16(errUnsupportedForFormat)
	}
	return nil
}
