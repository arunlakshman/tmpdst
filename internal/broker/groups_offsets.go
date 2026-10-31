package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// OffsetCommit (key 8) and OffsetFetch (key 9). Offsets live in the control
// plane, with the group (decision Q5). The broker checks nothing but the group
// name. Each partition gets its own code.

// offsetCommit answers OffsetCommit. Version 0 has no generation or member, so
// it is a manual commit (generation -1, no member), as is generation -1 at
// any version.
func (b *Broker) offsetCommit(s *slot, r *kmsg.OffsetCommitRequest) []core.Action {
	response := kmsg.NewPtrOffsetCommitResponse()
	s.response = response
	call := core.OffsetCommit{Group: r.Group, Generation: -1}
	if r.Version >= 1 {
		call.Member, call.Generation = r.MemberID, r.Generation
	}
	for _, t := range r.Topics {
		rt := kmsg.OffsetCommitResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rt.Partitions = append(rt.Partitions, kmsg.OffsetCommitResponseTopicPartition{Partition: p.Partition})
			call.Offsets = append(call.Offsets, core.PartitionOffset{
				TopicPartition: core.NewTopicPartition(t.Topic, p.Partition), Offset: p.Offset,
				LeaderEpoch: leaderEpochOf(r.Version, p.LeaderEpoch), Metadata: derefOr(p.Metadata),
			})
		}
		response.Topics = append(response.Topics, rt)
	}
	parts := commitParts(response)
	if r.Group == "" {
		setCommitCodes(parts, errInvalidGroupID)
		return nil
	}
	if len(call.Offsets) == 0 {
		return nil
	}
	fill := func(e core.ControlPlaneDone) {
		if e.Err != core.OK {
			setCommitCodes(parts, groupErrCode(e.Err))
			return
		}
		errs := core.ResponseAs[core.OffsetCommitResp](e).Errs
		for i, p := range parts {
			code := core.ErrIO // the control plane gave no code for this partition
			if i < len(errs) {
				code = errs[i]
			}
			p.ErrorCode = int16(groupErrCode(code))
		}
	}
	return b.groupCall(s, call, 0, fill)
}

// leaderEpochOf is the leader epoch of a commit. Versions before 6 have none.
func leaderEpochOf(version int16, epoch int32) int32 {
	if version < 6 {
		return -1
	}
	return epoch
}

// commitParts lists the partitions of a commit response, in request order.
func commitParts(response *kmsg.OffsetCommitResponse) []*kmsg.OffsetCommitResponseTopicPartition {
	var out []*kmsg.OffsetCommitResponseTopicPartition
	for i := range response.Topics {
		for j := range response.Topics[i].Partitions {
			out = append(out, &response.Topics[i].Partitions[j])
		}
	}
	return out
}

func setCommitCodes(parts []*kmsg.OffsetCommitResponseTopicPartition, code kafkaErrorCode) {
	for _, p := range parts {
		p.ErrorCode = int16(code)
	}
}

// fetchAsk is one group of an OffsetFetch request. all means every committed
// offset: an empty topic list, from version 2 on. Otherwise want lists the
// partitions asked for, in request order.
type fetchAsk struct {
	name string
	all  bool
	want []core.TopicPartition
}

// askTopic is one topic of an OffsetFetch request group.
type askTopic struct {
	topic string
	parts []int32
}

func newFetchAsk(version int16, name string, topics []askTopic) fetchAsk {
	a := fetchAsk{name: name, all: len(topics) == 0 && version >= 2}
	for _, t := range topics {
		for _, p := range t.parts {
			a.want = append(a.want, core.NewTopicPartition(t.topic, p))
		}
	}
	return a
}

// fetchRow is one partition of an OffsetFetch answer.
type fetchRow struct {
	topicPartition core.TopicPartition
	offset         int64
	epoch          int32
	meta           *string
	code           kafkaErrorCode
}

// offsetFetch answers OffsetFetch. Version 8 asks for several groups, one
// control plane call each. Version 0 to 7 ask for one group.
func (b *Broker) offsetFetch(s *slot, r *kmsg.OffsetFetchRequest) []core.Action {
	response := kmsg.NewPtrOffsetFetchResponse()
	s.response = response
	batched := r.Version >= 8
	var asks []fetchAsk
	if batched {
		response.Groups = make([]kmsg.OffsetFetchResponseGroup, len(r.Groups))
		for i, g := range r.Groups {
			response.Groups[i].Group = g.Group
			var topics []askTopic
			for _, t := range g.Topics {
				topics = append(topics, askTopic{topic: t.Topic, parts: t.Partitions})
			}
			asks = append(asks, newFetchAsk(r.Version, g.Group, topics))
		}
	} else {
		var topics []askTopic
		for _, t := range r.Topics {
			topics = append(topics, askTopic{topic: t.Topic, parts: t.Partitions})
		}
		asks = []fetchAsk{newFetchAsk(r.Version, r.Group, topics)}
	}
	var acts []core.Action
	for i, a := range asks {
		acts = append(acts, b.fetchGroup(s, response, i, batched, a)...)
	}
	return acts
}

// fetchGroup answers one group of an OffsetFetch. Its answer goes to group idx
// (version 8) or to the whole response (earlier versions).
func (b *Broker) fetchGroup(s *slot, response *kmsg.OffsetFetchResponse, idx int, batched bool, a fetchAsk) []core.Action {
	set := func(rows []fetchRow, code kafkaErrorCode) {
		setFetchRows(response, idx, batched, rows, code)
	}
	switch {
	case a.name == "":
		set(errRows(a, errInvalidGroupID), errInvalidGroupID)
		return nil
	case !a.all && len(a.want) == 0: // nothing asked
		set(nil, errNone)
		return nil
	}
	fill := func(e core.ControlPlaneDone) {
		if e.Err != core.OK {
			code := groupErrCode(e.Err)
			set(errRows(a, code), code)
			return
		}
		set(fetchRows(a, core.ResponseAs[core.OffsetFetchResp](e).Offsets), errNone)
	}
	return b.groupCall(s, core.OffsetFetch{Group: a.name, Partitions: a.want}, 0, fill)
}

// errRows lists the partitions of a failed answer with the code. An "all"
// answer has no partition to list.
func errRows(a fetchAsk, code kafkaErrorCode) []fetchRow {
	var rows []fetchRow
	for _, topicPartition := range a.want {
		rows = append(rows, fetchRow{topicPartition: topicPartition, offset: -1, epoch: -1, code: code})
	}
	return rows
}

// fetchRows turns the control plane answer into rows. A list of partitions
// keeps the request order. An "all" answer keeps the control plane order.
func fetchRows(a fetchAsk, got []core.PartitionOffset) []fetchRow {
	if a.all {
		rows := make([]fetchRow, 0, len(got))
		for _, o := range got {
			rows = append(rows, rowOf(o))
		}
		return rows
	}
	rows := make([]fetchRow, 0, len(a.want))
	for i, topicPartition := range a.want {
		if i < len(got) {
			rows = append(rows, rowOf(got[i]))
			continue
		}
		rows = append(rows, fetchRow{topicPartition: topicPartition, offset: -1, epoch: -1})
	}
	return rows
}

func rowOf(o core.PartitionOffset) fetchRow {
	return fetchRow{topicPartition: o.TopicPartition, offset: o.Offset, epoch: o.LeaderEpoch, meta: new(o.Metadata)}
}

// setFetchRows writes rows and a group code into the response.
func setFetchRows(response *kmsg.OffsetFetchResponse, idx int, batched bool, rows []fetchRow, code kafkaErrorCode) {
	if batched {
		g := &response.Groups[idx]
		g.Topics, g.ErrorCode = groupTopics(rows), int16(code)
		return
	}
	response.Topics, response.ErrorCode = fetchTopics(rows), int16(code)
}

// splitTopics groups consecutive rows of one topic.
func splitTopics(rows []fetchRow) [][]fetchRow {
	var out [][]fetchRow
	for i, r := range rows {
		if i == 0 || rows[i-1].topicPartition.Topic != r.topicPartition.Topic {
			out = append(out, nil)
		}
		out[len(out)-1] = append(out[len(out)-1], r)
	}
	return out
}

//nolint:dupl // the same loop as groupTopics, for the other kmsg type
func fetchTopics(rows []fetchRow) []kmsg.OffsetFetchResponseTopic {
	var out []kmsg.OffsetFetchResponseTopic
	for _, grp := range splitTopics(rows) {
		t := kmsg.OffsetFetchResponseTopic{Topic: grp[0].topicPartition.Topic}
		for _, r := range grp {
			t.Partitions = append(t.Partitions, kmsg.OffsetFetchResponseTopicPartition{
				Partition: r.topicPartition.Partition, Offset: r.offset, LeaderEpoch: r.epoch, Metadata: r.meta, ErrorCode: int16(r.code)})
		}
		out = append(out, t)
	}
	return out
}

//nolint:dupl // the same loop as fetchTopics, for the other kmsg type
func groupTopics(rows []fetchRow) []kmsg.OffsetFetchResponseGroupTopic {
	var out []kmsg.OffsetFetchResponseGroupTopic
	for _, grp := range splitTopics(rows) {
		t := kmsg.OffsetFetchResponseGroupTopic{Topic: grp[0].topicPartition.Topic}
		for _, r := range grp {
			t.Partitions = append(t.Partitions, kmsg.OffsetFetchResponseGroupTopicPartition{
				Partition: r.topicPartition.Partition, Offset: r.offset, LeaderEpoch: r.epoch, Metadata: r.meta, ErrorCode: int16(r.code)})
		}
		out = append(out, t)
	}
	return out
}
