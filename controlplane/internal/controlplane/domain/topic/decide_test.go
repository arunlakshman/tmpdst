package topic

import (
	"reflect"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// plainView is a View in plain maps, so the rules run with no store.
type plainView struct {
	topics  map[string]int32
	ends    map[contract.TopicPartition]int64
	commits map[string]CommitRecord
}

func (v plainView) Topic(name string) (int32, bool) {
	n, ok := v.topics[name]
	return n, ok
}

func (v plainView) End(topicPartition contract.TopicPartition) int64 { return v.ends[topicPartition] }

func (v plainView) Commit(segmentKey string) (CommitRecord, bool) {
	rec, ok := v.commits[segmentKey]
	return rec, ok
}

func batch(topic string, partition int32, records int64) contract.PendingBatch {
	return contract.PendingBatch{TopicPartition: contract.NewTopicPartition(topic, partition), Records: records,
		Loc: runtime.SegmentRef{Key: "seg", Off: records, Len: records * 10}}
}

func rangeOf(b contract.PendingBatch, base int64) contract.BatchRange {
	return contract.BatchRange{TopicPartition: b.TopicPartition, Base: contract.Offset(base), Count: b.Records, Loc: b.Loc}
}

func TestDecideCommit(t *testing.T) {
	a0, a1, b0 := batch("a", 0, 3), batch("a", 1, 2), batch("b", 0, 4)
	known := contract.CommitSegment{SegmentKey: "known", Batches: []contract.PendingBatch{a0, a1}}
	view := plainView{
		topics:  map[string]int32{"a": 1},
		ends:    map[contract.TopicPartition]int64{a0.TopicPartition: 10},
		commits: map[string]CommitRecord{"known": {Digest: checksum(known.Batches), Bases: []int64{7, 0}}},
	}
	for _, c := range []struct {
		name      string
		operation contract.CommitSegment
		events    []Event
		assigned  []contract.BatchRange
		code      runtime.ErrCode
	}{
		{
			name:      "a replay of a known key gets its first ranges and makes no event",
			operation: known,
			assigned:  []contract.BatchRange{rangeOf(a0, 7), rangeOf(a1, 0)},
			code:      runtime.OK,
		},
		{
			name:      "a known key with another request is a conflict",
			operation: contract.CommitSegment{SegmentKey: "known", Batches: []contract.PendingBatch{a0}},
			code:      runtime.ErrConflict,
		},
		{
			name:      "a known key is answered before the request is checked",
			operation: contract.CommitSegment{SegmentKey: "known"},
			code:      runtime.ErrConflict,
		},
		{name: "no key", operation: contract.CommitSegment{Batches: []contract.PendingBatch{a0}}, code: runtime.ErrInvalid},
		{name: "no batch", operation: contract.CommitSegment{SegmentKey: "k"}, code: runtime.ErrInvalid},
		{name: "a batch with no records", operation: contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{a0, batch("a", 0, 0)}}, code: runtime.ErrInvalid},
		{name: "a batch with no topic", operation: contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{batch("", 0, 1)}}, code: runtime.ErrInvalid},
		{name: "a batch with a negative partition", operation: contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{batch("a", -1, 1)}}, code: runtime.ErrInvalid},
		{
			name:      "offsets start at the partition end and grow in request order",
			operation: contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{a0, b0, a0, a1}},
			assigned:  []contract.BatchRange{rangeOf(a0, 10), rangeOf(b0, 0), rangeOf(a0, 13), rangeOf(a1, 0)},
			events: []Event{
				SegmentCommitted{Slot: 5, SegmentKey: "k", Digest: checksum([]contract.PendingBatch{a0, b0, a0, a1}),
					Assigned: []contract.BatchRange{rangeOf(a0, 10), rangeOf(b0, 0), rangeOf(a0, 13), rangeOf(a1, 0)}},
				PartitionsGrown{Topic: "a", Partitions: 2},
				TopicCreated{Topic: "b", Partitions: 1},
			},
			code: runtime.OK,
		},
		{
			name:      "a commit inside the partition count grows nothing",
			operation: contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{a0}},
			assigned:  []contract.BatchRange{rangeOf(a0, 10)},
			events:    []Event{SegmentCommitted{Slot: 5, SegmentKey: "k", Digest: checksum([]contract.PendingBatch{a0}), Assigned: []contract.BatchRange{rangeOf(a0, 10)}}},
			code:      runtime.OK,
		},
	} {
		events, assigned, code := DecideCommit(view, 5, c.operation)
		if code != c.code || !reflect.DeepEqual(events, c.events) || !reflect.DeepEqual(assigned, c.assigned) {
			t.Errorf("%s:\n got %v %+v %+v\nwant %v %+v %+v", c.name, code, events, assigned, c.code, c.events, c.assigned)
		}
	}
}

// A window record with another number of bases than the request is corrupt
// state: the node stops.
func TestDecideCommitPanicsOnACorruptRecord(t *testing.T) {
	a0 := batch("a", 0, 1)
	op := contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{a0}}
	view := plainView{commits: map[string]CommitRecord{"k": {Digest: checksum(op.Batches), Bases: []int64{1, 2}}}}
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	DecideCommit(view, 0, op)
}

func TestDecideCreate(t *testing.T) {
	view := plainView{topics: map[string]int32{"t": 3}}
	for _, c := range []struct {
		name      string
		operation contract.CreateTopic
		events    []Event
		code      runtime.ErrCode
	}{
		{"a new topic", contract.CreateTopic{Name: "u", Partitions: 2}, []Event{TopicCreated{Topic: "u", Partitions: 2}}, runtime.OK},
		{"an exclusive create of a new topic", contract.CreateTopic{Name: "u", Partitions: 2, FailIfExists: true}, []Event{TopicCreated{Topic: "u", Partitions: 2}}, runtime.OK},
		{"an exclusive create of a topic that exists", contract.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}, nil, runtime.ErrTopicExists},
		{"a plain create grows the topic", contract.CreateTopic{Name: "t", Partitions: 5}, []Event{PartitionsGrown{Topic: "t", Partitions: 5}}, runtime.OK},
		{"a plain create with the same count changes nothing", contract.CreateTopic{Name: "t", Partitions: 3}, nil, runtime.OK},
		{"a plain create never shrinks", contract.CreateTopic{Name: "t", Partitions: 1}, nil, runtime.OK},
		{"an empty name", contract.CreateTopic{Partitions: 1, FailIfExists: true}, nil, runtime.ErrInvalid},
		{"zero partitions", contract.CreateTopic{Name: "t", FailIfExists: true}, nil, runtime.ErrInvalid},
		{"negative partitions", contract.CreateTopic{Name: "u", Partitions: -1}, nil, runtime.ErrInvalid},
	} {
		events, code := DecideCreate(view, c.operation)
		if code != c.code || !reflect.DeepEqual(events, c.events) {
			t.Errorf("%s: got %v %+v, want %v %+v", c.name, code, events, c.code, c.events)
		}
	}
}
