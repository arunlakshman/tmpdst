package topic

import (
	"fmt"
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// The rules of the topic context. A Decide function reads the state before a
// command through a View and returns the events the command makes and its
// answer. It writes nothing: Apply writes the events. So a test can call it
// with a plain View, and the answer depends only on the state before the
// command and on the command.

// View is the state that the rules read: the state before the command.
// tables.go has the View over a transaction.
type View interface {
	// Topic is the partition count of a topic; ok is false when the topic
	// does not exist.
	Topic(name string) (int32, bool)
	// End is the next offset of a partition: 0 when nothing was committed to it.
	End(topicPartition contract.TopicPartition) int64
	// Commit is the record of a segment key in the idempotency window; ok is
	// false when the key is not there.
	Commit(segmentKey string) (CommitRecord, bool)
}

// CommitRecord is what the idempotency window keeps of a segment key: the
// digest of its request and the base offset given to each batch, in request
// order.
type CommitRecord struct {
	Digest [checksumLength]byte
	Bases  []int64
}

// DecideCommit decides a CommitSegment at log slot slot, and returns its
// events, the ranges of its answer and its code. A key that is in the window
// gets its first ranges back and makes no event, or ErrConflict when the
// request differs. A new key gets offsets: each batch's base is its
// partition's end, in request order, and the end then grows by the batch's
// records. The topics grow to hold every partition of the commit.
func DecideCommit(view View, slot int64, operation contract.CommitSegment) ([]Event, []contract.BatchRange, runtime.ErrCode) {
	dig := checksum(operation.Batches)
	if rec, ok := view.Commit(operation.SegmentKey); ok {
		assigned, code := replay(rec, dig, operation)
		return nil, assigned, code
	}
	if !validCommit(operation) {
		return nil, nil, runtime.ErrInvalid
	}
	assigned := assign(view, operation.Batches)
	events := []Event{SegmentCommitted{Slot: slot, SegmentKey: operation.SegmentKey, Digest: dig, Assigned: assigned}}
	return append(events, growTopics(view, assigned)...), assigned, runtime.OK
}

// DecideCreate decides a CreateTopic: the topic gets at least the partitions
// asked for, and never shrinks. ErrInvalid for an empty name or partitions <= 0.
// With FailIfExists, a topic that exists gives ErrTopicExists and no event.
func DecideCreate(view View, operation contract.CreateTopic) ([]Event, runtime.ErrCode) {
	if operation.Name == "" || operation.Partitions <= 0 {
		return nil, runtime.ErrInvalid
	}
	if _, exists := view.Topic(operation.Name); exists && operation.FailIfExists {
		return nil, runtime.ErrTopicExists
	}
	if e, ok := atLeast(view, operation.Name, operation.Partitions); ok {
		return []Event{e}, runtime.OK
	}
	return nil, runtime.OK
}

// replay answers a commit whose key is in the window: the first ranges,
// rebuilt from the request and the stored bases, or ErrConflict if the
// request differs. A record with another number of bases is corrupt state,
// and the node stops (fail-stop).
func replay(rec CommitRecord, dig [checksumLength]byte, operation contract.CommitSegment) ([]contract.BatchRange, runtime.ErrCode) {
	if rec.Digest != dig {
		return nil, runtime.ErrConflict
	}
	if len(rec.Bases) != len(operation.Batches) {
		panic(fmt.Errorf("topics: key %q: %w", operation.SegmentKey, errCorrupt))
	}
	rs := make([]contract.BatchRange, len(operation.Batches))
	for i, b := range operation.Batches {
		rs[i] = contract.BatchRange{TopicPartition: b.TopicPartition, Base: contract.Offset(rec.Bases[i]), Count: b.Records, Loc: b.Loc}
	}
	return rs, runtime.OK
}

func validBatch(b contract.PendingBatch) bool {
	return b.Records > 0 && b.TopicPartition.Topic != "" && b.TopicPartition.Partition >= 0
}

func validCommit(operation contract.CommitSegment) bool {
	return operation.SegmentKey != "" && len(operation.Batches) > 0 && slices.IndexFunc(operation.Batches, func(b contract.PendingBatch) bool { return !validBatch(b) }) < 0
}

// assign hands out offsets: each batch's Base is its partition's end, in
// request order, the end then growing by Records.
func assign(view View, bs []contract.PendingBatch) []contract.BatchRange {
	ends := map[contract.TopicPartition]int64{} // lookups only
	out := make([]contract.BatchRange, 0, len(bs))
	for _, b := range bs {
		base, seen := ends[b.TopicPartition]
		if !seen {
			base = view.End(b.TopicPartition)
		}
		out = append(out, contract.BatchRange{TopicPartition: b.TopicPartition, Base: contract.Offset(base), Count: b.Records, Loc: b.Loc})
		ends[b.TopicPartition] = max(ends[b.TopicPartition], base+b.Records)
	}
	return out
}

// growTopics is the events that give each topic of assigned a partition count
// that holds every partition in it, in the order the topics first appear.
func growTopics(view View, assigned []contract.BatchRange) []Event {
	var order []string
	want := map[string]int32{} // lookups only
	for _, r := range assigned {
		topic := r.TopicPartition.Topic
		if _, seen := want[topic]; !seen {
			order = append(order, topic)
		}
		want[topic] = max(want[topic], r.TopicPartition.Partition+1)
	}
	var events []Event
	for _, topic := range order {
		if e, ok := atLeast(view, topic, want[topic]); ok {
			events = append(events, e)
		}
	}
	return events
}

// atLeast is the event that gives topic at least n partitions: TopicCreated
// when it does not exist, PartitionsGrown when it has fewer, and none (ok is
// false) when it has n or more.
func atLeast(view View, topic string, n int32) (Event, bool) {
	partitions, exists := view.Topic(topic)
	switch {
	case !exists:
		return TopicCreated{Topic: topic, Partitions: n}, true
	case n > partitions:
		return PartitionsGrown{Topic: topic, Partitions: n}, true
	}
	return nil, false
}
