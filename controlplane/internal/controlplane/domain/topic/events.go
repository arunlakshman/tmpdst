package topic

import "github.com/manifesterio/mds/controlplane/contract"

// Event is a fact that a command made. The log holds commands, not events: an
// event is made when its command is applied, on every replica, and is never
// stored. Apply (tables.go) writes it to the tables.
type Event interface{ isEvent() }

// SegmentCommitted says that a new segment key committed at log slot Slot and
// was given the offsets in Assigned, in request order. Digest identifies the
// request, so that a replay of the key can be told from a conflict.
type SegmentCommitted struct {
	Slot       int64
	SegmentKey string
	Digest     [checksumLength]byte
	Assigned   []contract.BatchRange
}

// TopicCreated says that a topic that did not exist now has Partitions
// partitions. A CreateTopic makes it, and so does a CommitSegment to a topic
// that does not exist.
type TopicCreated struct { //nolint:revive // the event names in docs/greenfield-refactor-plan.md (T5.1)
	Topic      string
	Partitions int32
}

// PartitionsGrown says that a topic now has Partitions partitions, more than
// before. A CreateTopic with more partitions makes it, and so does a
// CommitSegment to a partition past the topic's count.
type PartitionsGrown struct {
	Topic      string
	Partitions int32
}

func (SegmentCommitted) isEvent() {}
func (TopicCreated) isEvent()     {}
func (PartitionsGrown) isEvent()  {}
