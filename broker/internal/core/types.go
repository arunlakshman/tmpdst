package core

import (
	"fmt"
	"strconv"
)

// NodeTime is a node-local monotonic instant in nanoseconds. In simulation it
// includes the node's clock skew. Used for deadlines, leases and batch windows.
type NodeTime int64

// RealTime is UTC, nanoseconds since the Unix epoch. Used for Kafka
// LogAppendTime, credential expiry and anything persisted. Scripted in simulator.
type RealTime int64

// Duration is nanoseconds.
type Duration int64

// Duration units.
const (
	Nanosecond  Duration = 1
	Microsecond          = 1000 * Nanosecond
	Millisecond          = 1000 * Microsecond
	Second               = 1000 * Millisecond
)

// ID is a 128-bit identifier drawn from the seeded stream (never uuid.New).
type ID [16]byte

type (
	// NodeID names a node.
	NodeID string // stable identity of a broker (or simulator workload)
	// BootNumber identifies one boot of a node.
	BootNumber ID // fresh per boot; names one run of a node in logs, facts and the runtime
	// ConnID names a connection.
	ConnID uint64 // a connection, as seen by one node
	// RequestID correlates an Action with its completion Event. Unique within one
	// incarnation only; it must never reach a durable name (object keys come
	// from Environment.NewID()).
	RequestID uint64
	// TimerID names a timer.
	TimerID uint64
	// TimerVersion is a timer generation.
	TimerVersion uint64 // generation: a TimerFired whose Gen is stale is ignored
	// Offset is a partition log offset.
	Offset int64
	// LogPosition is a position in the replicated command log (loglet).
	LogPosition int64
)

// NodeInstance names one specific boot of one node.
type NodeInstance struct {
	Node NodeID
	Inc  BootNumber
}

// TopicPartition names one partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// NewTopicPartition builds a TopicPartition.
func NewTopicPartition(topic string, partition int32) TopicPartition {
	return TopicPartition{Topic: topic, Partition: partition}
}

func (topicPartition TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", topicPartition.Topic, topicPartition.Partition)
}

// SegmentRef is a byte range inside an object in the object store.
type SegmentRef struct {
	Key string
	Off int64
	Len int64 // <= 0 means to the end of the object
}

// ErrCode is an error as plain data so events stay comparable and hashable.
// It is folded into the hash: append-only from here on, never reordered.
//
// ErrNotFound and ErrConflict are always truthful; engines inject
// only ErrTimeout, ErrUnavailable, ErrIO and ErrConnClosed. ErrCorrupt comes
// only from the core's decoder, never from system.ErrorClassifier.
type ErrCode uint16

// ErrCode values.
const (
	OK             ErrCode = iota
	ErrTimeout             // deadline passed, or the response was lost; see Outcome (a lost response is ErrTimeout+MaybeApplied, never silence)
	ErrUnavailable         // 503 / throttled / overloaded
	ErrNotFound
	ErrConflict   // idempotency-key mismatch (same SegmentKey, different request)
	_             // retired: was ErrFenced (decision 44); keeps the numbers below stable
	ErrConnClosed // connection lost
	ErrTooLarge
	ErrCorrupt
	ErrInvalid // malformed request; never retried
	ErrIO      // anything else from the outside world

	ErrUnknownMember             // group member id or group leader is not a member
	ErrIllegalGeneration         // group generation does not match the one asked for
	ErrUnknownPartition          // topic or partition does not exist
	ErrRebalanceInProgress       // group is rebalancing; rejoin (slice 4b)
	ErrInconsistentGroupProtocol // members have no protocol in common, or a wrong protocol type (slice 4b)
	ErrTopicExists               // CreateTopic with FailIfExists: the topic is already there
)

// Outcome states what a completion proves about side effects (lesson 1).
// Folded into the hash: append-only from here on. A LostResponse surfaces as
// ErrTimeout+MaybeApplied, never as silence.
type Outcome uint8

// Outcome values.
const (
	NotApplied   Outcome = iota // provably no effect
	MaybeApplied                // may have taken effect; retry only if idempotent
	Applied                     // took effect (the reply itself may be unusable)
)

// String returns the frozen spelling of e. Spellings are never changed.
func (e ErrCode) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrUnavailable:
		return "ErrUnavailable"
	case ErrNotFound:
		return "ErrNotFound"
	case ErrConflict:
		return "ErrConflict"
	case ErrConnClosed:
		return "ErrConnClosed"
	case ErrTooLarge:
		return "ErrTooLarge"
	case ErrCorrupt:
		return "ErrCorrupt"
	case ErrInvalid:
		return "ErrInvalid"
	case ErrIO:
		return "ErrIO"
	case ErrUnknownMember:
		return "ErrUnknownMember"
	case ErrIllegalGeneration:
		return "ErrIllegalGeneration"
	case ErrUnknownPartition:
		return "ErrUnknownPartition"
	case ErrRebalanceInProgress:
		return "ErrRebalanceInProgress"
	case ErrInconsistentGroupProtocol:
		return "ErrInconsistentGroupProtocol"
	case ErrTopicExists:
		return "ErrTopicExists"
	}
	return "ErrCode(" + strconv.FormatUint(uint64(e), 10) + ")"
}
