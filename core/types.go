package core

import "strconv"

// Time is a node-local monotonic instant in nanoseconds. In simulation it
// includes the node's clock skew. Used for deadlines, leases and batch windows.
type Time int64

// WallTime is UTC, nanoseconds since the Unix epoch. Used for Kafka
// LogAppendTime, credential expiry and anything persisted. Scripted in sim.
type WallTime int64

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
	NodeID string // stable identity of a broker (or sim workload)
	// Incarnation identifies one boot of a node.
	Incarnation ID // fresh per boot; names one run of a node in logs, facts and the runtime
	// ConnID names a connection.
	ConnID uint64 // a connection, as seen by one node
	// ReqID correlates an Action with its completion Event. Unique within one
	// incarnation only; it must never reach a durable name (object keys come
	// from Env.NewID()).
	ReqID uint64
	// TimerID names a timer.
	TimerID uint64
	// TimerGen is a timer generation.
	TimerGen uint64 // generation: a TimerFired whose Gen is stale is ignored
	// Offset is a partition log offset.
	Offset int64
)

// NodeRef names one specific boot of one node.
type NodeRef struct {
	Node NodeID
	Inc  Incarnation
}

// TopicPartition names one partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
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
// only from the core's decoder, never from sys.Classifier.
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
	}
	return "ErrCode(" + strconv.FormatUint(uint64(e), 10) + ")"
}
