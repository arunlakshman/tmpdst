package core

// Time is a node-local monotonic instant in nanoseconds. In simulation it
// includes the node's clock skew. Used for deadlines, leases and batch windows.
type Time int64

// WallTime is UTC, nanoseconds since the Unix epoch. Used for Kafka
// LogAppendTime, credential expiry and anything persisted. Scripted in sim.
type WallTime int64

// Duration is nanoseconds.
type Duration int64

const (
	Nanosecond  Duration = 1
	Microsecond          = 1000 * Nanosecond
	Millisecond          = 1000 * Microsecond
	Second               = 1000 * Millisecond
)

// ID is a 128-bit identifier drawn from the seeded stream (never uuid.New).
type ID [16]byte

type (
	NodeID      string // stable identity of a broker (or sim workload)
	Incarnation ID     // fresh per boot; references to an old one are fenced
	Epoch       uint64 // fencing token issued by the control plane
	ConnID      uint64 // a connection, as seen by one node
	ReqID       uint64 // correlates an Action with its completion Event
	TimerID     uint64
	TimerGen    uint64 // generation: a TimerFired whose Gen is stale is ignored
	Offset      int64
)

// NodeRef names one specific boot of one node.
type NodeRef struct {
	Node NodeID
	Inc  Incarnation
}

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
type ErrCode uint16

const (
	OK             ErrCode = iota
	ErrTimeout             // deadline passed; see Outcome for what it proves
	ErrUnavailable         // 503 / throttled / overloaded
	ErrNotFound
	ErrConflict    // CAS / idempotency-key mismatch
	ErrFenced      // stale incarnation or epoch
	ErrConnClosed  // connection lost
	ErrInterrupted // the caller's node crashed while the op was pending (sim)
	ErrTooLarge
	ErrCorrupt
	ErrInvalid // malformed request; never retried
	ErrIO      // anything else from the outside world
)

// Outcome states what a completion proves about side effects (lesson 1).
type Outcome uint8

const (
	NotApplied   Outcome = iota // provably no effect
	MaybeApplied                // may have taken effect; retry only if idempotent
	Applied                     // took effect (the reply itself may be unusable)
)
