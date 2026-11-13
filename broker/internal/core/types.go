package core

import "github.com/manifesterio/mds/controlplane/contract"

// The time types, ID, NodeID, BootNumber, RequestID, TimerID, TimerVersion and NodeInstance
// are not defined here. They come from dst/machine, and the broker and the control plane
// share them.
//
// The control plane call and reply types (CommitSegment, GroupJoin, and the others), and the
// small value types they use, are defined once in controlplane/contract. The names below are
// aliases of contract types, so that the broker keeps its short names. They are not copies.

// ConnID names a connection.
type ConnID uint64 // a connection, as seen by one node

// Offset is a partition log offset.
type Offset = contract.Offset

// TopicPartition names one partition of a topic.
type TopicPartition = contract.TopicPartition

// SegmentRef is a byte range inside an object in the object store.
type SegmentRef = contract.SegmentRef

// BrokerAddr is one broker's advertised address, as Metadata lists it.
type BrokerAddr = contract.BrokerAddr

// ErrCode is an error as plain data. See contract.ErrCode.
type ErrCode = contract.ErrCode

// ErrCode values. They are the values of contract.
const (
	OK                           = contract.OK
	ErrTimeout                   = contract.ErrTimeout
	ErrUnavailable               = contract.ErrUnavailable
	ErrNotFound                  = contract.ErrNotFound
	ErrConflict                  = contract.ErrConflict
	ErrConnClosed                = contract.ErrConnClosed
	ErrTooLarge                  = contract.ErrTooLarge
	ErrCorrupt                   = contract.ErrCorrupt
	ErrInvalid                   = contract.ErrInvalid
	ErrIO                        = contract.ErrIO
	ErrUnknownMember             = contract.ErrUnknownMember
	ErrIllegalGeneration         = contract.ErrIllegalGeneration
	ErrUnknownPartition          = contract.ErrUnknownPartition
	ErrRebalanceInProgress       = contract.ErrRebalanceInProgress
	ErrInconsistentGroupProtocol = contract.ErrInconsistentGroupProtocol
	ErrTopicExists               = contract.ErrTopicExists
)

// Outcome states what a completion proves about side effects. See contract.Outcome.
type Outcome = contract.Outcome

// Outcome values. They are the values of contract.
const (
	NotApplied   = contract.NotApplied
	MaybeApplied = contract.MaybeApplied
	Applied      = contract.Applied
)
