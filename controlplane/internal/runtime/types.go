package runtime

import "github.com/manifesterio/mds/controlplane/contract"

// The time types, ID, NodeID, BootNumber, RequestID, TimerID, TimerVersion and NodeInstance
// are not defined here. They come from dst/machine, and the broker and the control plane
// share them.
//
// ErrCode, Outcome and SegmentRef are public (the broker uses them), so they are defined in
// contract. The names below are aliases of them, so that the code of this module keeps its
// short names.

// LogPosition is a position in the replicated command log (loglet).
type LogPosition int64

// SegmentRef is a byte range inside an object in the object store.
type SegmentRef = contract.SegmentRef

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
