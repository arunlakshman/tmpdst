package contract

import (
	"github.com/manifesterio/mds/shared/objectstore"
	"github.com/manifesterio/mds/shared/status"
)

// SegmentRef is a byte range inside an object in the object store. It is defined in
// shared/objectstore, because the shared object store code uses it. This name stays so
// that the contract reads the same.
type SegmentRef = objectstore.SegmentRef

// ErrCode is an error as plain data so events stay comparable and hashable. It is
// defined in shared/status (see there for the rules and the values). This name stays so
// that the contract reads the same.
type ErrCode = status.ErrCode

// Outcome states what a completion proves about side effects. It is defined in
// shared/status. This name stays so that the contract reads the same.
type Outcome = status.Outcome

// ErrCode values. They are the values of shared/status.
const (
	OK                           = status.OK
	ErrTimeout                   = status.ErrTimeout
	ErrUnavailable               = status.ErrUnavailable
	ErrNotFound                  = status.ErrNotFound
	ErrConflict                  = status.ErrConflict
	ErrConnClosed                = status.ErrConnClosed
	ErrTooLarge                  = status.ErrTooLarge
	ErrCorrupt                   = status.ErrCorrupt
	ErrInvalid                   = status.ErrInvalid
	ErrIO                        = status.ErrIO
	ErrUnknownMember             = status.ErrUnknownMember
	ErrIllegalGeneration         = status.ErrIllegalGeneration
	ErrUnknownPartition          = status.ErrUnknownPartition
	ErrRebalanceInProgress       = status.ErrRebalanceInProgress
	ErrInconsistentGroupProtocol = status.ErrInconsistentGroupProtocol
	ErrTopicExists               = status.ErrTopicExists
)

// Outcome values. They are the values of shared/status.
const (
	NotApplied   = status.NotApplied
	MaybeApplied = status.MaybeApplied
	Applied      = status.Applied
)
