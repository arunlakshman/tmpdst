package executor

import "errors"

var (
	// ErrInvalidSlot is returned for a negative log slot.
	ErrInvalidSlot = errors.New("executor: invalid log slot")
	// ErrNotFound is returned when an object key is missing or out of range.
	ErrNotFound = errors.New("executor: object not found")
)
