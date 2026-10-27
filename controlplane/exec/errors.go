package exec

import "errors"

var (
	// ErrInvalidSlot is returned for a negative log slot.
	ErrInvalidSlot = errors.New("exec: invalid log slot")
	// ErrNotFound is returned when an object key is missing or out of range.
	ErrNotFound = errors.New("exec: object not found")
)
