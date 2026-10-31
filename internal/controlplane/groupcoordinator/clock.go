package groupcoordinator

import "time"

// Clock is the time source of the coordinator. Production uses the wall
// clock. Tests use a fake that fires timers when the test moves it.
type Clock interface {
	Now() time.Time
	// AfterFunc calls f once after d, on its own goroutine.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a pending AfterFunc call.
type Timer interface {
	// Stop cancels the call if it has not started yet.
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }
