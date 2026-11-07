package core

// Clock is the only source of time and timers for code that is not a Machine:
// the run loops, the group coordinator, the log adapters. A Machine does not use
// it. A Machine gets time on each Event (At, Wall) and asks for a timer with
// the SetTimer action.
//
// There are two implementations: system/realclock for production and
// system/fakeclock for tests and simulation, where time moves only when the
// test says so. Code must not call time.Now, time.AfterFunc and the like itself;
// architecture_test.go checks this for the control-plane packages.
type Clock interface {
	// Mono is node-local monotonic time. Use it for deadlines and for leases.
	Mono() NodeTime
	// Wall is UTC. Use it for anything that is stored or shown.
	Wall() RealTime
	// AfterFunc calls f once, on its own goroutine, after d of Mono time.
	AfterFunc(d Duration, f func()) Timer
}

// Timer is a pending AfterFunc call.
type Timer interface {
	// Stop cancels the call if it has not started yet. It reports whether it did.
	Stop() bool
}
