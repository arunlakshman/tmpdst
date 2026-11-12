// Package live runs one dst sim.Process in production. It is the real-time twin
// of dst's sim.Runner: one goroutine owns the process, values for it arrive in
// an inbox, timers are real timers, and values that are not control values
// (calls to the log, the object store) go to an Exec function that must not
// block and sends the answer back with Inbox.Deliver.
//
// live knows nothing of the control plane. The caller gives it the value that
// starts the process (Boot), and a function that makes the value that asks for
// a graceful stop (Stop). Together with the production adapters it is the only
// code that reads real time or real randomness; it is not on the determinism
// allow-list.
//
// How a run ends:
//
//	nil                         the process returned sim.Exit
//	ErrStoppedForRestart        the process returned sim.Crash (a tripped guard);
//	                            the caller should restart the whole process
//	ErrStopped                  the context ended
//	ErrKilled                   a graceful stop was asked and StopGrace ran out
package live
