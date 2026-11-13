// Package live runs one machine in production. It is the real-time twin of the
// simulation kernel (package kernel): one goroutine owns the machine, inputs for it
// arrive in an inbox, timers are real timers, and the outputs of the machine go to a
// Route function of the product, which starts the real work (calls to a log, an object
// store, a network) and sends the answers back to the inbox.
//
// live knows no product. The caller gives it the machine (a machine.Factory), the first
// input (Boot), the input that asks for a graceful stop (StopMsg), and the Route
// function, which has one exhaustive switch over the output type of the machine.
//
// There are two queues, Control and Data (machine.Class). Control is taken first, so a
// stop request does not wait behind a long queue. Data may be bounded, so a slow machine
// slows its senders down.
//
// How a run ends:
//
//	nil                         the machine called End(machine.Exit)
//	ErrStoppedForRestart        the machine called End(machine.Trip, cause); the caller
//	                            should restart the whole process
//	ErrStopped                  the context ended
//	ErrKilled                   a graceful stop was asked and StopGrace ran out
//	any other error             Route returned it
//
// Together with the production adapters (real clock, real IO) this is the only code that
// reads real time. It is not on the determinism allow-list.
package live
