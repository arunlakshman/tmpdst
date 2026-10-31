// Package runloop is the production event loop for the control-plane machine.
// It is the extractable twin of system.Run (decision 55): one goroutine owns
// Step, timers, and inject work from the gRPC adapter. It imports only
// mds/internal/core and stdlib (decision 56).
package runloop
