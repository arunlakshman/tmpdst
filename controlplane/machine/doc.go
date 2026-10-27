// Package machine is the control-plane replica state machine (decision 55).
//
// It implements core.Machine so one Step runs under the sim kernel and a
// control-plane-owned run loop. This package is an extractable unit under
// controlplane/ autonomy (decision 56): it may import manifester/core and
// other controlplane packages, but not broker, sim, sys, cmd, veritas, kwire,
// format, net/http, Pebble, or the Dynamo SDK.
//
// Log I/O goes out only as Log* IOActions. CP-DSM-3 adds a timer-driven
// catch-up sketch (CheckTail then Read) that does not apply commands yet.
package machine
