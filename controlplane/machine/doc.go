// Package machine is the control-plane replica state machine (decision 55).
//
// It implements core.Machine so one Step runs under the sim kernel and a
// control-plane-owned run loop. This package is an extractable unit under
// controlplane/ autonomy (decision 56): it may import manifester/core and
// other controlplane packages, but not broker, sim, sys, cmd, veritas, kwire,
// format, net/http, Pebble, or the Dynamo SDK.
//
// CP-DSM-2 scaffolds New, Step, StateHash, and boot handling for Started and
// Stop only. Later PRs add log I/O, apply, commit queue, and roles.
package machine
