// Package machine is the control-plane replica state machine (decision 55).
//
// It implements core.Machine so one Step runs under the sim kernel and a
// control-plane-owned run loop. This package is an extractable unit under
// controlplane/ autonomy (decision 56): it may import manifester/internal/core and
// other controlplane packages, but not broker, sim, sys, cmd, veritas, kwire,
// format, net/http, Pebble, or the Dynamo SDK.
//
// Log I/O goes out only as Log* IOActions. Catch-up reads the log, applies
// entries in slot order through an in-memory applier fold, and emits no-op
// LogAppend actions to fill holes. Client commits enter via Offer into a
// bounded queue; flush packs up to MaxCmdsPerSlot commands into one entry and
// keeps at most K LogAppends in flight. Answers leave via TakeAnswers once the
// winning slot applies. Advisory roles use timers for heartbeat, takeover, and
// failback (the slot put remains the fence). Checkpoints publish applier
// snapshots via ObjPut and trim via LogTrim; Started restores then catch-up
// before Ready.
package machine
