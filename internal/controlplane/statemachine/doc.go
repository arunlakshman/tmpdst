// Package statemachine is the control-plane replica state machine (decision 55).
//
// It implements core.Machine so one Step runs under the sim kernel and a
// control-plane-owned run loop. This package is an extractable unit under
// controlplane/ autonomy (decision 56): it may import mds/internal/core and
// other controlplane packages, but not broker, simulator, system, cmd, speccheck, kafkawire,
// format, net/http, Pebble, or the Dynamo SDK.
//
// Log I/O goes out only as Log* IOCalls. Catch-up reads the log, applies
// entries in slot order through an in-memory state updater fold, and emits no-op
// LogAppend actions to fill gaps. Client commits enter via Offer into a
// bounded queue; flush packs up to MaxCmdsPerSlot commands into one entry and
// keeps at most K LogAppends in flight. Answers leave via TakeAnswers once the
// winning slot applies. Advisory roles use timers for heartbeat, takeover, and
// failback (the slot put remains the fence). Checkpoints publish state updater
// snapshots via ObjectPut and trim via LogDeleteOld; NodeStarted restores then catch-up
// before Ready.
package statemachine
