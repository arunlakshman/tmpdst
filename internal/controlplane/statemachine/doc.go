// Package statemachine is the control-plane replica state machine (decision 55).
//
// It implements core.Machine so one Step runs under the sim kernel and a
// control-plane-owned run loop. This package is an extractable unit under
// controlplane/ autonomy (decision 56): it may import github.com/manifesterio/mds/internal/core and
// other controlplane packages, but not broker, simulator, system, cmd, speccheck, kafkawire,
// format, net/http, Pebble, or the Dynamo SDK.
//
// Log I/O goes out only as Log* IOCalls. Catch-up reads the log, applies
// entries in slot order through an in-memory state updater fold. The log has no
// empty slot below its tail: a node that finds its next slot missing while a
// later one exists fell behind a trim and restores from the checkpoint. Client
// commits enter via Offer into a bounded queue; flush packs up to MaxCmdsPerSlot
// commands into one entry and keeps one LogAppend in flight. Answers leave via
// TakeAnswers once the winning slot applies.
//
// One node writes at a time. A node becomes the writer by claiming a new epoch
// in the log (LogClaimEpoch), then writes a fence entry at the tail; every entry
// it writes carries the epoch. A node that sees a newer epoch, or whose put is
// refused below the floor of the log, stops writing and stays a standby that
// still reads. Checkpoints publish state updater snapshots via ObjectPut and
// trim via LogDeleteOld; NodeStarted restores then catch-up before Ready, and a
// node that is restoring never claims and never writes.
package statemachine
