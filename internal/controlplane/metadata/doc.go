// Package metadata is the kernel of the replicated control-plane state (decision
// 49). It was called stateupdater before. It is the production state machine of
// design A: the same semantics as speccheck.ControlPlane, on an embedded ordered
// KV store instead of in-memory maps. speccheck stays the independent oracle
// (decision 21); this package is tested against it in lockstep.
//
// The kernel holds a list of modules (module.Module): topics for topics and
// segments, and groups/storage for consumer groups. Each module owns its own
// key prefixes and its own rules. The kernel does four things:
//   - It sends every operation to the module that owns it (Mode, Write, Read, Exec).
//     A new context is a new module in the list, not a new switch.
//   - It owns the log format of the entries (Entry, Encode, Decode, EntryOf,
//     Operation) and applies them in log order (Apply, ApplyBytes, CommitBatch).
//   - It writes and restores the checkpoint (Snapshot, Restore). The checkpoint
//     carries every key of every module; its format did not change.
//   - It computes the state hash, which leaves out the idempotency window.
//
// The key layout is in the packages that own the keys: T, E, R, K and S in
// topics, G, M and O in groups/storage.
//
// It is I/O shell code, outside the determinism closure, but its behaviour is
// a pure function of the commands applied in order.
//
// A store failure is fail-stop: Exec and Apply panic (the replica's
// memory-equivalent state is no longer trustworthy); the error-returning
// methods are for callers that handle it.
//
// NewMem, Encode/Decode, and Apply/ApplyBytes expose the pure command fold for
// controlplane/replica Step without Pebble.
package metadata
