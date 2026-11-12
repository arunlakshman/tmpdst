// Package metadb is the kernel of the replicated control-plane state. It has
// the same rules as the reference model in internal/spec, but it keeps the
// state in an embedded ordered key-value store instead of in-memory maps.
// internal/spec stays the independent reference; this package is tested
// against it.
//
// The kernel holds one table built from the schemas of the contexts
// (Schema): domain/topic for topics and segments, and domain/group for
// consumer groups. Each context owns its own key prefixes and its own rules.
// The kernel does four things:
//   - It sends every operation to its row in the table (Mode, Write, Read).
//     A new operation is one row in a schema, not a new switch.
//   - It owns the log entry format (Entry, Encode, Decode, EntryOf): an id and
//     one command, as the protobuf message logv1.Entry. A new command needs a
//     new field there and a case in entry_codec.go. The kernel applies the
//     entries in log order (Apply, ApplyBytes).
//   - It writes and restores the checkpoint (Snapshot, Restore). The checkpoint
//     carries every key of every context.
//   - It computes the state hash, which leaves out the idempotency window.
//
// The key layout is in the packages that own the keys: T, E, R, K and S in
// domain/topic, G, M and O in domain/group.
//
// It is I/O shell code, outside the determinism closure, but its behaviour is
// a pure function of the commands applied in order.
//
// A store failure is fail-stop: Apply and CreateTopic panic (the replica's
// memory-equivalent state is no longer trustworthy); the error-returning
// methods are for callers that handle it.
//
// NewMem, Encode/Decode, and Apply/ApplyBytes expose the pure command fold for
// controlplane/replica Step without Pebble.
package metadb
