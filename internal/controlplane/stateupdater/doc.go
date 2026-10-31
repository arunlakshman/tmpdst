// Package stateupdater is the production control-plane state machine of design A
// (decision 49): the same semantics as speccheck.ControlPlane for CommitSegment,
// CreateTopic, Lookup, DescribeTopics and End, on an embedded ordered KV store
// instead of in-memory maps. speccheck stays the independent oracle (decision
// 21); this package is tested against it in lockstep.
//
// It is I/O shell code, outside the determinism closure, but its behaviour is
// a pure function of the commands applied in order.
//
// Layout (all keys are prefix-free per kind):
//
//	T name                         topic -> partition count
//	E tp                           partition -> end offset (next offset)
//	R tp base(8, big endian)       one packed run per (partition, segment commit)
//	K segment-key                  idempotency record: slot, request digest, bases
//	S slot(8) segment-key          window index, so Trim can drop old K records
//	G group                        group header: generation, protocolType,
//	                               protocol, leader, assignedGeneration
//	M group member                 one member: clientId, clientHost,
//	                               groupInstanceId, sessionTimeoutMs,
//	                               rebalanceTimeoutMs, joinedTimeMs, metadata,
//	                               assignment (member id is the key tail)
//	O group tp                     committed offset: offset, leaderEpoch,
//	                               metadata, commitTimeMs
//
// where tp is uvarint(len topic), topic, partition (4, big endian), and group
// in M and O keys is uvarint(len group), group (so the keys of one group are
// one prefix). Numbers in G, M and O values are zigzag varints; strings and
// byte fields are uvarint length then bytes. A run holds
// the contiguous batches one commit assigned to one partition, packed as in
// cp/dynamo/runs.go (key table, then varints per batch), about 6 B per batch.
//
// Idempotency window (decision 48): a K record is kept until Trim drops it,
// that is for as long as the log keeps the command. A replay inside the window
// gets the original ranges (rebuilt from the replayed request and the stored
// bases) or ErrConflict when the request digest differs. After Trim the key is
// forgotten and the same key commits again as a new segment.
//
// A store failure is fail-stop: Exec panics (the replica's memory-equivalent
// state is no longer trustworthy); the error-returning methods are for callers
// that handle it.
//
// Group commands (decision Q5, Q7, Q11) fold the same way: GroupInstall,
// GroupAssign, GroupRemove and OffsetCommit are log entries, and OffsetFetch,
// DescribeGroup and ListGroups are reads. Each write is checked before it
// writes anything. A group command over 256 KiB is ErrTooLarge; the size is
// counted from the fields (see cmdSize), so it is an upper bound of the stored
// bytes. The checkpoint carries G, M and O keys like every other key; its
// format did not change.
//
// NewMem, Encode/Decode, and Apply/ApplyBytes expose the pure command fold for
// controlplane/statemachine Step without Pebble.
package stateupdater
